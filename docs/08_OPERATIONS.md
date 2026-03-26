# 8. Operations

## Install

Build and install the extension against the target PostgreSQL instance:

```sh
make
sudo make install
```

Validated PostgreSQL versions: `14`, `15`, `16`, `17`, and `18`.

Then create the extension in the target database:

```sql
create extension pg_liquid;
```

This installs:

- the `liquid` schema
- `liquid.vertices`
- `liquid.edges`
- `liquid.row_normalizers`
- `liquid.row_normalizer_bindings`
- `liquid.query(text)`
- `liquid.query_as(text, text)`
- `liquid.read_as(text, text)`
- the row normalizer management functions

## Upgrade

`pg_liquid` ships upgrade paths from the legacy internal versions `1.0.0`,
`1.1.0`, and `1.2.0` to the public releases `0.1.0` and `0.1.1`:

```sql
alter extension pg_liquid update to '0.1.1';
```

The repository regression `sql/liquid_upgrade.sql` validates both:

- empty install/upgrade behavior
- data-bearing `1.0.0 -> 0.1.1` upgrades

## Rollout Checklist

Recommended rollout sequence:

1. Install the extension binaries on the target PostgreSQL host.
2. Rehearse `create extension` or `alter extension ... update` on a staging copy.
3. Apply the extension change in production.
4. Lock down direct access to the underlying `liquid` tables.
5. If using table-authoritative normalization, register normalizers and backfill.
6. Run smoke queries through `liquid.query(...)`.
7. Verify benchmark and regression expectations on representative data.

## Privilege Model

CLS is enforced by the Liquid execution functions, not by direct reads from the
storage tables. Production deployment should grant the narrowest useful
function surface while restricting direct reads from the graph tables.

Recommended pattern:

```sql
revoke all on schema liquid from public;
revoke all on all tables in schema liquid from public;
revoke all on all functions in schema liquid from public;

grant usage on schema liquid to app_user;
grant execute on function liquid.read_as(text, text) to app_user;
```

`liquid.read_as(text, text)` is the supported least-privilege read surface for
application roles. It is `SECURITY DEFINER`, binds principal context per call,
and rejects top-level assertions.

`liquid.query(text)` and `liquid.query_as(text, text)` remain `SECURITY
INVOKER`. They are still the right surfaces for trusted operators and
write-capable server code, but `EXECUTE` alone is not enough for an unprivileged
direct SQL role because they read the underlying `liquid` tables as the caller.

That means the privilege model is:

- app and AI reader roles: `USAGE` on schema plus `EXECUTE` on `liquid.read_as(text, text)`
- trusted write-capable server code: `liquid.query_as(text, text)` or `liquid.query(text)` with the needed internal privileges
- operator workflows: normalizer management and maintenance sessions stay privileged

`pg_liquid.policy_principal` is still caller-controlled session state. A direct
SQL client that can `SET` or `RESET` it can impersonate another principal or
disable CLS filtering for `liquid.query(...)`. Prefer `liquid.read_as(...)` or
`liquid.query_as(...)` instead of exposing raw GUC management to application
code.

If application code should manage row normalizers, it also needs execute access
to:

- `liquid.create_row_normalizer(...)`
- `liquid.drop_row_normalizer(...)`
- `liquid.rebuild_row_normalizer(...)`

Those normalizer management functions also depend on internal `liquid` objects.
Treat them as privileged operator APIs today, not as a ready-made least-privilege
application surface.

## Session Setup

Preferred application path:

```sql
select *
from liquid.read_as('user:alice', $$
  Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text);
```

`liquid.read_as(...)` is the preferred application read path. It binds the
request principal for the duration of the call, evaluates CLS as that principal
or any inherited `liquid/acts_for` ancestor, and rejects top-level assertions.

`liquid.query_as(...)` remains available for trusted write-capable wrappers that
need to seed or mutate Liquid state before querying.

`pg_liquid.policy_principal` remains available for privileged maintenance or
manual operator sessions:

```sql
set pg_liquid.policy_principal = 'user:alice';
reset pg_liquid.policy_principal;
```

## Table Normalizers

Row normalizers are the supported way to project an authoritative relational
table into Liquid compounds.

Preconditions:

- the source relation must be a base table
- the source table must have a primary key
- the compound type must already be defined in the Liquid graph schema
- the `role_columns` JSON object must map every compound role exactly once
- if any mapped source column is `NULL`, that row projects no compound

To project a table into a compound type:

```sql
select liquid.create_row_normalizer(
  'public.film_performances'::regclass,
  'film_perf',
  'FilmPerf',
  '{"actor":"actor_name","film":"film_title","role":"role_name"}'::jsonb
);
```

This installs an `AFTER INSERT OR UPDATE OR DELETE FOR EACH ROW` trigger on the
source table and, by default, backfills existing rows.

Recommended rollout sequence for a new normalizer:

1. Define the target compound schema through `liquid.query(...)`.
2. Create the row normalizer with `backfill => true` in staging first.
3. Verify projected compounds through `liquid.query(...)`.
4. Promote the same registration in production.

Runtime behavior:

- inserts project a compound and its role edges
- updates diff old and new projections rather than blindly deleting/reinserting
- deletes remove bindings and only tombstone graph edges when no other row still
  supports the same triple
- duplicate relational rows may project the same compound safely

Operational helpers:

```sql
select liquid.rebuild_row_normalizer(
  'public.film_performances'::regclass,
  'film_perf'
);

select liquid.drop_row_normalizer(
  'public.film_performances'::regclass,
  'film_perf',
  true
);
```

Use `rebuild_row_normalizer(...)` after schema drift, manual graph repair, or
if you need to regenerate bindings from the source table. Use
`drop_row_normalizer(..., purge => true)` to remove the trigger and deproject
all graph facts maintained by that normalizer.

## Smoke Checks

Minimal post-deploy checks:

```sql
select * from liquid.query($$
  Edge("a", "knows", "b").
  Edge(x, "knows", y)?
$$) as t(x text, y text);
```

For CLS validation:

```sql
select * from liquid.query($$
  DefPred("owner", "1", "liquid/node", "0", "liquid/string").
  DefPred("name", "1", "liquid/node", "0", "liquid/string").
  Edge("name", "liquid/readable_if_subject_has", "owner").
  Edge("person:alice", "owner", "user:alice").
  Edge("person:alice", "name", "Alice").
  Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text);
```

Run that once with:

- `set pg_liquid.policy_principal = 'user:alice'`

Expected:

- one visible `name` row for `person:alice`

Then run with:

- `reset pg_liquid.policy_principal`

Expected:

- all live matching facts are visible

## Rollback

There is no reverse migration shipped from `0.1.0` back to the legacy internal
`1.2.0`, `1.1.0`, or `1.0.0` versions.

Operationally, rollback means one of:

- restore the database from backup/snapshot
- restore the older extension binaries and database state together
- treat rollout as forward-only and fix forward if the extension has already
  mutated data

Do not assume `alter extension ... update` can be reversed.
