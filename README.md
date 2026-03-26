# pg_liquid

`pg_liquid` is a PostgreSQL extension that maps the LIquid blog language and
data model onto native PostgreSQL storage and execution.

It ships:

- `liquid.query(program text)`
- `liquid.query_as(principal text, program text)`
- `liquid.read_as(principal text, program text)`
- row normalizer management functions for projecting relational rows into
  LIquid compounds

Supported LIquid program features include:

- `%` comments
- `.`-terminated assertions and rule definitions
- one terminal `?` query
- `Edge(...)`
- named compounds via `Type@(cid=..., role=...)`
- query-local recursive rules
- CLS-aware reads through `liquid.read_as(...)` and `liquid.query_as(...)`

## Version

This PGXN package is version `0.1.0`.

## Build And Install

Build and install against the target PostgreSQL instance:

```sh
cd libs/pg_liquid
make
make install
```

Then create the extension in the target database:

```sql
create extension pg_liquid;
```

## Upgrade

The shipped upgrade path covers the legacy internal versions `1.0.0`,
`1.1.0`, and `1.2.0` to the public release `0.1.0`:

```sql
alter extension pg_liquid update to '0.1.0';
```

Alias scripts are also included for the legacy non-semver version names `1.0`,
`1.1`, and `1.2`.

## Validation

Useful local checks:

```sh
make -C libs/pg_liquid package-check
make -C libs/pg_liquid installcheck
make -C libs/pg_liquid bench-check
make -C libs/pg_liquid pgxn-package
```

`package-check` validates `META.json`, the control file, and the referenced SQL
install script. `pgxn-package` builds a release tarball under `release/`.

## Repository Layout

- [docs](./docs)
- [sql](./sql)
- [src](./src)
- [tests](./tests)

The operational rollout details live in [docs/08_OPERATIONS.md](./docs/08_OPERATIONS.md).
