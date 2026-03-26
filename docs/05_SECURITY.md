# 5. Security

## Current Model

`pg_liquid` implements a graph-level read filter for LIquid facts and
compounds.

The executor reads the session principal from:

- `pg_liquid.policy_principal`

If that setting is empty or unset, the executor does not apply LIquid-specific
filtering and all non-deleted graph facts are readable.

If that setting is present, the executor only exposes data granted to that
principal through graph edges and derived CLS policies.

## First-Class Policy Model

The preferred authoring model is:

- bind the trusted request principal through `liquid.query_as(principal, program)` or `liquid.read_as(principal, program)`
- author explicit grants and derived policies as LIquid compounds
- keep `pg_liquid.policy_principal` as the internal execution carrier, not the
  application-facing API

Primary explicit grant compounds:

- `ReadPredicate@(principal="...", predicate="...")`
- `ReadCompound@(principal="...", compound_type="...")`
- `ReadTriple@(principal="...", subject="...", predicate="...", object="...")`

Primary derived policy compounds:

- `PredicateReadBySubject@(predicate="...", relation="...")`
- `PredicateReadByObject@(predicate="...", relation="...")`
- `CompoundReadByRole@(compound_type="...", role="...")`

Principal inheritance is also supported:

- `Edge("<child-principal>", "liquid/acts_for", "<parent-principal>")`

Optional metadata for authoring and introspection:

- `Principal@(id="...", kind="...")`

The legacy built-in policy edges are still supported for compatibility:

- `Edge("<predicate>", "liquid/readable_if_subject_has", "<relation-predicate>")`
- `Edge("<predicate>", "liquid/readable_if_object_has", "<relation-predicate>")`
- `Edge("<compound-type>", "liquid/readable_compound_if_role_has", "<role-name>")`

Examples:

```liquid
PredicateReadBySubject@(predicate="name", relation="owner").
PredicateReadByObject@(predicate="member_of", relation="member").
CompoundReadByRole@(compound_type="Email", role="user").

Edge("person:alice", "owner", "user:alice").
Edge("org:acme", "member", "user:bob").
Email@(user="user:alice", domain="example.com").
```

With `set pg_liquid.policy_principal = 'user:alice'`, the executor will allow:

- `Edge("person:alice", "name", "...")`
- `Email@(...)` compounds whose `user` role is `"user:alice"`

With `set pg_liquid.policy_principal = 'user:bob'`, the executor will allow:

- `Edge(subject, "member_of", "org:acme")`

If `agent:support_bot` acts for `user:alice`:

```liquid
Edge("agent:support_bot", "liquid/acts_for", "user:alice").
```

the executor evaluates policy against the effective principal set
`{agent:support_bot, user:alice}`.

Legacy explicit grants are also still supported:

- `Edge("<principal>", "liquid/can_read_predicate", "<predicate>")`
- `Edge("<principal>", "liquid/can_read_compound", "<compound-type>")`
- `ReadTriple@(user="<principal>", subject="...", predicate="...", object="...")`

## Query Semantics

`liquid.query(...)` evaluates against all non-deleted rows in `liquid.edges`
that are visible to the current session principal.

The effective principal set is the bound principal plus every reachable
ancestor through `liquid/acts_for`.

For plain `Edge(...)` atoms, a fact is visible when at least one of these holds:

- the session has no `pg_liquid.policy_principal`
- the session principal has a matching `ReadPredicate` grant
- the session principal has a matching predicate grant
- the session principal has a matching exact `ReadTriple` grant
- the predicate is granted through `PredicateReadBySubject`
- the predicate is marked `liquid/readable_if_subject_has` and the subject has
  the required relation edge to the principal
- the predicate is granted through `PredicateReadByObject`
- the predicate is marked `liquid/readable_if_object_has` and the object has
  the required relation edge to the principal

For compound atoms, the executor allows a match when:

- the session has no `pg_liquid.policy_principal`
- the session principal has an explicit `ReadCompound` grant
- the session principal has an explicit compound-type grant
- the compound type is granted through `CompoundReadByRole`
- the compound type is marked `liquid/readable_compound_if_role_has` and the
  compound has the required role edge to the principal
- all role edges needed to materialize that compound are individually visible

## PostgreSQL Boundary

This CLS layer is an extension-level read filter. It does not replace ordinary
PostgreSQL controls such as:

- ownership of the `liquid` schema
- function execution privileges
- table ACLs

Applications still need normal PostgreSQL permissions to prevent direct reads
from bypassing `liquid.query(...)`. For least-privilege read-only access, prefer
granting only `EXECUTE` on `liquid.read_as(...)`.

## Trust Boundary Notes

`pg_liquid.policy_principal` is caller-supplied session context, not an
authenticated identity boundary on its own.

If a direct SQL client can `SET` or `RESET pg_liquid.policy_principal`, it can
impersonate another principal or disable CLS entirely for `liquid.query(...)`.

For AI or end-user scoped access, treat `pg_liquid` as a filtering engine behind
a trusted application boundary. Do not assume direct SQL access plus
`pg_liquid.policy_principal` is sufficient isolation by itself.

`liquid.read_as(...)` is the supported least-privilege read surface for direct
SQL roles that should not read internal `liquid` tables or execute assertion-
capable LIquid programs. It runs as a security-definer wrapper and rejects
top-level assertions.

Operational rollout guidance, including recommended `GRANT`/`REVOKE` patterns,
is in [08_OPERATIONS.md](08_OPERATIONS.md).
