# 2. Data Model

## Public Model

`pg_liquid` now exposes the Liquid blog data model directly:

- all persistent data is stored as graph edges `(subject, predicate, object)`
- schema is also graph data
- Datalog rules are query-local, not persisted
- compounds use relative identity with `Type@(cid=..., role=...)`

The public SQL execution surface is:

```sql
liquid.query(program text)
liquid.query_as(principal text, program text)
liquid.read_as(principal text, program text)
```

`program` is a Liquid program containing zero or more assertions and rules,
followed by one terminal `?` query.

`liquid.query(...)` is the base extension entrypoint.

`liquid.query_as(...)` and `liquid.read_as(...)` bind trusted principal context
for the duration of one call:

- `liquid.query_as(...)` supports trusted write-capable flows that may use
  top-level assertions before querying
- `liquid.read_as(...)` is the least-privilege read surface and rejects
  top-level assertions

## Query Language

Supported surface syntax:

- `%` line comments
- `.`-terminated assertions and rule definitions
- one terminal `?` query
- bare variables such as `x`
- `_` anonymous variables
- double-quoted string literals
- `Edge(s, "predicate", o)`
- `Type@(cid=x, role="value", ...)`

Legacy extension syntax is intentionally rejected:

- `?x` variables
- `query_subgraph(...)`
- subgraph arguments
- `sys:*` schema and security APIs

## Bootstrap Schema

Schema is encoded using Liquid bootstrap predicates:

- `liquid/type`
- `liquid/cardinality`
- `liquid/subject_meta`
- `liquid/object_meta`
- `liquid/compound_predicate`
- `liquid/mutable`

`DefPred(...)`, `DefCompound(...)`, and `TypeAndCardinality(...)` are query
predicates synthesized from that graph data so the Liquid blog examples can be
executed directly.

`pg_liquid` also ships a first-class policy vocabulary in graph data:

- `ReadPredicate@(principal=..., predicate=...)`
- `ReadCompound@(principal=..., compound_type=...)`
- `ReadTriple@(principal=..., subject=..., predicate=..., object=...)`
- `PredicateReadBySubject@(predicate=..., relation=...)`
- `PredicateReadByObject@(predicate=..., relation=...)`
- `CompoundReadByRole@(compound_type=..., role=...)`
- `Principal@(id=..., kind=...)`
- `Edge(child_principal, "liquid/acts_for", parent_principal)`

## Compounds

Compounds are stored as ordinary vertices plus role edges:

- the compound vertex literal is a canonical relative identity string such as
  `FilmPerf@(actor='Harrison Ford', film='Star Wars', role='Han Solo')`
- each named role becomes a graph edge from the compound vertex
- `cid=` is optional in queries and assertions; when omitted, the identity is
  still computed and used internally

`DefCompound("Type", "role", "cardinality", "type")` declares compound roles
and also exposes the implicit `Type@(...)` query form from the Liquid blog.
