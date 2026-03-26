# pg_liquid

`pg_liquid` brings Liquid-style graph, compound, and ontology querying into PostgreSQL as a native extension.

It is built for teams that want to:

- store graph facts as ordinary PostgreSQL data
- express ontology and workflow relationships with Liquid syntax
- query recursive graphs without standing up a separate graph database
- enforce principal-scoped reads with extension-level policy rules

## What You Can Do

- Create graph facts with `Edge(...)`
- Define structured compound types such as `Email@(...)` or `OntologyClaim@(...)`
- Write recursive query-local rules
- Build ontology vocabularies directly in graph data
- Project relational tables into Liquid compounds with row normalizers
- Expose least-privilege reads through `liquid.read_as(...)`

## Start Here

1. [Install pg_liquid](./getting-started/install.md)
2. [Create your first graph](./getting-started/first-graph.md)
3. [Query a graph](./guides/query-graphs.md)
4. [Build an ontology](./guides/ontologies.md)

## Compatibility

`pg_liquid` is currently validated against PostgreSQL `14`, `15`, `16`, `17`, and `18`.

## Core SQL Surface

```sql
liquid.query(program text)
liquid.query_as(principal text, program text)
liquid.read_as(principal text, program text)
liquid.create_row_normalizer(source_table regclass, normalizer_name text, compound_type text, role_columns jsonb, backfill boolean default true)
liquid.drop_row_normalizer(source_table regclass, normalizer_name text, purge boolean default true)
liquid.rebuild_row_normalizer(source_table regclass, normalizer_name text)
```

## Learn by Task

- [I want to model simple graph edges](./getting-started/first-graph.md)
- [I want to query paths and recursive reachability](./guides/query-graphs.md)
- [I want typed records and role-based modeling](./guides/compounds.md)
- [I want a real ontology vocabulary](./guides/ontologies.md)
- [I want PostgreSQL tables to feed Liquid compounds](./guides/normalizers.md)
- [I want principal-aware reads](./guides/security.md)
