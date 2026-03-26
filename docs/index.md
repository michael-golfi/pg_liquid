# pg_liquid

`pg_liquid` brings Liquid-style graph, compound, and ontology querying into
PostgreSQL as a native extension.

It is built for teams that want to:

- store graph facts as ordinary PostgreSQL data
- express ontology and workflow relationships with Liquid syntax
- query recursive graphs without standing up a separate graph database
- enforce principal-scoped reads with extension-level policy rules

## What You Can Do

- create graph facts with `Edge(...)`
- define structured compound types such as `Email@(...)` or `OntologyClaim@(...)`
- write recursive query-local rules
- build ontology vocabularies directly in graph data
- validate taxonomies and instance links with first-class helper functions
- project relational tables into Liquid compounds with row normalizers
- expose least-privilege reads through `liquid.read_as(...)`

## Start Here

1. [Install pg_liquid](./getting-started/install.md)
2. [Create your first graph](./getting-started/first-graph.md)
3. [Query a graph](./guides/query-graphs.md)
4. [Build an ontology](./guides/ontologies.md)
5. [Review example domains](./guides/examples.md)

## Compatibility

`pg_liquid` is currently validated against PostgreSQL `14`, `15`, `16`, `17`,
and `18`.
