# pg_liquid Documentation

`pg_liquid` is a PostgreSQL extension that implements the public Liquid blog
model on top of PostgreSQL storage.

Current shipped surface:

- `liquid.query(text)` for Liquid program execution
- `liquid.query_as(text, text)` for trusted principal-scoped Liquid execution
- `liquid.read_as(text, text)` for least-privilege principal-scoped Liquid reads
- `liquid.create_row_normalizer(...)`
- `liquid.drop_row_normalizer(...)`
- `liquid.rebuild_row_normalizer(...)`
- graph-backed persistent state in `liquid.vertices` and `liquid.edges`
- internal normalizer state in `liquid.row_normalizers` and
  `liquid.row_normalizer_bindings`
- Liquid program parsing with assertions, local rules, and one terminal query
- compounds through `Type@(cid=..., role=...)`
- tabular query results only

Documents:

- [Overview](01_OVERVIEW.md)
- [Data Model](02_DATA_MODEL.md)
- [Execution](03_EXECUTION.md)
- [Storage](04_STORAGE.md)
- [Security](05_SECURITY.md)
- [Testing](06_TESTING.md)
- [Roadmap](07_ROADMAP.md)
- [Operations](08_OPERATIONS.md)
- [Reference: Liquid blog](references/liquid_blog.md)
