# SQL API

## Query Execution

### `liquid.query(program text)`

Executes a Liquid program with optional top-level assertions and one terminal
query.

### `liquid.query_as(principal text, program text)`

Executes a Liquid program while binding a trusted principal for the duration of
the call.

### `liquid.read_as(principal text, program text)`

Executes a read-only Liquid program while binding a trusted principal.
Top-level assertions are rejected.

## Ontology Helpers

### `liquid.compound_roles(compound_type text)`

Returns the declared compound roles in canonical sorted order. This is the
supported public helper for schema-aware tooling and docs examples.

### `liquid.validate_taxonomy(subclass_predicate text)`

Returns issue rows for taxonomy integrity problems under the chosen subclass
predicate. Current issue types are:

- `self_parent`
- `cycle`
- `missing_referenced_class`

### `liquid.validate_instances(instance_predicate text, subclass_predicate text)`

Returns issue rows for instance-to-class references that do not line up with
the active taxonomy. Current issue types are:

- `dangling_class_reference`
- `invalid_class_reference`

## Row Normalizers

### `liquid.create_row_normalizer(source_table regclass, normalizer_name text, compound_type text, role_columns jsonb, backfill boolean default true)`

Creates a table-backed normalizer that projects rows into Liquid compounds.

### `liquid.drop_row_normalizer(source_table regclass, normalizer_name text, purge boolean default true)`

Drops a row normalizer and optionally purges the generated graph data.

### `liquid.rebuild_row_normalizer(source_table regclass, normalizer_name text)`

Rebuilds one normalizer from the current relational source data.

`create_row_normalizer(...)`, `drop_row_normalizer(...)`, and
`rebuild_row_normalizer(...)` are operator-facing APIs. They are revoked from
`public` by default in `0.1.6`.
