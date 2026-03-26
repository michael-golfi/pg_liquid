# SQL API

## Query Execution

### `liquid.query(program text)`

Executes a LIquid program with optional top-level assertions and one terminal query.

### `liquid.query_as(principal text, program text)`

Executes a LIquid program while binding a trusted principal for the duration of the call.

### `liquid.read_as(principal text, program text)`

Executes a read-only LIquid program while binding a trusted principal. Top-level assertions are rejected.

## Row Normalizers

### `liquid.create_row_normalizer(source_table regclass, normalizer_name text, compound_type text, role_columns jsonb, backfill boolean default true)`

Creates a table-backed normalizer that projects rows into LIquid compounds.

### `liquid.drop_row_normalizer(source_table regclass, normalizer_name text, purge boolean default true)`

Drops a row normalizer and optionally purges the generated graph data.

### `liquid.rebuild_row_normalizer(source_table regclass, normalizer_name text)`

Rebuilds one normalizer from the current relational source data.
