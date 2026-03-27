# SQL API

## Query Execution

### `liquid.query(program text)`

Executes a Liquid program with optional top-level assertions and one terminal query.
The SQL `AS t(...)` record type must declare exactly the same number of columns
as the terminal query projects. Queries that project zero variables, such as a
terminal query made entirely of `_` anonymous slots, are rejected by this API.

### `liquid.query_as(principal text, program text)`

Executes a Liquid program while binding a trusted principal for the duration of the call.

### `liquid.read_as(principal text, program text)`

Executes a read-only Liquid program while binding a trusted principal. Top-level assertions are rejected.

Quoted constants used in query-local rules and query atoms are interned before
solving, so rule outputs may contain fresh literals even if those literals did
not exist in `liquid.vertices` before the call started.

## Row Normalizers

### `liquid.create_row_normalizer(source_table regclass, normalizer_name text, compound_type text, role_columns jsonb, backfill boolean default true)`

Creates a table-backed normalizer that projects rows into Liquid compounds.

### `liquid.drop_row_normalizer(source_table regclass, normalizer_name text, purge boolean default true)`

Drops a row normalizer and optionally purges the generated graph data.

### `liquid.rebuild_row_normalizer(source_table regclass, normalizer_name text)`

Rebuilds one normalizer from the current relational source data.
