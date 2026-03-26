# 1. Overview

## Goal

`pg_liquid` maps the Liquid blog language and data model into PostgreSQL
without inventing a separate public API surface.

## Shipped Interface

The extension exports:

```sql
liquid.query(program text)
liquid.query_as(principal text, program text)
liquid.read_as(principal text, program text)
liquid.create_row_normalizer(source_table regclass, normalizer_name text, compound_type text, role_columns jsonb, backfill boolean default true)
liquid.drop_row_normalizer(source_table regclass, normalizer_name text, purge boolean default true)
liquid.rebuild_row_normalizer(source_table regclass, normalizer_name text)
```

`program` is a Liquid Datalog program containing:

- `%` comments
- `.`-terminated assertions and rule definitions
- one terminal `?` query
- bare variables and `_`
- quoted string constants
- `Edge(...)`
- `Type@(cid=..., role=...)`

## Deliberate Scope

Implemented:

- graph-backed bootstrap schema
- query-local rule evaluation
- relative-identity compounds
- table-to-compound normalization through row triggers
- trusted-principal query and read wrappers plus session-principal CLS filtering with explicit grants, derived policy compounds, and inherited `liquid/acts_for` principal scope
- install and upgrade support
- regression coverage for blog examples and parser/compound/tombstone edge cases

Not implemented:

- subgraph query APIs
- parallel custom scan execution
- persisted global rules

## PostgreSQL-Specific Tradeoffs

This implementation uses PostgreSQL tables and indexes rather than Liquid's
custom in-memory storage engine. That means:

- MVCC still applies
- physical storage is relational
- equality-heavy probes are improved with both btree and hash indexes
- the execution engine relies on in-memory caches and adaptive constraint
  ordering rather than a custom storage access method
