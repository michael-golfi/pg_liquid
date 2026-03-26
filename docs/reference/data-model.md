# Data Model

The public model in `pg_liquid` is graph-first.

## Persistent Facts

All persistent facts are stored as graph edges:

- subject
- predicate
- object

## Schema as Data

Predicate and compound declarations are represented in graph data and exposed through Liquid-compatible query predicates.

## Compounds

Compounds are represented as:

- one canonical compound vertex literal
- one role edge per compound field

See the original deep-dive for more detail: [2. Data Model](../02_DATA_MODEL.md)
