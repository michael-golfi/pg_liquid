# 7. Roadmap

## Completed

- public execution surface: `liquid.query(text)`, `liquid.query_as(text, text)`,
  and `liquid.read_as(text, text)`
- extension-native table-to-compound normalization through row triggers
- LIquid blog syntax parsing
- graph-backed bootstrap schema
- relative-identity compounds
- query-local rule evaluation
- install/upgrade path to public release `0.1.0`
- regression coverage for blog parity and edge cases
- first-class CLS policy model with explicit grants and derived policy compounds
- `liquid/acts_for` principal inheritance plus `Principal@(...)` metadata
- session-principal CLS policy enforcement
- benchmark smoke harness
- CI build/test matrix across PostgreSQL 14-18

## Next Useful Work

- benchmark comparison against larger datasets and different PostgreSQL settings
- read-only consumer integration examples and operator rollout recipes
- more exhaustive parser fuzzing and stress tests
- deeper planner/statistics tuning for large skewed graphs
- richer table normalization features such as joined projections, conditional
  mappings, or async CDC

## Explicitly Deferred

- subgraph-return interface
- custom parallel execution path
