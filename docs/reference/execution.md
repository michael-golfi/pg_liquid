# Execution Model

`pg_liquid` parses one LIquid program, executes any top-level assertions, and evaluates the terminal query against the current graph state.

## Important Runtime Properties

- assertions run before the query
- rules are query-local
- recursive rules are supported
- PostgreSQL remains the storage and transaction boundary

Deep reference: [3. Execution](../03_EXECUTION.md)
