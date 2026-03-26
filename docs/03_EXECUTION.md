# 3. Execution

## Evaluation Model

`pg_liquid` parses a LIquid program, executes any allowed top-level assertions,
reparses the program against the updated graph state, and then evaluates the
terminal query.

Persistent assertions supported at the top level:

- `Edge(...)`
- `DefPred(...)`
- `DefCompound(...)`
- `Type@(cid=..., ...)`

Rules are query-local only.

Wrapper behavior:

- `liquid.query(...)` allows top-level assertions
- `liquid.query_as(...)` allows top-level assertions and binds a trusted
  principal for the duration of the call
- `liquid.read_as(...)` binds a trusted principal for the duration of the call
  and rejects top-level assertions

## Solver

The solver uses an iterative binding frontier:

1. choose the cheapest remaining atom
2. scan matching EDB, compound, or derived facts
3. extend and deduplicate bindings
4. continue until all constraints are satisfied

Recursive rules are evaluated with semi-naive iteration. Recursive rules with
multiple recursive body atoms are expanded across delta positions so one
recursive atom reads `delta_k` while the others read the full IDB.

## Data Access

Execution uses:

- SPI-backed graph access
- a selective in-memory edge cache for relevant predicates
- cache-backed compound scans when the required role predicates are resident
- hash-based deduplication for bindings and derived facts

When principal context is bound through `liquid.query_as(...)`,
`liquid.read_as(...)`, or `pg_liquid.policy_principal`, the executor:

1. resolves the effective principal set through `liquid/acts_for`
2. preloads explicit grants from `ReadPredicate`, `ReadCompound`, and
   `ReadTriple`
3. preloads derived subject/object/compound visibility from
   `PredicateReadBySubject`, `PredicateReadByObject`, and
   `CompoundReadByRole`
4. also loads legacy edge-based policy facts for compatibility
5. filters both edge scans and compound scans through that policy layer before
   facts become visible to the solver

The edge cache is bounded. If the estimated cache size exceeds the configured
budget, execution falls back to direct SPI scans instead of materializing the
full cache. The budget can be tuned with:

- `pg_liquid.edge_cache_budget_kb`
- `pg_liquid.edge_cache_row_estimate_bytes`

Queries with variable edge predicates still fall back to loading all live edges
when that is required for correctness and within the configured cache budget.

## Results

`liquid.query(...)`, `liquid.query_as(...)`, and `liquid.read_as(...)` return
tabular bindings only. Output columns correspond to the first appearance order
of named variables in the terminal query, and values are returned as LIquid
string literals rather than internal vertex ids.
