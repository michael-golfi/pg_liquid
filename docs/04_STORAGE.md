# 4. Storage

## Physical Schema

`pg_liquid` stores graph state in two core tables:

```sql
create table liquid.vertices (
  id bigserial primary key,
  literal text not null unique
);

create table liquid.edges (
  subject_id bigint not null references liquid.vertices(id),
  predicate_id bigint not null references liquid.vertices(id),
  object_id bigint not null references liquid.vertices(id),
  tx_id bigint not null default txid_current(),
  is_deleted boolean not null default false,
  primary key (subject_id, predicate_id, object_id)
);
```

It also stores normalizer metadata in internal extension tables:

```sql
create table liquid.row_normalizers (...);
create table liquid.row_normalizer_bindings (...);
```

There is no subgraph column and no auxiliary graph schema catalog.

## Indexes

The shipped indexes match the evaluator's main access paths:

- `(subject_id, predicate_id)` for forward traversals
- `(predicate_id, object_id)` for reverse typed lookups
- `(predicate_id)` for predicate scans
- `(subject_id)` for subject scans
- hash indexes on `vertices.literal`, `edges.subject_id`, `edges.predicate_id`,
  and `edges.object_id` for single-column equality probes

All indexes are partial on `is_deleted = false`.

## Mutation Model

Writes are append/update in SQL terms but logically edge-oriented:

- vertices are created on demand from Liquid string identities
- facts are inserted as graph edges
- duplicate edge assertions reactivate tombstoned rows with
  `is_deleted = false`
- compounds are lowered to a canonical compound vertex plus role edges
- row normalizers project authoritative relational table rows into compounds
  and track provenance in `liquid.row_normalizer_bindings`

Rules are not stored persistently. They exist only for the duration of one
`liquid.query(...)` call.

## Row Normalizer Lifecycle

Row normalizers are table-authoritative projections from one PostgreSQL base
table into one Liquid compound type.

Each normalizer stores:

- the source table oid
- a stable normalizer name unique within that source table
- the target compound type
- a JSON role-to-column mapping
- the source table primary-key column list used to identify rows across updates
  and deletes

`liquid.row_normalizer_bindings` is the provenance ledger. It records which
source row currently supports which projected role edges. The graph itself does
not store source-row provenance.

The runtime lifecycle is:

- `INSERT`: project the new row into a canonical compound identity and role
  edges, record bindings, and reactivate or insert graph edges as needed
- `UPDATE`: diff the old and new projected edge sets, delete obsolete bindings,
  tombstone edges only when no other binding still supports them, and insert any
  new bindings
- `DELETE`: delete the row's bindings and tombstone graph edges only if that
  binding was the last remaining source for the same triple

If any mapped role column is `NULL`, the row projects no compound. Duplicate
relational rows can still project the same compound; the compound remains live
until the last supporting row is deleted or updated away.

## Execution Notes

The evaluator keeps an in-memory edge cache for the predicates needed by the
current query plan and falls back to scanning the full visible graph when an
edge constraint has a variable predicate. Results are returned as text bindings,
not internal vertex ids.

The repository also ships a current benchmark script in
`sql/liquid_bench.sql` and a `make bench BENCH_DB=<database>` target for
repeatable smoke benchmarking against the current Liquid-blog surface.
