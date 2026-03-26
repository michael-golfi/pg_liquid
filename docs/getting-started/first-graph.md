# Create Your First Graph

This is the fastest way to learn the core `pg_liquid` model.

## 1. Assert Basic Facts

Graph facts are ordinary LIquid assertions:

```sql
select *
from liquid.query($$
  Edge("alice", "knows", "bob").
  Edge("bob", "knows", "carol").
  Edge(subject, predicate, object)?
$$) as t(subject text, predicate text, object text)
order by 1, 2, 3;
```

Each `Edge(subject, predicate, object)` assertion inserts one graph fact.

## 2. Query by Predicate

```sql
select *
from liquid.query($$
  Edge("alice", "knows", "bob").
  Edge("alice", "likes", "datalog").
  Edge("alice", predicate, object)?
$$) as t(predicate text, object text)
order by 1;
```

## 3. Ask a Reachability Question

Rules are query-local. They do not persist in the database.

```sql
select reachable
from liquid.query($$
  Edge("alice", "knows", "bob").
  Edge("bob", "knows", "carol").
  Edge("carol", "knows", "dana").

  Reach(x, y) :- Edge(x, "knows", y).
  Reach(x, z) :- Reach(x, y), Reach(y, z).

  Reach("alice", reachable)?
$$) as t(reachable text)
order by 1;
```

## 4. Use Variables and Anonymous Slots

Variables are bare identifiers. `_` is anonymous.

```sql
select object
from liquid.query($$
  Edge("alice", "owns", "doc:1").
  Edge("alice", "owns", "doc:2").
  Edge("alice", "owns", object)?
$$) as t(object text)
order by 1;
```

## 5. Know the Mental Model

- assertions before the final query are evaluated first
- one terminal `?` query returns rows
- rules only exist for the duration of that call
- graph storage is still PostgreSQL storage underneath

## Next

- [Query a graph](/guides/query-graphs)
- [Model compounds](/guides/compounds)
