# Query a Graph

Use this guide once you are comfortable asserting `Edge(...)` facts.

## Step 1. Load Facts in the Same Program

```sql
select source, target
from liquid.query($$
  Edge("a", "path", "b").
  Edge("b", "path", "c").
  Edge("c", "path", "d").
  Edge(source, "path", target)?
$$) as t(source text, target text)
order by 1, 2;
```

## Step 2. Add Query-Local Rules

```sql
select target
from liquid.query($$
  Edge("a", "path", "b").
  Edge("b", "path", "c").
  Edge("c", "path", "d").

  Reach(x, y) :- Edge(x, "path", y).
  Reach(x, z) :- Reach(x, y), Reach(y, z).

  Reach("a", target)?
$$) as t(target text)
order by 1;
```

## Step 3. Reverse the Query Direction

```sql
select source
from liquid.query($$
  Edge("a", "path", "b").
  Edge("b", "path", "c").
  Edge("c", "path", "d").

  Reach(x, y) :- Edge(x, "path", y).
  Reach(x, z) :- Reach(x, y), Reach(y, z).

  Reach(source, "d")?
$$) as t(source text)
order by 1;
```

## Step 4. Combine Multiple Predicates

```sql
select person, skill
from liquid.query($$
  Edge("alice", "works_on", "project:search").
  Edge("alice", "uses", "postgres").
  Edge("bob", "works_on", "project:search").
  Edge("bob", "uses", "typescript").

  TeamSkill(person, skill) :-
    Edge(person, "works_on", "project:search"),
    Edge(person, "uses", skill).

  TeamSkill(person, skill)?
$$) as t(person text, skill text)
order by 1, 2;
```

## Step 5. Watch for Scope Rules

- top-level assertions run before the query
- rules are local to one `liquid.query(...)` call
- only one terminal query is allowed

## Useful Patterns

- reachability and ancestry
- dependency traversal
- workflow state transitions
- ontology subclass closure

## Next

- [Model compounds](/guides/compounds)
- [Build an ontology](/guides/ontologies)
