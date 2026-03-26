# Model Compounds

Compounds let you represent structured records in Liquid syntax while still storing everything in graph form.

## Step 1. Define a Compound Type

```sql
select cid, user_name, domain
from liquid.query($$
  DefCompound("Email", "user", "0", "liquid/string").
  DefCompound("Email", "domain", "0", "liquid/string").
  Edge("Email", "liquid/mutable", "false").

  Email@(user="root", domain="example.com").
  Email@(cid=cid, user=user_name, domain=domain)?
$$) as t(cid text, user_name text, domain text);
```

## Step 2. Understand `cid`

The compound identity is a canonical Liquid literal built from the type and roles.

- `cid=` is optional when asserting
- `cid=` is useful when querying or joining compounds with edges

## Step 3. Join Compounds with Other Facts

```sql
select cid, label
from liquid.query($$
  DefCompound("Task", "label", "0", "liquid/string").
  Edge("Task", "liquid/mutable", "false").

  Task@(label="write_docs").
  Edge("workflow:docs", "has_task", "Task@(label='write_docs')").

  Task@(cid=cid, label=label)?
$$) as t(cid text, label text);
```

## Step 4. Use Compounds as Your Public Model

Compounds are a good fit for:

- typed domain records
- claims or observations
- workflow entities
- security grants
- ontology statements with structured authorship

## Step 5. Know the Storage Model

Compounds are still graph data:

- one compound vertex
- one edge per role
- canonical relative identity string as the vertex literal

## Next

- [Build an ontology](./ontologies.md)
- [Use CLS and principal scopes](./security.md)
