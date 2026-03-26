# Build an Ontology

This guide shows one practical way to represent ontology structures with `pg_liquid`.

## Step 1. Define Core Predicates

Start with a small vocabulary:

```sql
select *
from liquid.query($$
  DefPred("onto/preferred_label", "1", "liquid/node", "0", "liquid/string").
  DefPred("onto/subclass_of", "0", "liquid/node", "0", "liquid/node").
  DefPred("onto/instance_of", "0", "liquid/node", "0", "liquid/node").

  Edge("class/Thing", "onto/preferred_label", "Thing").
  Edge("class/Document", "onto/subclass_of", "class/Thing").
  Edge("class/ApiReference", "onto/subclass_of", "class/Document").
  Edge("concept/api_reference", "onto/instance_of", "class/ApiReference").

  Edge(subject, predicate, object)?
$$) as t(subject text, predicate text, object text);
```

## Step 2. Add Human Labels

```sql
select subject, label
from liquid.query($$
  DefPred("onto/preferred_label", "1", "liquid/node", "0", "liquid/string").
  Edge("concept/api_reference", "onto/preferred_label", "API reference").
  Edge(subject, "onto/preferred_label", label)?
$$) as t(subject text, label text);
```

## Step 3. Query Subclass Closure

```sql
select inferred_type
from liquid.query($$
  DefPred("onto/subclass_of", "0", "liquid/node", "0", "liquid/node").
  DefPred("onto/instance_of", "0", "liquid/node", "0", "liquid/node").

  Edge("class/Document", "onto/subclass_of", "class/Thing").
  Edge("class/ApiReference", "onto/subclass_of", "class/Document").
  Edge("concept/api_reference", "onto/instance_of", "class/ApiReference").

  TypeOf(x, t) :- Edge(x, "onto/instance_of", t).
  TypeOf(x, sup) :- TypeOf(x, t), Edge(t, "onto/subclass_of", sup).

  TypeOf("concept/api_reference", inferred_type)?
$$) as t(inferred_type text)
order by 1;
```

## Step 4. Model Claims as Compounds

Ontology work usually needs provenance. Compounds are a strong fit for that.

```sql
select subject, predicate, object, confidence
from liquid.query($$
  DefCompound("OntologyClaim", "subject", "0", "liquid/node").
  DefCompound("OntologyClaim", "predicate", "0", "liquid/node").
  DefCompound("OntologyClaim", "object", "0", "liquid/node").
  DefCompound("OntologyClaim", "confidence", "0", "liquid/string").
  Edge("OntologyClaim", "liquid/mutable", "false").

  OntologyClaim@(
    subject="concept/api_reference",
    predicate="onto/has_property",
    object="property/authoritative",
    confidence="0.88"
  ).

  OntologyClaim@(subject=subject, predicate=predicate, object=object, confidence=confidence)?
$$) as t(subject text, predicate text, object text, confidence text);
```

## Step 5. Recommended Ontology Pattern

- use `Edge(...)` for base taxonomy facts
- use compounds for authored claims, provenance, and workflow metadata
- keep inference rules query-local unless you explicitly materialize results

## Next

- [Use row normalizers](./normalizers.md)
- [Use security and principal scopes](./security.md)
