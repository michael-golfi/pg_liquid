# Build an Ontology

This guide shows one practical way to represent ontology structures with
`pg_liquid`, including validation and a relational projection path.

## Step 1. Define Core Predicates

Start with a small vocabulary for a library catalog:

```sql
select *
from liquid.query($$
  DefPred("onto/preferred_label", "1", "liquid/node", "0", "liquid/string").
  DefPred("onto/subclass_of", "0", "liquid/node", "0", "liquid/node").
  DefPred("onto/instance_of", "0", "liquid/node", "0", "liquid/node").

  Edge("class/Thing", "onto/preferred_label", "Thing").
  Edge("class/LibraryItem", "onto/preferred_label", "Library item").
  Edge("class/Book", "onto/preferred_label", "Book").
  Edge("class/ReferenceBook", "onto/preferred_label", "Reference book").
  Edge("class/LibraryItem", "onto/subclass_of", "class/Thing").
  Edge("class/Book", "onto/subclass_of", "class/LibraryItem").
  Edge("class/ReferenceBook", "onto/subclass_of", "class/Book").
  Edge("item/isbn-9780131103627", "onto/instance_of", "class/ReferenceBook").

  Edge(subject, predicate, object)?
$$) as t(subject text, predicate text, object text);
```

## Step 2. Validate Taxonomy And Instances

`0.1.6` adds read-only helper APIs for ontology checks:

```sql
select issue_type, subject_literal, related_literal, detail
from liquid.validate_taxonomy('onto/subclass_of')
order by 1, 2, 3;
```

```sql
select issue_type, subject_literal, related_literal, detail
from liquid.validate_instances('onto/instance_of', 'onto/subclass_of')
order by 1, 2, 3;
```

The validator surfaces catch self-parenting, cycles, placeholder class
references, dangling instance targets, and instance references into invalid
classes.

## Step 3. Query Subclass Closure

```sql
select inferred_type
from liquid.query($$
  DefPred("onto/subclass_of", "0", "liquid/node", "0", "liquid/node").
  DefPred("onto/instance_of", "0", "liquid/node", "0", "liquid/node").

  Edge("class/LibraryItem", "onto/subclass_of", "class/Thing").
  Edge("class/Book", "onto/subclass_of", "class/LibraryItem").
  Edge("class/ReferenceBook", "onto/subclass_of", "class/Book").
  Edge("item/isbn-9780131103627", "onto/instance_of", "class/ReferenceBook").

  TypeOf(x, t) :- Edge(x, "onto/instance_of", t).
  TypeOf(x, sup) :- TypeOf(x, t), Edge(t, "onto/subclass_of", sup).

  TypeOf("item/isbn-9780131103627", inferred_type)?
$$) as t(inferred_type text)
order by 1;
```

## Step 4. Model Curated Claims As Compounds

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
    subject="item/isbn-9780131103627",
    predicate="catalog/has_curator_tag",
    object="tag/reference_only",
    confidence="0.98"
  ).

  OntologyClaim@(subject=subject, predicate=predicate, object=object, confidence=confidence)?
$$) as t(subject text, predicate text, object text, confidence text);
```

## Step 5. Project Relational Rows Into The Same Ontology

If your source of truth is relational, define a compound and project it:

```sql
select liquid.create_row_normalizer(
  'public.catalog_holdings'::regclass,
  'catalog_holdings_norm',
  'CatalogHolding',
  '{"holding_id":"holding_id","work_id":"work_id","branch_id":"branch_id"}'::jsonb,
  true
);
```

## Step 6. Recommended Ontology Pattern

- use `Edge(...)` for base taxonomy facts
- run `validate_taxonomy(...)` and `validate_instances(...)` before publishing
  new ontology slices
- use compounds for authored claims, provenance, and workflow metadata
- keep inference rules query-local unless you explicitly materialize results

## Next

- [Explore more example domains](./examples.md)
- [Use row normalizers](./normalizers.md)
- [Use security and principal scopes](./security.md)
