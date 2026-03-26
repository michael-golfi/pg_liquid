# Add Row Normalizers

Row normalizers project relational rows into Liquid compounds.

## When to Use Them

Use a row normalizer when:

- your source of truth is a PostgreSQL table
- you want Liquid-style compound queries over those rows
- you want updates to stay synchronized automatically

## Step 1. Create a Source Table

```sql
create table account_profiles (
  account_id text primary key,
  display_name text not null,
  tier text not null
);
```

## Step 2. Define The Compound Type

```sql
select *
from liquid.query($$
  DefCompound("AccountProfile", "account_id", "0", "liquid/string").
  DefCompound("AccountProfile", "display_name", "0", "liquid/string").
  DefCompound("AccountProfile", "tier", "0", "liquid/string").
  Edge("AccountProfile", "liquid/mutable", "false").
  AccountProfile@(account_id=account_id, display_name=display_name, tier=tier)?
$$) as t(account_id text, display_name text, tier text);
```

## Step 3. Create The Normalizer

```sql
select liquid.create_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer',
  'AccountProfile',
  '{
    "account_id": "account_id",
    "display_name": "display_name",
    "tier": "tier"
  }'::jsonb,
  true
);
```

## Step 4. Query The Generated Compound View

```sql
select account_id, display_name, tier
from liquid.query($$
  AccountProfile@(account_id=account_id, display_name=display_name, tier=tier)?
$$) as t(account_id text, display_name text, tier text);
```

## Step 5. Rebuild Or Remove It

```sql
select liquid.rebuild_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer'
);

select liquid.drop_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer',
  true
);
```

## Notes

- normalizers are operator-facing APIs, not a replacement for extension installs
- use them for canonical relational data that should also be queryable as
  Liquid compounds
