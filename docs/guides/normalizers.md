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

## Step 2. Create the Normalizer

```sql
select liquid.create_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer',
  'AccountProfile',
  '{
    "account_id": "id",
    "display_name": "display_name",
    "tier": "tier"
  }'::jsonb,
  true
);
```

## Step 3. Query the Generated Compound View

```sql
select id, display_name, tier
from liquid.query($$
  AccountProfile@(id=id, display_name=display_name, tier=tier)?
$$) as t(id text, display_name text, tier text);
```

## Step 4. Rebuild When Needed

```sql
select liquid.rebuild_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer'
);
```

## Step 5. Remove It Cleanly

```sql
select liquid.drop_row_normalizer(
  'account_profiles'::regclass,
  'account_profile_normalizer',
  true
);
```

## Notes

- normalizers are operational APIs, not a replacement for extension installs
- use them for canonical relational data that should also be queryable as Liquid compounds
