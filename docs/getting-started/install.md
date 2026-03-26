# Install

This guide walks through a clean `pg_liquid` install in PostgreSQL.

## 1. Pick a Supported PostgreSQL Version

`pg_liquid` is validated against PostgreSQL `14` through `18`.

## 2. Build Against Your Target Server

Use the `pg_config` for the PostgreSQL instance you will install into.

```sh
make
make install
```

If you need a specific PostgreSQL version:

```sh
PG_CONFIG=/path/to/pg_config make
PG_CONFIG=/path/to/pg_config make install
```

## 3. Create the Extension

Connect to the target database and install the extension:

```sql
create extension pg_liquid;
```

## 4. Verify the Install

```sql
select extname, extversion
from pg_extension
where extname = 'pg_liquid';

select schema_name
from information_schema.schemata
where schema_name = 'liquid';
```

You should see:

- `pg_liquid` in `pg_extension`
- the `liquid` schema present

## 5. Understand What Was Installed

The extension installs:

- graph storage tables under `liquid`
- LIquid query entrypoints
- row normalizer management functions
- bootstrap graph vocabulary needed for predicates, compounds, and policy rules

## 6. Run a First Smoke Query

```sql
select *
from liquid.query($$
  Edge("alice", "likes", "graphs").
  Edge(subject, predicate, object)?
$$) as t(subject text, predicate text, object text);
```

## 7. Next Steps

- [Create your first graph](./first-graph.md)
- [Query a graph](../guides/query-graphs.md)
- [Upgrade and operate](../guides/operations.md)
