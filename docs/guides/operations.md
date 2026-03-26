# Upgrade and Operate

Use this page for production-facing extension lifecycle tasks.

## Install or Upgrade

```sql
create extension pg_liquid;
```

or:

```sql
alter extension pg_liquid update to '0.1.1';
```

## Supported Upgrade Path

The extension ships upgrade scripts from legacy internal `1.x` versions to the public `0.1.x` line.

## Recommended Rollout Sequence

1. Build and install extension binaries on the PostgreSQL host.
2. Rehearse `create extension` or `alter extension ... update` in staging.
3. Run smoke queries through `liquid.query(...)` or `liquid.read_as(...)`.
4. Validate principal-scoped access if you use CLS.
5. If using row normalizers, rebuild or backfill them deliberately.

## Useful Validation Commands

```sh
make package-check
make installcheck
make bench-check
make pgxn-package
```

## GitHub and PGXN

The repository now supports:

- GitHub Actions CI
- benchmark validation
- automatic version bumping on release
- GitHub release publishing
- PGXN package publishing

## PostgreSQL Version Matrix

Current validated range:

- PostgreSQL 14
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18
