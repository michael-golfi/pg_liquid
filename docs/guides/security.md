# Use CLS and Principal Scopes

`pg_liquid` includes extension-level read filtering for graph and compound data.

## The Three Entry Points

- `liquid.query(program)`: unrestricted by default unless
  `pg_liquid.policy_principal` is set
- `liquid.query_as(principal, program)`: trusted wrapper for principal-bound
  reads or writes
- `liquid.read_as(principal, program)`: least-privilege read wrapper; rejects
  top-level assertions

## Step 1. Define What Can Be Read

```sql
select *
from liquid.query($$
  DefCompound("UserSignal", "user", "0", "liquid/node").
  DefCompound("UserSignal", "value", "0", "liquid/string").
  Edge("UserSignal", "liquid/mutable", "false").

  CompoundReadByRole@(compound_type="UserSignal", role="user").

  UserSignal@(user="user:alice", value="prefers_async_updates").
  UserSignal@(user="user:bob", value="prefers_dashboards").
  UserSignal@(cid=cid, user=user_id, value=value)?
$$) as t(cid text, user_id text, value text);
```

## Step 2. Read as a Principal

```sql
select user_id, value
from liquid.read_as('user:alice', $$
  UserSignal@(cid=cid, user=user_id, value=value)?
$$) as t(cid text, user_id text, value text);
```

Only compounds whose guarded role matches `user:alice` are visible.

## Step 3. Add Principal Inheritance

```sql
select *
from liquid.query($$
  Edge("session:alice-app", "liquid/acts_for", "user:alice").
$$) as t(dummy text);
```

Now a bound session principal can inherit the user’s effective scope.

## Step 4. Keep PostgreSQL ACLs in Place

`pg_liquid` policy rules do not replace ordinary PostgreSQL permissions.

Recommended production baseline:

```sql
revoke all on schema liquid from public;
revoke all on all tables in schema liquid from public;
revoke all on all functions in schema liquid from public;

grant usage on schema liquid to app_user;
grant execute on function liquid.read_as(text, text) to app_user;
grant execute on function liquid.compound_roles(text) to app_user;
grant execute on function liquid.validate_taxonomy(text) to app_user;
grant execute on function liquid.validate_instances(text, text) to app_user;
```

`0.1.6` also revokes the internal `liquid._*` helpers, trigger entrypoint, and
row normalizer management functions from `public` by default. Keep those grants
for operator or trusted server roles only.
