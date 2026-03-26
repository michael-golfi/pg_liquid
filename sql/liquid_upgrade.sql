drop extension if exists pg_liquid cascade;

create extension pg_liquid version '1.0.0';

select count(*) as pre_upgrade_seed_count
from liquid.query($$
DefPred("name", "1", "liquid/node", "0", "liquid/string").
Edge("Alice", "name", "Alice Example").
DefCompound("Email", "user", "0", "liquid/string").
DefCompound("Email", "domain", "0", "liquid/string").
Edge("Email", "liquid/mutable", "false").
Email@(user="root", domain="example.com").
Edge(subject_literal, predicate_literal, object_literal)?
$$) as t(subject_literal text, predicate_literal text, object_literal text)
where subject_literal = 'Alice'
  and predicate_literal = 'name'
  and object_literal = 'Alice Example';

alter extension pg_liquid update to '0.1.0';

\t on
select proname
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'liquid'
  and p.proname in (
    'compound_identity_literal',
    'create_row_normalizer',
    'drop_row_normalizer',
    'project_compound_edges',
    'query',
    'query_as',
    'read_as',
    'rebuild_row_normalizer',
    'tg_apply_row_normalizer'
  )
order by proname;

select count(*) as normalizer_table_count
from pg_tables
where schemaname = 'liquid'
  and tablename in ('row_normalizers', 'row_normalizer_bindings');

select count(*) as legacy_function_count
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'liquid'
  and p.proname in (
    'query_subgraph',
    'insert_edge',
    'delete_edge',
    'assert_rule',
    'insert_compound',
    'get_compound_id',
    'define_predicate',
    'define_compound',
    'set_predicate_policy'
  );
\t off

select value_literal
from liquid.query($$
Edge("Alice", "name", value_literal)?
$$) as t(value_literal text);

select cid,
       account_user,
       domain
from liquid.query($$
Email@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text);

select p as predicate_name,
       sc as subject_cardinality,
       st as subject_type,
       oc as object_cardinality,
       ot as object_type
from liquid.query($$
DefPred("name", "1", "liquid/node", "0", "liquid/string").
DefPred(p, sc, st, oc, ot)?
$$) as t(p text, sc text, st text, oc text, ot text)
where p = 'name';

\t on
select 'upgrade_complete' as upgrade_status;
\t off
