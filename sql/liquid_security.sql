set client_min_messages = warning;
drop extension if exists pg_liquid cascade;
reset client_min_messages;

create extension pg_liquid;

select count(*) as seed_edge_count
from liquid.query($$
DefPred("name", "1", "liquid/node", "0", "liquid/string").
DefPred("owner", "1", "liquid/node", "0", "liquid/string").
DefPred("member_of", "1", "liquid/node", "0", "liquid/node").
DefPred("member", "1", "liquid/node", "0", "liquid/string").
DefCompound("Email", "user", "0", "liquid/string").
DefCompound("Email", "domain", "0", "liquid/string").
Edge("Email", "liquid/mutable", "false").
Edge("name", "liquid/readable_if_subject_has", "owner").
Edge("member_of", "liquid/readable_if_object_has", "member").
Edge("Email", "liquid/readable_compound_if_role_has", "user").
Edge("person:alice", "owner", "liquid_alice").
Edge("person:alice", "name", "Alice Example").
Edge("person:bob", "name", "Bob Example").
Edge("person:alice", "member_of", "org:acme").
Edge("org:acme", "member", "liquid_bob").
Email@(user="liquid_alice", domain="example.com").
ReadTriple@(principal="liquid_charlie", user="liquid_charlie", subject="person:bob", predicate="name", object="Bob Example").
Edge(subject_literal, predicate_literal, object_literal)?
$$) as t(subject_literal text, predicate_literal text, object_literal text)
where predicate_literal in ('name', 'member_of');

set pg_liquid.policy_principal = 'liquid_alice';

select subject_literal,
       object_literal
from liquid.query($$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select cid,
       account_user,
       domain
from liquid.query($$
Email@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text);

select count(*) as hidden_email_role_edges
from liquid.query($$
Edge(subject_literal, "user", object_literal)?
$$) as t(subject_literal text, object_literal text);

reset pg_liquid.policy_principal;

drop role if exists liquid_reader;
create role liquid_reader;

revoke all on schema liquid from public;
revoke all on all tables in schema liquid from public;
revoke all on all functions in schema liquid from public;

grant usage on schema liquid to liquid_reader;
grant execute on function liquid.read_as(text, text) to liquid_reader;

select has_table_privilege('liquid_reader', 'liquid.edges', 'select') as reader_has_edge_select;

select has_function_privilege('liquid_reader', 'liquid.read_as(text, text)', 'execute') as reader_has_read_as_execute;

select has_function_privilege('liquid_reader', 'liquid.query_as(text, text)', 'execute') as reader_has_query_as_execute;

set role liquid_reader;

do $$
begin
  execute 'set pg_liquid.policy_principal = ''liquid_bob''';
exception
  when insufficient_privilege then
    raise notice 'policy_principal_set_rejected';
end $$;

select subject_literal,
       object_literal
from liquid.read_as('liquid_alice', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

do $$
begin
  perform count(*)
  from liquid.read_as('liquid_alice', $liquid$
Edge("person:eve", "name", "Eve Example").
Edge(subject_literal, "name", object_literal)?
$liquid$) as t(subject_literal text, object_literal text);
exception
  when insufficient_privilege then
    raise notice 'read_as_rejected_assertions';
end $$;

reset role;

drop owned by liquid_reader;
drop role liquid_reader;
set pg_liquid.policy_principal = 'liquid_bob';

select subject_literal,
       object_literal
from liquid.query($$
Edge(subject_literal, "member_of", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select count(*) as bob_visible_names
from liquid.query($$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text);

reset pg_liquid.policy_principal;
set pg_liquid.policy_principal = 'liquid_charlie';

select subject_literal,
       object_literal
from liquid.query($$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

reset pg_liquid.policy_principal;
