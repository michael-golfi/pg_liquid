drop extension if exists pg_liquid cascade;

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

PredicateReadBySubject@(predicate="name", relation="owner").
PredicateReadByObject@(predicate="member_of", relation="member").
CompoundReadByRole@(compound_type="Email", role="user").

ReadPredicate@(principal="agent:auditor", predicate="name").
ReadCompound@(principal="agent:auditor", compound_type="Email").
ReadPredicate@(principal="team:audit", predicate="member_of").
ReadTriple@(principal="agent:charlie", user="agent:charlie", subject="person:bob", predicate="name", object="Bob Example").
ReadTriple@(principal="agent:hybrid", user="agent:hybrid", subject="person:bob", predicate="name", object="Bob Example").

Edge("agent:legacy", "liquid/can_read_predicate", "name").
ReadTriple@(principal="agent:legacy_triple", user="agent:legacy_triple", subject="person:bob", predicate="name", object="Bob Example").
Edge("agent:delegate", "liquid/acts_for", "team:support").
Edge("team:support", "liquid/acts_for", "agent:support").
Edge("agent:hybrid", "liquid/acts_for", "team:audit").

Edge("person:alice", "owner", "agent:support").
Edge("org:acme", "member", "agent:support").
Edge("person:alice", "name", "Alice Example").
Edge("person:bob", "name", "Bob Example").
Edge("person:alice", "member_of", "org:acme").
Email@(user="agent:support", domain="example.com").
Principal@(id="agent:support", kind="agent").
Principal@(id="team:support", kind="team").

Edge(subject_literal, predicate_literal, object_literal)?
$$) as t(subject_literal text, predicate_literal text, object_literal text)
where predicate_literal in ('name', 'member_of');

select subject_literal,
       object_literal
from liquid.query_as('agent:support', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select subject_literal,
       object_literal
from liquid.query_as('agent:support', $$
Edge(subject_literal, "member_of", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select cid,
       account_user,
       domain
from liquid.query_as('agent:support', $$
Email@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text)
order by 1, 2, 3;

select count(*) as hidden_email_role_edges
from liquid.query_as('agent:support', $$
Edge(subject_literal, "user", object_literal)?
$$) as t(subject_literal text, object_literal text);

select subject_literal,
       object_literal
from liquid.query_as('agent:auditor', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select count(*) as auditor_email_count
from liquid.query_as('agent:auditor', $$
Email@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text);

select subject_literal,
       object_literal
from liquid.query_as('agent:delegate', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select count(*) as delegated_email_count
from liquid.query_as('agent:delegate', $$
Email@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text);

select subject_literal,
       predicate_literal,
       object_literal
from liquid.query_as('agent:hybrid', $$
Edge(subject_literal, predicate_literal, object_literal)?
$$) as t(subject_literal text, predicate_literal text, object_literal text)
where predicate_literal in ('name', 'member_of')
order by 1, 2, 3;

select subject_literal,
       object_literal
from liquid.query_as('agent:charlie', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select subject_literal,
       object_literal
from liquid.query_as('agent:legacy', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select subject_literal,
       object_literal
from liquid.query_as('agent:legacy_triple', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select coalesce(nullif(current_setting('pg_liquid.policy_principal', true), ''), 'unset') as principal_after_query_as;

set pg_liquid.policy_principal = 'agent:legacy';

select count(*) as wrapped_support_name_count
from liquid.query_as('agent:support', $$
Edge(subject_literal, "name", object_literal)?
$$) as t(subject_literal text, object_literal text);

select current_setting('pg_liquid.policy_principal') as principal_after_nested_wrapper;

reset pg_liquid.policy_principal;

select principal_id,
       principal_kind
from liquid.query($$
Principal@(cid=cid, id=principal_id, kind=principal_kind)?
$$) as t(cid text, principal_id text, principal_kind text)
order by 1, 2;
