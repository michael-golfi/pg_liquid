drop extension if exists pg_liquid cascade;

create extension pg_liquid;

select count(*) as seeded_agent_context_count
from liquid.query($$
DefCompound("AgentContext", "viewer", "0", "liquid/string").
DefCompound("AgentContext", "user", "0", "liquid/string").
DefCompound("AgentContext", "topic", "0", "liquid/string").
DefCompound("AgentContext", "summary", "0", "liquid/string").
Edge("AgentContext", "liquid/mutable", "false").
CompoundReadByRole@(compound_type="AgentContext", role="viewer").
AgentContext@(viewer="agent:support", user="user:alice", topic="billing", summary="Alice billing context").
AgentContext@(viewer="agent:support", user="user:bob", topic="shipping", summary="Bob shipping context").
AgentContext@(viewer="agent:finance", user="user:bob", topic="invoice", summary="Bob invoice context").
AgentContext@(cid=cid, viewer=viewer_id, user=user_id, topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, user_id text, topic text, summary text);

select user_id,
       topic,
       summary
from liquid.read_as('agent:support', $$
AgentContext@(cid=cid, viewer=viewer_id, user=user_id, topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, user_id text, topic text, summary text)
order by 1, 2, 3;

select count(*) as hidden_context_summary_edges
from liquid.read_as('agent:support', $$
Edge(subject_literal, "summary", object_literal)?
$$) as t(subject_literal text, object_literal text);

select count(*) as bob_support_context_count
from liquid.read_as('agent:support', $$
AgentContext@(cid=cid, viewer=viewer_id, user="user:bob", topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, topic text, summary text);

select user_id,
       topic,
       summary
from liquid.read_as('agent:finance', $$
AgentContext@(cid=cid, viewer=viewer_id, user=user_id, topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, user_id text, topic text, summary text)
order by 1, 2, 3;

select count(*) as untrusted_context_count
from liquid.read_as('agent:untrusted', $$
AgentContext@(cid=cid, viewer=viewer_id, user=user_id, topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, user_id text, topic text, summary text);

select count(*) as unscoped_context_count
from liquid.query($$
AgentContext@(cid=cid, viewer=viewer_id, user=user_id, topic=topic, summary=summary)?
$$) as t(cid text, viewer_id text, user_id text, topic text, summary text);
