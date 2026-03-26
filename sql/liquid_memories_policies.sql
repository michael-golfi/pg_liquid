set client_min_messages = warning;

drop extension if exists pg_liquid cascade;

create extension pg_liquid;

select count(*) as seeded_conversation_memory_count
from liquid.query($$
DefPred("owner", "1", "liquid/node", "0", "liquid/string").
DefPred("conversation_note", "1", "liquid/node", "0", "liquid/string").

DefCompound("UserProfile", "user", "0", "liquid/string").
DefCompound("UserProfile", "display_name", "0", "liquid/string").
DefCompound("UserProfile", "timezone", "0", "liquid/string").
Edge("UserProfile", "liquid/mutable", "false").

DefCompound("ConversationMemory", "user", "0", "liquid/string").
DefCompound("ConversationMemory", "conversation_id", "0", "liquid/string").
DefCompound("ConversationMemory", "fact_key", "0", "liquid/string").
DefCompound("ConversationMemory", "fact_value", "0", "liquid/string").
Edge("ConversationMemory", "liquid/mutable", "false").

Edge("session:alice-mobile", "liquid/acts_for", "user:alice").
Edge("session:bob-mobile", "liquid/acts_for", "user:bob").

UserProfile@(user="user:alice", display_name="Alice Example", timezone="America/Toronto").
UserProfile@(user="user:bob", display_name="Bob Example", timezone="America/Chicago").

ConversationMemory@(user="user:alice", conversation_id="conv:alice:1", fact_key="summary_style", fact_value="concise").
ConversationMemory@(user="user:alice", conversation_id="conv:alice:1", fact_key="review_depth", fact_value="detailed").
ConversationMemory@(user="user:bob", conversation_id="conv:bob:1", fact_key="focus_area", fact_value="reliability").

Edge("thread:alice:1", "owner", "user:alice").
Edge("thread:bob:1", "owner", "user:bob").
Edge("thread:alice:1", "conversation_note", "Alice prefers concise status updates").
Edge("thread:bob:1", "conversation_note", "Bob owns reliability follow-ups").

ConversationMemory@(cid=cid, user=user_id, conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, user_id text, conversation_id text, fact_key text, fact_value text);

select count(*) as alice_visible_profiles_without_compound_policy
from liquid.read_as('session:alice-mobile', $$
UserProfile@(cid=cid, user=user_id, display_name=display_name, timezone=timezone)?
$$) as t(cid text, user_id text, display_name text, timezone text);

select count(*) as alice_visible_memories_without_compound_policy
from liquid.read_as('session:alice-mobile', $$
ConversationMemory@(cid=cid, user=user_id, conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, user_id text, conversation_id text, fact_key text, fact_value text);

select count(*) as alice_visible_notes_without_predicate_policy
from liquid.read_as('session:alice-mobile', $$
Edge(subject_literal, "conversation_note", note_text)?
$$) as t(subject_literal text, note_text text);

select count(*) as enabled_compound_policy_count
from liquid.query($$
CompoundReadByRole@(compound_type="UserProfile", role="user").
CompoundReadByRole@(compound_type="ConversationMemory", role="user").
CompoundReadByRole@(cid=cid, compound_type=compound_type, role=role)?
$$) as t(cid text, compound_type text, role text)
where compound_type in ('UserProfile', 'ConversationMemory');

select user_id,
       display_name,
       timezone
from liquid.read_as('session:alice-mobile', $$
UserProfile@(cid=cid, user=user_id, display_name=display_name, timezone=timezone)?
$$) as t(cid text, user_id text, display_name text, timezone text)
order by 1, 2, 3;

select conversation_id,
       fact_key,
       fact_value
from liquid.read_as('session:alice-mobile', $$
ConversationMemory@(cid=cid, user="user:alice", conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, conversation_id text, fact_key text, fact_value text)
order by 1, 2, 3;

select count(*) as bob_visible_alice_profile_count
from liquid.read_as('session:bob-mobile', $$
UserProfile@(cid=cid, user="user:alice", display_name=display_name, timezone=timezone)?
$$) as t(cid text, display_name text, timezone text);

select count(*) as bob_visible_alice_memory_count
from liquid.read_as('session:bob-mobile', $$
ConversationMemory@(cid=cid, user="user:alice", conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, conversation_id text, fact_key text, fact_value text);

select conversation_id,
       fact_key,
       fact_value
from liquid.read_as('session:bob-mobile', $$
ConversationMemory@(cid=cid, user="user:bob", conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, conversation_id text, fact_key text, fact_value text)
order by 1, 2, 3;

select count(*) as untrusted_memory_count
from liquid.read_as('session:untrusted', $$
ConversationMemory@(cid=cid, user=user_id, conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, user_id text, conversation_id text, fact_key text, fact_value text);

select count(*) as enabled_predicate_policy_count
from liquid.query($$
PredicateReadBySubject@(predicate="conversation_note", relation="owner").
PredicateReadBySubject@(cid=cid, predicate=predicate, relation=relation)?
$$) as t(cid text, predicate text, relation text)
where predicate = 'conversation_note'
  and relation = 'owner';

select subject_literal,
       note_text
from liquid.read_as('session:alice-mobile', $$
Edge(subject_literal, "conversation_note", note_text)?
$$) as t(subject_literal text, note_text text)
order by 1, 2;

select subject_literal,
       note_text
from liquid.read_as('session:bob-mobile', $$
Edge(subject_literal, "conversation_note", note_text)?
$$) as t(subject_literal text, note_text text)
order by 1, 2;

select count(*) as untrusted_note_count
from liquid.read_as('session:untrusted', $$
Edge(subject_literal, "conversation_note", note_text)?
$$) as t(subject_literal text, note_text text);

select count(*) as hidden_memory_value_edges
from liquid.read_as('session:alice-mobile', $$
Edge(subject_literal, "fact_value", object_literal)?
$$) as t(subject_literal text, object_literal text);

select count(*) as unscoped_memory_count
from liquid.query($$
ConversationMemory@(cid=cid, user=user_id, conversation_id=conversation_id, fact_key=fact_key, fact_value=fact_value)?
$$) as t(cid text, user_id text, conversation_id text, fact_key text, fact_value text);
