set client_min_messages = warning;

drop extension if exists pg_liquid cascade;

create extension pg_liquid;

select liquid._ensure_memory_extraction_schema();

select count(*) as seeded_turn_count
from liquid.query($$
Edge("session:alice-mobile", "liquid/acts_for", "user:alice").
Edge("session:bob-mobile", "liquid/acts_for", "user:bob").

ConversationTurn@(user="user:alice", conversation_id="thread-1", turn_id="turn-1").
ConversationTurn@(user="user:bob", conversation_id="thread-2", turn_id="turn-2").

ConversationEpisode@(user="user:alice", conversation_id="thread-1", episode_id="episode-1").
ConversationEpisode@(user="user:bob", conversation_id="thread-2", episode_id="episode-2").

ConversationTurn@(cid=cid, user=user_id, conversation_id=conversation_id, turn_id=turn_id)?
$$) as t(cid text, user_id text, conversation_id text, turn_id text);

select conversation_id,
       turn_id
from liquid.read_as('session:alice-mobile', $$
ConversationTurn@(cid=cid, user="user:alice", conversation_id=conversation_id, turn_id=turn_id)?
$$) as t(cid text, conversation_id text, turn_id text)
order by 1, 2;

select conversation_id,
       episode_id
from liquid.read_as('session:alice-mobile', $$
ConversationEpisode@(cid=cid, user="user:alice", conversation_id=conversation_id, episode_id=episode_id)?
$$) as t(cid text, conversation_id text, episode_id text)
order by 1, 2;

select count(*) as bob_visible_alice_turn_count
from liquid.read_as('session:bob-mobile', $$
ConversationTurn@(cid=cid, user="user:alice", conversation_id=conversation_id, turn_id=turn_id)?
$$) as t(cid text, conversation_id text, turn_id text);
