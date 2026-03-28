// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  resetLiquidSchema,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('integration: memory extraction write surface', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_memory_extraction');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('bootstraps conversation turn and episode compounds behind the canonical user-scoped read model', async () => {
    await db.sql`select liquid._ensure_memory_extraction_schema()`;

    await db.sql`
      select count(*)
      from liquid.query(
        ${joinProgram([
          'Edge("session:alice-mobile", "liquid/acts_for", "user:alice").',
          'Edge("session:bob-mobile", "liquid/acts_for", "user:bob").',
          'ConversationTurn@(user="user:alice", conversation_id="thread-1", turn_id="turn-1").',
          'ConversationTurn@(user="user:bob", conversation_id="thread-2", turn_id="turn-2").',
          'ConversationEpisode@(user="user:alice", conversation_id="thread-1", episode_id="episode-1").',
          'ConversationEpisode@(user="user:bob", conversation_id="thread-2", episode_id="episode-2").',
          'ConversationEpisode@(cid=cid, user=user_id, conversation_id=conversation_id, episode_id=episode_id)?',
        ])}
      ) as t(cid text, user_id text, conversation_id text, episode_id text)
    `;

    const aliceTurns = await db.sql<Array<{ conversation_id: string; turn_id: string }>>`
      select conversation_id, turn_id
      from liquid.read_as(
        'session:alice-mobile',
        ${joinProgram([
          'ConversationTurn@(cid=cid, user="user:alice", conversation_id=conversation_id, turn_id=turn_id)?',
        ])}
      ) as t(cid text, conversation_id text, turn_id text)
      order by conversation_id, turn_id
    `;
    const aliceEpisodes = await db.sql<Array<{ conversation_id: string; episode_id: string }>>`
      select conversation_id, episode_id
      from liquid.read_as(
        'session:alice-mobile',
        ${joinProgram([
          'ConversationEpisode@(cid=cid, user="user:alice", conversation_id=conversation_id, episode_id=episode_id)?',
        ])}
      ) as t(cid text, conversation_id text, episode_id text)
      order by conversation_id, episode_id
    `;
    const bobVisibleAliceTurns = await db.sql<Array<{ turn_id: string }>>`
      select turn_id
      from liquid.read_as(
        'session:bob-mobile',
        ${joinProgram([
          'ConversationTurn@(cid=cid, user="user:alice", conversation_id=conversation_id, turn_id=turn_id)?',
        ])}
      ) as t(cid text, conversation_id text, turn_id text)
    `;

    expect(aliceTurns).toEqual([{ conversation_id: 'thread-1', turn_id: 'turn-1' }]);
    expect(aliceEpisodes).toEqual([{ conversation_id: 'thread-1', episode_id: 'episode-1' }]);
    expect(bobVisibleAliceTurns).toEqual([]);
  });

  it('applies profile and conversation memory batches with deterministic update and retract semantics', async () => {
    await db.sql`
      select liquid.apply_memory_extraction(
        'agent:extractor',
        ${db.sql.json({
          runId: 'run-1',
          threadId: 'thread-1',
          observedAt: '2026-03-28T18:00:00.000Z',
          memories: [
            {
              scope: 'profile',
              userId: 'user:alice',
              memoryKey: 'diet_type',
              memoryValue: 'vegan',
              supportRef: 'message:1',
              confidence: 0.9,
              operation: 'upsert',
            },
            {
              scope: 'conversation',
              userId: 'user:alice',
              conversationId: 'thread-1',
              memoryKey: 'summary',
              memoryValue: 'Prefers concise planning updates',
              supportRef: 'message:2',
              confidence: 0.8,
              operation: 'upsert',
            },
          ],
        })}
      )
    `;

    const firstProfileProgram = joinProgram([
      'ProfileSnapshot(memory_key, memory_value, status) :-',
      '  ProfileMemory@(cid=cid, user="user:alice", memory_key=memory_key),',
      '  Edge(cid, "liquid/memory_value", memory_value),',
      '  Edge(cid, "liquid/memory_status", status).',
      'ProfileSnapshot(memory_key, memory_value, status)?',
    ]);
    const firstConversationProgram = joinProgram([
      'ConversationSnapshot(memory_key, memory_value, status) :-',
      '  ConversationMemory@(cid=cid, user="user:alice", conversation_id="thread-1", memory_key=memory_key),',
      '  Edge(cid, "liquid/memory_value", memory_value),',
      '  Edge(cid, "liquid/memory_status", status).',
      'ConversationSnapshot(memory_key, memory_value, status)?',
    ]);
    const firstProfile = await db.sql<Array<{ memory_key: string; memory_value: string; status: string }>>`
      select memory_key, memory_value, status
      from liquid.query(${firstProfileProgram}) as t(memory_key text, memory_value text, status text)
      order by memory_key
    `;
    const firstConversation = await db.sql<Array<{ memory_key: string; memory_value: string; status: string }>>`
      select memory_key, memory_value, status
      from liquid.query(${firstConversationProgram}) as t(memory_key text, memory_value text, status text)
      order by memory_key
    `;

    expect(firstProfile).toEqual([{ memory_key: 'diet_type', memory_value: 'vegan', status: 'active' }]);
    expect(firstConversation).toEqual([
      { memory_key: 'summary', memory_value: 'Prefers concise planning updates', status: 'active' },
    ]);

    await db.sql`
      select liquid.apply_memory_extraction(
        'agent:extractor',
        ${db.sql.json({
          runId: 'run-1',
          threadId: 'thread-1',
          observedAt: '2026-03-28T18:00:00.000Z',
          memories: [
            {
              scope: 'profile',
              userId: 'user:alice',
              memoryKey: 'diet_type',
              memoryValue: 'vegan',
              supportRef: 'message:1',
              confidence: 0.9,
              operation: 'upsert',
            },
            {
              scope: 'conversation',
              userId: 'user:alice',
              conversationId: 'thread-1',
              memoryKey: 'summary',
              memoryValue: 'Prefers concise planning updates',
              supportRef: 'message:2',
              confidence: 0.8,
              operation: 'upsert',
            },
          ],
        })}
      )
    `;

    const replayedProfile = await db.sql<Array<{ memory_key: string; memory_value: string; status: string }>>`
      select memory_key, memory_value, status
      from liquid.query(${firstProfileProgram}) as t(memory_key text, memory_value text, status text)
      order by memory_key
    `;
    const replayedConversation = await db.sql<Array<{ memory_key: string; memory_value: string; status: string }>>`
      select memory_key, memory_value, status
      from liquid.query(${firstConversationProgram}) as t(memory_key text, memory_value text, status text)
      order by memory_key
    `;
    const replayedExtractorRuns = await db.sql<Array<{ run_id: string }>>`
      select run_id
      from liquid.query($$
        ExtractorRun@(cid=cid, principal="agent:extractor", run_id=run_id, thread_id="thread-1", observed_at=observed_at)?
      $$) as t(cid text, run_id text, observed_at text)
      order by run_id
    `;
    const replayedSupportRefs = await db.sql<Array<{ support_ref: string }>>`
      select support_ref
      from liquid.query($$
        MemorySupport@(cid=cid, memory_literal=memory_literal, run_id=run_id, support_ref=support_ref)?
      $$) as t(cid text, memory_literal text, run_id text, support_ref text)
      order by support_ref
    `;

    expect(replayedProfile).toEqual(firstProfile);
    expect(replayedConversation).toEqual(firstConversation);
    expect(replayedExtractorRuns).toEqual([{ run_id: 'run-1' }]);
    expect(replayedSupportRefs).toEqual([{ support_ref: 'message:1' }, { support_ref: 'message:2' }]);

    await db.sql`
      select liquid.apply_memory_extraction(
        'agent:extractor',
        ${db.sql.json({
          runId: 'run-2',
          threadId: 'thread-1',
          observedAt: '2026-03-28T18:05:00.000Z',
          memories: [
            {
              scope: 'profile',
              userId: 'user:alice',
              memoryKey: 'diet_type',
              memoryValue: 'pescatarian',
              supportRef: 'message:3',
              confidence: 0.95,
              operation: 'upsert',
            },
            {
              scope: 'conversation',
              userId: 'user:alice',
              conversationId: 'thread-1',
              memoryKey: 'summary',
              supportRef: 'message:4',
              operation: 'retract',
            },
          ],
        })}
      )
    `;

    const updatedProfileProgram = joinProgram([
      'ProfileSnapshot(memory_key, memory_value, status) :-',
      '  ProfileMemory@(cid=cid, user="user:alice", memory_key=memory_key),',
      '  Edge(cid, "liquid/memory_value", memory_value),',
      '  Edge(cid, "liquid/memory_status", status).',
      'ProfileSnapshot(memory_key, memory_value, status)?',
    ]);
    const retractedConversationStatusProgram = joinProgram([
      'ConversationStatus(memory_key, status) :-',
      '  ConversationMemory@(cid=cid, user="user:alice", conversation_id="thread-1", memory_key=memory_key),',
      '  Edge(cid, "liquid/memory_status", status).',
      'ConversationStatus(memory_key, status)?',
    ]);
    const retractedConversationValuesProgram = joinProgram([
      'ConversationValue(memory_value) :-',
      '  ConversationMemory@(cid=cid, user="user:alice", conversation_id="thread-1", memory_key="summary"),',
      '  Edge(cid, "liquid/memory_value", memory_value).',
      'ConversationValue(memory_value)?',
    ]);
    const updatedProfile = await db.sql<Array<{ memory_key: string; memory_value: string; status: string }>>`
      select memory_key, memory_value, status
      from liquid.query(${updatedProfileProgram}) as t(memory_key text, memory_value text, status text)
      order by memory_key
    `;
    const retractedConversationStatus = await db.sql<Array<{ memory_key: string; status: string }>>`
      select memory_key, status
      from liquid.query(${retractedConversationStatusProgram}) as t(memory_key text, status text)
      order by memory_key
    `;
    const retractedConversationValues = await db.sql<Array<{ memory_value: string }>>`
      select memory_value
      from liquid.query(${retractedConversationValuesProgram}) as t(memory_value text)
    `;
    const extractorRuns = await db.sql<Array<{ run_id: string }>>`
      select run_id
      from liquid.query($$
        ExtractorRun@(cid=cid, principal="agent:extractor", run_id=run_id, thread_id="thread-1", observed_at=observed_at)?
      $$) as t(cid text, run_id text, observed_at text)
      order by run_id
    `;
    const supportRefs = await db.sql<Array<{ support_ref: string }>>`
      select support_ref
      from liquid.query($$
        MemorySupport@(cid=cid, memory_literal=memory_literal, run_id=run_id, support_ref=support_ref)?
      $$) as t(cid text, memory_literal text, run_id text, support_ref text)
      order by support_ref
    `;

    expect(updatedProfile).toEqual([{ memory_key: 'diet_type', memory_value: 'pescatarian', status: 'active' }]);
    expect(retractedConversationStatus).toEqual([{ memory_key: 'summary', status: 'retracted' }]);
    expect(retractedConversationValues).toEqual([]);
    expect(extractorRuns).toEqual([{ run_id: 'run-1' }, { run_id: 'run-2' }]);
    expect(supportRefs).toEqual([{ support_ref: 'message:1' }, { support_ref: 'message:2' }, { support_ref: 'message:3' }, { support_ref: 'message:4' }]);
  });
});
