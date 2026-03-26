// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  liquidCount,
  resetLiquidSchema,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: compound_lookup_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_compound');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('reads compound rows with cid and role projection', async () => {
    const program = [
      'DefCompound("UserContact", "contact/user", "0", "liquid/string").',
      'DefCompound("UserContact", "contact/channel", "0", "liquid/string").',
      'DefCompound("UserContact", "contact/value", "0", "liquid/string").',
      'Edge("UserContact", "liquid/mutable", "false").',
      'UserContact@(contact/user="user/1", contact/channel="email", contact/value="ops@example.com").',
      'UserContact@(contact/user="user/2", contact/channel="sms", contact/value="+15551234567").',
      'UserContact@(cid=cid, contact/user=user_id, contact/channel=channel, contact/value=value)?',
    ].join('\n');

    const result = await timedMs(async () =>
      liquidCount(db.sql, program, 'cid text, user_id text, channel text, value text'),
    );

    expect(result.value).toBe(2);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
