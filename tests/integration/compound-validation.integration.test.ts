// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  resetLiquidSchema,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('integration: compound schema validation', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_compound');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('rejects compound inserts missing required roles', async () => {
    const invalidProgram = joinProgram([
      'DefCompound("ValidatedEmail", "user", "0", "liquid/string").',
      'DefCompound("ValidatedEmail", "domain", "0", "liquid/string").',
      'ValidatedEmail@(user="root").',
      'ValidatedEmail@(cid=cid, user=account_user, domain=domain)?',
    ]);

    await expect(
      db.sql`
        select *
        from liquid.query(${invalidProgram}) as t(cid text, account_user text, domain text)
      `,
    ).rejects.toMatchObject({ code: '22023' });
  });

  it('accepts compound inserts that match schema', async () => {
    const validProgram = joinProgram([
      'DefCompound("ValidatedEmail", "user", "0", "liquid/string").',
      'DefCompound("ValidatedEmail", "domain", "0", "liquid/string").',
      'ValidatedEmail@(user="root", domain="example.com").',
      'ValidatedEmail@(cid=cid, user=account_user, domain=domain)?',
    ]);

    const rows = await db.sql<Array<{ cid: string; account_user: string; domain: string }>>`
      select *
      from liquid.query(${validProgram}) as t(cid text, account_user text, domain text)
    `;

    expect(rows).toHaveLength(1);
    expect(rows[0]?.account_user).toBe('root');
    expect(rows[0]?.domain).toBe('example.com');
  });
});
