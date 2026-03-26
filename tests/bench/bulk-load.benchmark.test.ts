// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  generateBulkLoadAssertions,
  joinProgram,
  liquidCount,
  resetLiquidSchema,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: bulk_load_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_bulk');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('loads and projects user-session edges', async () => {
    const benchN = 1200;
    const program = joinProgram([
      generateBulkLoadAssertions(benchN),
      'Edge(user_id, "auth/has_session", session_id)?',
    ]);

    const result = await timedMs(async () =>
      liquidCount(db.sql, program, 'user_id text, session_id text'),
    );

    expect(result.value).toBe(benchN);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
