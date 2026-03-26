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

describe('benchmark: base_scan_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_scan');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('scans base storage repeatedly for a predicate', async () => {
    const benchN = 1200;
    const scanRuns = 75;

    const seedProgram = joinProgram([
      generateBulkLoadAssertions(benchN),
      'Edge(subject_id, "auth/has_session", session_id)?',
    ]);
    const seeded = await liquidCount(db.sql, seedProgram, 'subject_id text, session_id text');
    expect(seeded).toBe(benchN);

    const result = await timedMs(async () => {
      const rows = await db.sql<Array<{ total: string }>>`
        with runs as (
          select generate_series(1, ${scanRuns}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint + (runs.run_id * 0)::bigint as sample_count
          from liquid.edges e
          join liquid.vertices p on p.id = e.predicate_id
          where e.is_deleted = false
            and p.literal = 'auth/has_session'
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });

    expect(result.value).toBe(benchN * scanRuns);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
