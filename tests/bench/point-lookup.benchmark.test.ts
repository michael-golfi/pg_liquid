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

describe('benchmark: point_lookup_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_point');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('performs repeated equality lookup through liquid.query', async () => {
    const benchN = 1200;
    const pointRuns = 75;
    const pointProgram = 'Edge("session/1", "auth/from_device", device_id)?';

    const seedProgram = joinProgram([
      generateBulkLoadAssertions(benchN),
      'Edge("session/1", "auth/from_device", seed_device)?',
    ]);
    const seeded = await liquidCount(db.sql, seedProgram, 'seed_device text');
    expect(seeded).toBe(1);

    const result = await timedMs(async () => {
      const rows = await db.sql<Array<{ total: string }>>`
        with runs as (
          select generate_series(1, ${pointRuns}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint as sample_count
          from liquid.query(concat('% run ', runs.run_id::text, E'\n', ${pointProgram}::text))
            as t(device_id text)
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });

    expect(result.value).toBe(pointRuns);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
