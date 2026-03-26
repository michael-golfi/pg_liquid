// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  liquidCount,
  resetLiquidSchema,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: read_as_lookup_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_read_as');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('performs repeated scoped reads through liquid.read_as', async () => {
    const benchN = 600;
    const seedProgram = joinProgram([
      'DefCompound("AssetAccess", "viewer", "0", "liquid/string").',
      'DefCompound("AssetAccess", "asset_id", "0", "liquid/string").',
      'DefCompound("AssetAccess", "location", "0", "liquid/string").',
      'Edge("AssetAccess", "liquid/mutable", "false").',
      'CompoundReadByRole@(compound_type="AssetAccess", role="viewer").',
      'Edge("session:ops-console", "liquid/acts_for", "viewer:ops").',
      ...Array.from(
        { length: benchN },
        (_, index) =>
          `AssetAccess@(viewer="viewer:ops", asset_id="asset/${index + 1}", location="zone/${(index % 6) + 1}").`,
      ),
      'AssetAccess@(cid=cid, viewer=viewer_id, asset_id=asset_id, location=location)?',
    ]);

    const seeded = await liquidCount(
      db.sql,
      seedProgram,
      'cid text, viewer_id text, asset_id text, location text',
    );
    expect(seeded).toBe(benchN);

    const result = await timedMs(async () => {
      const rows = await db.sql<Array<{ total: string }>>`
        with runs as (
          select generate_series(1, 40) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint as sample_count
          from liquid.read_as(
            'session:ops-console',
            'AssetAccess@(cid=cid, viewer="viewer:ops", asset_id=asset_id, location=location)?'
          ) as t(cid text, asset_id text, location text)
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });

    expect(result.value).toBe(benchN * 40);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
