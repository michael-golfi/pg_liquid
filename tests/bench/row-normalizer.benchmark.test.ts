// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  resetLiquidSchema,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: row_normalizer_*', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_normalizer');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('times backfill and trigger-maintained updates', async () => {
    await db.sql`
      create table public.asset_registry (
        asset_id text primary key,
        site_code text not null,
        workstream text not null
      )
    `;

    await db.sql`
      insert into public.asset_registry
      select
        concat('asset-', series_id::text),
        concat('site-', ((series_id - 1) % 5 + 1)::text),
        concat('stream-', ((series_id - 1) % 8 + 1)::text)
      from generate_series(1, 250) as series(series_id)
    `;

    await db.sql`
      select *
      from liquid.query(
        ${joinProgram([
          'DefCompound("AssetRecord", "asset_id", "0", "liquid/string").',
          'DefCompound("AssetRecord", "site_code", "0", "liquid/string").',
          'DefCompound("AssetRecord", "workstream", "0", "liquid/string").',
          'Edge("AssetRecord", "liquid/mutable", "false").',
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?',
        ])}
      ) as t(asset_id text, site_code text, workstream text)
    `;

    const backfill = await timedMs(async () => {
      await db.sql`
        select liquid.create_row_normalizer(
          'public.asset_registry'::regclass,
          'asset_registry_norm',
          'AssetRecord',
          '{"asset_id":"asset_id","site_code":"site_code","workstream":"workstream"}'::jsonb,
          true
        )
      `;
      const rows = await db.sql<Array<{ count: string }>>`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });

    expect(backfill.value).toBe(250);
    expect(backfill.elapsedMs).toBeGreaterThan(0);

    const update = await timedMs(async () => {
      await db.sql`
        update public.asset_registry
        set site_code = 'site-updated'
        where asset_id in ('asset-1', 'asset-2', 'asset-3')
      `;
      const rows = await db.sql<Array<{ count: string }>>`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
        where asset_id in ('asset-1', 'asset-2', 'asset-3')
          and site_code = 'site-updated'
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });

    expect(update.value).toBe(3);
    expect(update.elapsedMs).toBeGreaterThan(0);
  });
});
