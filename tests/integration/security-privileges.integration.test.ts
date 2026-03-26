// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import postgres from 'postgres';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  type PgLiquidTestDb,
  resetLiquidSchema,
} from '../helpers/pg-liquid-test-helpers.js';

describe('integration: function privilege hardening', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_privileges');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('uses narrowed search_path settings on security-definer functions', async () => {
    const rows = await db.sql<Array<{ function_name: string; config: string[] | null }>>`
      select
        p.proname as function_name,
        p.proconfig as config
      from pg_proc p
      join pg_namespace n
        on n.oid = p.pronamespace
      where n.nspname = 'liquid'
        and p.proname in (
          'read_as',
          '_ensure_edge',
          '_tombstone_edge',
          '_deproject_normalizer',
          '_apply_row_normalizer_change',
          'tg_apply_row_normalizer'
        )
      order by p.proname
    `;

    expect(rows).toHaveLength(6);
    for (const row of rows) {
      const config = row.config?.join(',') ?? '';
      expect(config).toContain('search_path=');
      expect(config).toContain('pg_catalog');
      expect(config).toContain('liquid');
      expect(config).not.toContain('public');
      expect(config).not.toContain('pg_temp');
    }
  });

  it('rejects internal and operator APIs for an unprivileged role while keeping public helpers readable', async () => {
    const roleName = `liquid_guest_${process.pid}_${Date.now()}`;
    const roleSql = postgres({
      database: db.dbName,
      max: 1,
      idle_timeout: 5,
      connect_timeout: 10,
      onnotice: () => undefined,
    });

    await db.sql.unsafe(`create role ${roleName}`);

    try {
      await db.sql.unsafe(`grant usage on schema liquid to ${roleName}`);

      await roleSql.unsafe(`set role ${roleName}`);

      const publicRows = await roleSql`
        select liquid.compound_roles('UnknownCompound') as roles
      `;
      expect(publicRows[0]?.roles ?? []).toEqual([]);

      await expect(
        roleSql.unsafe(`select liquid._ensure_edge('asset/a', 'rel/owns', 'asset/b')`),
      ).rejects.toMatchObject({
        message: expect.stringContaining('permission denied for function _ensure_edge'),
      });

      await expect(
        roleSql.unsafe(
          `select liquid.create_row_normalizer('pg_catalog.pg_class'::regclass, 'audit', 'AuditRow', '{}'::jsonb, false)`,
        ),
      ).rejects.toMatchObject({
        message: expect.stringContaining('permission denied for function create_row_normalizer'),
      });
    } finally {
      try {
        await roleSql.unsafe('reset role');
      } finally {
        await roleSql.end({ timeout: 5 });
        await db.sql.unsafe(`revoke usage on schema liquid from ${roleName}`);
        await db.sql.unsafe(`drop role if exists ${roleName}`);
      }
    }
  });
});
