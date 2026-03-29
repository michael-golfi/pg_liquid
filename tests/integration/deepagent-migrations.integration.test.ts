// @vitest-environment node
import { fileURLToPath } from 'node:url';

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  resetLiquidSchema,
  runSqlFile,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

const migration000026Path = fileURLToPath(
  new URL('../../../db/migrations/committed/000026.sql', import.meta.url),
);
const migration000027Path = fileURLToPath(
  new URL('../../../db/migrations/committed/000027.sql', import.meta.url),
);

describe('integration: DeepAgent committed migration regressions', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_deepagent_migrations');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
    await db.sql`drop schema if exists app_public cascade`;
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('executes 000026 and returns the expected CoachMemory Liquid fact count', async () => {
    const stdout = await runSqlFile(db.dbName, migration000026Path, {
      tuplesOnly: true,
      unaligned: true,
    });

    expect(stdout).toBe('227');
  });

  it('executes 000027 against a pre-migration grocery snapshot table without base_price', async () => {
    await db.sql`create schema app_public`;
    await db.sql`
      create table app_public.grocery_price_snapshots (
        price numeric(10, 2) not null
      )
    `;
    await db.sql`
      insert into app_public.grocery_price_snapshots (price)
      values (5.99)
    `;

    await runSqlFile(db.dbName, migration000027Path);

    const columns = await db.sql<Array<{ column_name: string }>>`
      select column_name
      from information_schema.columns
      where table_schema = 'app_public'
        and table_name = 'grocery_price_snapshots'
        and column_name in ('base_price', 'sale_state')
      order by column_name
    `;
    const rows = await db.sql<Array<{ price: string; base_price: string | null; sale_state: string }>>`
      select
        price::text as price,
        base_price::text as base_price,
        sale_state
      from app_public.grocery_price_snapshots
    `;

    expect(columns).toEqual([{ column_name: 'base_price' }, { column_name: 'sale_state' }]);
    expect(rows).toEqual([{ price: '5.99', base_price: null, sale_state: 'unknown' }]);
  });

  it('backfills sale_state correctly when 000027 runs over rows that already have base_price', async () => {
    await db.sql`create schema app_public`;
    await db.sql`
      create table app_public.grocery_price_snapshots (
        price numeric(10, 2) not null,
        base_price numeric(10, 2)
      )
    `;
    await db.sql`
      insert into app_public.grocery_price_snapshots (price, base_price)
      values
        (3.99, 5.99),
        (4.49, 4.49),
        (2.19, null)
    `;

    await runSqlFile(db.dbName, migration000027Path);

    const rows = await db.sql<Array<{ price: string; base_price: string | null; sale_state: string }>>`
      select
        price::text as price,
        base_price::text as base_price,
        sale_state
      from app_public.grocery_price_snapshots
      order by price asc
    `;
    const constraints = await db.sql<Array<{ constraint_name: string }>>`
      select conname as constraint_name
      from pg_constraint
      where conname = 'grocery_price_snapshots_sale_state_check'
    `;

    expect(rows).toEqual([
      { price: '2.19', base_price: null, sale_state: 'unknown' },
      { price: '3.99', base_price: '5.99', sale_state: 'on_sale' },
      { price: '4.49', base_price: '4.49', sale_state: 'not_on_sale' },
    ]);
    expect(constraints).toEqual([{ constraint_name: 'grocery_price_snapshots_sale_state_check' }]);
  });
});
