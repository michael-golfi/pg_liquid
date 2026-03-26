// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  expectedChainClosureCount,
  generateManagementChainAssertions,
  joinProgram,
  liquidCount,
  resetLiquidSchema,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: recursive_closure_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_recursive');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('evaluates transitive closure over a management chain', async () => {
    const chainN = 80;
    const predicate = 'org/manages';
    const recursiveProgram = [
      `ManagesReach(x, y) :- Edge(x, "${predicate}", y).`,
      'ManagesReach(x, z) :- ManagesReach(x, y), ManagesReach(y, z).',
      'ManagesReach(x, z)?',
    ].join('\n');

    const seedProgram = joinProgram([
      generateManagementChainAssertions(chainN, predicate),
      `Edge("employee/1", "${predicate}", seed_dst)?`,
    ]);
    const seeded = await liquidCount(db.sql, seedProgram, 'seed_dst text');
    expect(seeded).toBe(1);

    const result = await timedMs(async () =>
      liquidCount(db.sql, recursiveProgram, 'x text, z text'),
    );

    expect(result.value).toBe(expectedChainClosureCount(chainN));
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
