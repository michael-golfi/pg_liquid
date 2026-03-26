// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  generateTransitGraphAssertions,
  joinProgram,
  resetLiquidSchema,
  shortestDepthBySqlBfs,
  timedMs,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('benchmark: shortest_path_stress_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_shortest');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('runs reachability on a branchy shortest-path-shaped graph', async () => {
    const layers = 120;
    const width = 8;
    const startNode = 'SpNode:1:1';
    const targetNode = `SpNode:${layers}:${width}`;

    const program = joinProgram([
      generateTransitGraphAssertions(layers, width),
      `Reach(node_id) :- Edge("${startNode}", "sp_next", node_id).`,
      'Reach(node_id) :- Reach(prev_id), Edge(prev_id, "sp_next", node_id).',
      'Reach(node_id)?',
    ]);

    const result = await timedMs(async () => {
      const rows = await db.sql<Array<{ target_hits: string }>>`
        select count(*)::bigint as target_hits
        from liquid.query(${program}) as t(node_id text)
        where node_id = ${targetNode}
      `;
      return Number.parseInt(rows[0]?.target_hits ?? '0', 10);
    });

    expect(result.value).toBe(1);
    expect(result.elapsedMs).toBeGreaterThan(0);

    const depth = await shortestDepthBySqlBfs(db.sql, startNode, targetNode, 'sp_next', layers + 2);
    expect(depth).not.toBeNull();
    expect(depth!).toBeGreaterThan(0);
    expect(depth!).toBeLessThanOrEqual(layers);
  });
});
