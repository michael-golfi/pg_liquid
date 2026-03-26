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

describe('benchmark: ontology_validation_ms', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_ontology_validation');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('validates taxonomy and instance integrity on a realistic graph', async () => {
    const classCount = 120;
    const seedProgram = joinProgram([
      'Edge("class/Thing", "onto/preferred_label", "Thing").',
      ...Array.from({ length: classCount }, (_, index) => {
        const classId = index + 1;
        return joinProgram([
          `Edge("class/C${classId}", "onto/preferred_label", "Class ${classId}").`,
          `Edge("class/C${classId}", "onto/subclass_of", "${classId === 1 ? 'class/Thing' : `class/C${classId - 1}`}").`,
          `Edge("instance/${classId}", "onto/instance_of", "class/C${classId}").`,
        ]);
      }),
      'Edge("class/CycleA", "onto/subclass_of", "class/CycleB").',
      'Edge("class/CycleB", "onto/subclass_of", "class/CycleA").',
      'Edge("instance/missing", "onto/instance_of", "class/Unknown").',
      'Edge(subject_id, "onto/subclass_of", object_id)?',
    ]);

    const seeded = await liquidCount(db.sql, seedProgram, 'subject_id text, object_id text');
    expect(seeded).toBe(classCount + 2);

    const result = await timedMs(async () => {
      const rows = await db.sql<Array<{ total_issues: string }>>`
        with taxonomy as (
          select count(*)::bigint as issue_count
          from liquid.validate_taxonomy('onto/subclass_of')
        ),
        instances as (
          select count(*)::bigint as issue_count
          from liquid.validate_instances('onto/instance_of', 'onto/subclass_of')
        )
        select (taxonomy.issue_count + instances.issue_count)::bigint as total_issues
        from taxonomy, instances
      `;
      return Number.parseInt(rows[0]?.total_issues ?? '0', 10);
    });

    expect(result.value).toBeGreaterThanOrEqual(3);
    expect(result.elapsedMs).toBeGreaterThan(0);
  });
});
