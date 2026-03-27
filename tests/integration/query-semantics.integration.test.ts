// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  resetLiquidSchema,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('integration: query semantics', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_query_semantics');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('allows query-local rules to emit fresh quoted constants', async () => {
    const program = joinProgram([
      'Edge("seed/source", "seed/p", "seed/target").',
      'FreshLiteral("__fresh_rule_literal__") :- Edge("seed/source", "seed/p", "seed/target").',
      'FreshLiteral(result)?',
    ]);

    const rows = await db.sql<Array<{ result: string }>>`
      select result
      from liquid.query(${program}) as t(result text)
    `;

    expect(rows).toEqual([{ result: '__fresh_rule_literal__' }]);
  });

  it('rejects SQL record types that declare too many columns', async () => {
    const program = joinProgram([
      'Edge("arity/source", "arity/p", "arity/object").',
      'Edge("arity/source", "arity/p", object_literal)?',
    ]);

    await expect(
      db.sql`
        select *
        from liquid.query(${program}) as t(object_literal text, extra_column text)
      `,
    ).rejects.toMatchObject({ code: '42804' });
  });

  it('rejects SQL record types that declare too few columns', async () => {
    const program = joinProgram([
      'Edge("arity/source", "arity/p", "arity/object").',
      'Edge(subject_literal, predicate_literal, object_literal)?',
    ]);

    await expect(
      db.sql`
        select *
        from liquid.query(${program}) as t(subject_literal text, predicate_literal text)
      `,
    ).rejects.toMatchObject({ code: '42804' });
  });

  it('rejects zero-output queries instead of materializing a dummy NULL column', async () => {
    const program = joinProgram([
      'Edge("arity/source", "arity/p", "arity/object").',
      'Edge("arity/source", "arity/p", _)?',
    ]);

    await expect(
      db.sql`
        select *
        from liquid.query(${program}) as t(dummy text)
      `,
    ).rejects.toMatchObject({ code: '22023' });
  });
});
