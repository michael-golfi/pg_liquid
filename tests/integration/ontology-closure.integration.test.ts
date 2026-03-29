// @vitest-environment node
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  joinProgram,
  resetLiquidSchema,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

describe('integration: ontology closure', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_itest_ontology');
  });

  beforeEach(async () => {
    await resetLiquidSchema(db.sql);
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('derives transitive class ancestry', async () => {
    const program = joinProgram([
      'Edge("class/ApiReference", "onto/subclass_of", "class/Document").',
      'Edge("class/Document", "onto/subclass_of", "class/Artifact").',
      'Edge("class/Artifact", "onto/subclass_of", "class/Thing").',
      'ClassAncestor(child_class, parent_class) :- Edge(child_class, "onto/subclass_of", parent_class).',
      'ClassAncestor(child_class, ancestor_class) :-',
      '  Edge(child_class, "onto/subclass_of", parent_class),',
      '  ClassAncestor(parent_class, ancestor_class).',
      'ClassAncestor("class/ApiReference", ancestor_class)?',
    ]);

    const rows = await db.sql<Array<{ ancestor_class: string }>>`
      select ancestor_class
      from liquid.query(${program}) as t(ancestor_class text)
      order by ancestor_class
    `;
    const ancestors = rows.map((row) => row.ancestor_class);

    expect(ancestors).toContain('class/Document');
    expect(ancestors).toContain('class/Artifact');
    expect(ancestors).toContain('class/Thing');
  });

  it('derives ancestry across branching recursive frontiers beyond reserve capacity', async () => {
    const directAncestors = Array.from({ length: 10 }, (_, index) => `class/Branch${index + 1}`);
    const branchEdges = directAncestors.flatMap((ancestorClass) => [
      `Edge("class/ApiReference", "onto/subclass_of", "${ancestorClass}").`,
      `Edge("${ancestorClass}", "onto/subclass_of", "class/Thing").`,
    ]);
    const program = joinProgram([
      ...branchEdges,
      'ClassAncestor(child_class, parent_class) :- Edge(child_class, "onto/subclass_of", parent_class).',
      'ClassAncestor(child_class, ancestor_class) :-',
      '  Edge(child_class, "onto/subclass_of", parent_class),',
      '  ClassAncestor(parent_class, ancestor_class).',
      'ClassAncestor("class/ApiReference", ancestor_class)?',
    ]);

    const rows = await db.sql<Array<{ ancestor_class: string }>>`
      select ancestor_class
      from liquid.query(${program}) as t(ancestor_class text)
      order by ancestor_class
    `;

    expect(rows.map((row) => row.ancestor_class)).toEqual([
      ...directAncestors,
      'class/Thing',
    ]);
  });
});
