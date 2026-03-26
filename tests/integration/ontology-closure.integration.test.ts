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

  it('exposes declared compound roles through the public helper', async () => {
    const program = joinProgram([
      'DefCompound("BookCopy", "copy_id", "0", "liquid/string").',
      'DefCompound("BookCopy", "work_id", "0", "liquid/string").',
      'DefCompound("BookCopy", "branch_id", "0", "liquid/string").',
      'DefCompound("BookCopy", role_name, role_cardinality, role_type)?',
    ]);

    await db.sql`
      select *
      from liquid.query(${program}) as t(role_name text, role_cardinality text, role_type text)
    `;

    const rows = await db.sql<Array<{ role_name: string }>>`
      select unnest(liquid.compound_roles('BookCopy')) as role_name
    `;

    expect(rows.map((row) => row.role_name)).toEqual(['branch_id', 'copy_id', 'work_id']);
  });

  it('flags taxonomy cycles, self-parenting, and placeholder class references', async () => {
    const program = joinProgram([
      'Edge("class/Item", "onto/preferred_label", "Item").',
      'Edge("class/Book", "onto/preferred_label", "Book").',
      'Edge("class/Book", "onto/subclass_of", "class/Item").',
      'Edge("class/Broken", "onto/subclass_of", "class/Broken").',
      'Edge("class/CycleA", "onto/subclass_of", "class/CycleB").',
      'Edge("class/CycleB", "onto/subclass_of", "class/CycleA").',
      'Edge(seed, "onto/subclass_of", root)?',
    ]);

    await db.sql`
      select *
      from liquid.query(${program}) as t(seed text, root text)
    `;

    const issues = await db.sql<
      Array<{ issue_type: string; subject_literal: string; related_literal: string | null }>
    >`
      select issue_type, subject_literal, related_literal
      from liquid.validate_taxonomy('onto/subclass_of')
      order by issue_type, subject_literal, related_literal
    `;

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          issue_type: 'self_parent',
          subject_literal: 'class/Broken',
          related_literal: 'class/Broken',
        }),
        expect.objectContaining({
          issue_type: 'cycle',
          subject_literal: 'class/CycleA',
        }),
        expect.objectContaining({
          issue_type: 'missing_referenced_class',
          subject_literal: 'class/CycleA',
        }),
      ]),
    );
  });

  it('flags dangling and invalid class references in instance assertions', async () => {
    const program = joinProgram([
      'Edge("class/Thing", "onto/preferred_label", "Thing").',
      'Edge("class/Asset", "onto/preferred_label", "Asset").',
      'Edge("class/Asset", "onto/subclass_of", "class/Thing").',
      'Edge("class/CycleA", "onto/subclass_of", "class/CycleB").',
      'Edge("class/CycleB", "onto/subclass_of", "class/CycleA").',
      'Edge("asset/router-1", "onto/instance_of", "class/Asset").',
      'Edge("asset/router-2", "onto/instance_of", "class/Unknown").',
      'Edge("asset/router-3", "onto/instance_of", "class/CycleA").',
      'Edge(instance_id, "onto/instance_of", class_id)?',
    ]);

    await db.sql`
      select *
      from liquid.query(${program}) as t(instance_id text, class_id text)
    `;

    const issues = await db.sql<
      Array<{ issue_type: string; subject_literal: string; related_literal: string | null }>
    >`
      select issue_type, subject_literal, related_literal
      from liquid.validate_instances('onto/instance_of', 'onto/subclass_of')
      order by issue_type, subject_literal, related_literal
    `;

    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          issue_type: 'dangling_class_reference',
          subject_literal: 'asset/router-2',
          related_literal: 'class/Unknown',
        }),
        expect.objectContaining({
          issue_type: 'invalid_class_reference',
          subject_literal: 'asset/router-3',
          related_literal: 'class/CycleA',
        }),
      ]),
    );
  });
});
