#!/usr/bin/env node

import { mkdir, writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import path from 'node:path';

import postgres from 'postgres';

const POINT_RUNS = 120;
const BASE_SCAN_RUNS = 100;
const RECURSIVE_BASE_RUNS = 20;
const RECURSIVE_STRESS_RUNS = 10;
const READ_AS_RUNS = 75;
const NORMALIZER_BENCH_ROWS = 400;

const WARMUP_PROGRAM = [
  'Edge("warm/node/1", "warm/link", "warm/node/2").',
  'WarmReach(x, y) :- Edge(x, "warm/link", y).',
  'WarmReach(x, z) :- WarmReach(x, y), WarmReach(y, z).',
  'WarmReach(x, z)?',
].join('\n');

const BULK_PROJECTION_QUERY = 'Edge(user_id, "auth/has_session", session_id)?';
const POINT_LOOKUP_TEMPLATE = 'Edge("session/{{SESSION_ID}}", "auth/from_device", device_id)?';

const RECURSIVE_CLOSURE_TEMPLATE = [
  'ManagesReach(x, y) :- Edge(x, "{{EDGE_PREDICATE}}", y).',
  'ManagesReach(x, z) :- ManagesReach(x, y), ManagesReach(y, z).',
  'ManagesReach(x, z)?',
].join('\n');

const SHORTEST_PATH_REACHABILITY_TEMPLATE = [
  'Reach(node_id) :- Edge("{{START_NODE}}", "sp_next", node_id).',
  'Reach(node_id) :- Reach(prev_id), Edge(prev_id, "sp_next", node_id).',
  'Reach(node_id)?',
].join('\n');

const COMPOUND_LOOKUP_PROGRAM = [
  'DefCompound("UserContact", "contact/user", "0", "liquid/string").',
  'DefCompound("UserContact", "contact/channel", "0", "liquid/string").',
  'DefCompound("UserContact", "contact/value", "0", "liquid/string").',
  'Edge("UserContact", "liquid/mutable", "false").',
  'UserContact@(contact/user="user/1", contact/channel="email", contact/value="ops@example.com").',
  'UserContact@(contact/user="user/2", contact/channel="sms", contact/value="+15551234567").',
  'UserContact@(cid=cid, contact/user=user_id, contact/channel=channel, contact/value=value)?',
].join('\n');

const METRIC_ORDER = [
  'base_scan_ms',
  'bulk_load_ms',
  'compound_lookup_ms',
  'ontology_validation_ms',
  'point_lookup_ms',
  'read_as_lookup_ms',
  'recursive_closure_ms',
  'recursive_closure_stress_ms',
  'row_normalizer_backfill_ms',
  'row_normalizer_trigger_delete_ms',
  'row_normalizer_trigger_insert_ms',
  'row_normalizer_trigger_update_ms',
  'shortest_path_stress_ms',
];

function asInt(value, fallback) {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function openSql(dbRef, max = 3) {
  if (dbRef.includes('://')) {
    return postgres(dbRef, {
      max,
      idle_timeout: 5,
      connect_timeout: 10,
      onnotice: () => undefined,
    });
  }
  return postgres({
    database: dbRef,
    max,
    idle_timeout: 5,
    connect_timeout: 10,
    onnotice: () => undefined,
  });
}

function joinPrograms(parts) {
  return parts
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
    .join('\n');
}

async function timedMs(fn) {
  const started = performance.now();
  const value = await fn();
  const finished = performance.now();
  return { elapsedMs: finished - started, value };
}

function expectEqual(actual, expected, context) {
  if (actual !== expected) {
    throw new Error(`${context}: expected ${expected}, got ${actual}`);
  }
}

function expectedChainClosureCount(chainLength) {
  return (chainLength * (chainLength + 1)) / 2;
}

function nodeLabel(layer, idx) {
  return `SpNode:${layer}:${idx}`;
}

function generateBulkLoadAssertions(benchN) {
  const lines = [];
  for (let n = 1; n <= benchN; n += 1) {
    lines.push(`Edge("user/${n}", "auth/has_session", "session/${n}").`);
    lines.push(`Edge("session/${n}", "auth/from_device", "device/${n}").`);
    lines.push(`Edge("user/${n}", "profile/lives_in", "city/${((n - 1) % 100) + 1}").`);
    lines.push(`Edge("user/${n}", "org/member_of", "team/${((n - 1) % 50) + 1}").`);
    lines.push(`Edge("user/${n}", "workflow/assigned_plan", "plan/${((n - 1) % 400) + 1}").`);
    lines.push(`Edge("user/${n}", "workflow/checkpoint", "checkin/${((n - 1) % 800) + 1}").`);
  }
  for (let team = 1; team <= 50; team += 1) {
    lines.push(`Edge("team/${team}", "org/owns_project", "project/${team}").`);
    lines.push(`Edge("team/${team}", "org/works_with_team", "team/${(team % 50) + 1}").`);
  }
  return lines.join('\n');
}

function generateManagementChainAssertions(chainLength, predicateLiteral = 'org/manages') {
  const lines = [];
  for (let n = 1; n <= chainLength; n += 1) {
    lines.push(`Edge("employee/${n}", "${predicateLiteral}", "employee/${n + 1}").`);
  }
  return lines.join('\n');
}

function generateTransitGraphAssertions(layers, width) {
  const lines = [];
  for (let layer = 1; layer <= layers; layer += 1) {
    for (let idx = 1; idx <= width; idx += 1) {
      const current = nodeLabel(layer, idx);
      lines.push(`Edge("${current}", "sp_kind", "kind/transit_stop").`);
      lines.push(`Edge("${current}", "sp_zone", "zone/${((layer - 1) % 6) + 1}").`);
      if (idx < width) {
        lines.push(`Edge("${current}", "sp_transfer", "${nodeLabel(layer, idx + 1)}").`);
      }
      if (layer < layers) {
        lines.push(`Edge("${current}", "sp_next", "${nodeLabel(layer + 1, idx)}").`);
        lines.push(`Edge("${current}", "sp_next", "${nodeLabel(layer + 1, (idx % width) + 1)}").`);
      }
      if (layer + 2 <= layers && idx % 2 === 0) {
        lines.push(`Edge("${current}", "sp_next", "${nodeLabel(layer + 2, idx)}").`);
      }
    }
  }
  return lines.join('\n');
}

function fillTemplate(template, replacements) {
  let output = template;
  for (const [key, value] of Object.entries(replacements)) {
    output = output.replaceAll(`{{${key}}}`, value);
  }
  return output;
}

function generateScopedReadProgram(count) {
  const lines = [
    'DefCompound("AssetAccess", "viewer", "0", "liquid/string").',
    'DefCompound("AssetAccess", "asset_id", "0", "liquid/string").',
    'DefCompound("AssetAccess", "location", "0", "liquid/string").',
    'Edge("AssetAccess", "liquid/mutable", "false").',
    'CompoundReadByRole@(compound_type="AssetAccess", role="viewer").',
    'Edge("session:ops-console", "liquid/acts_for", "viewer:ops").',
  ];

  for (let index = 1; index <= count; index += 1) {
    lines.push(
      `AssetAccess@(viewer="viewer:ops", asset_id="asset/${index}", location="warehouse/${((index - 1) % 8) + 1}").`,
    );
  }

  lines.push('AssetAccess@(cid=cid, viewer=viewer_id, asset_id=asset_id, location=location)?');
  return lines.join('\n');
}

function generateOntologyValidationProgram(classCount) {
  const lines = ['Edge("class/Thing", "onto/preferred_label", "Thing").'];

  for (let index = 1; index <= classCount; index += 1) {
    lines.push(`Edge("class/C${index}", "onto/preferred_label", "Class ${index}").`);
    lines.push(
      `Edge("class/C${index}", "onto/subclass_of", "${index === 1 ? 'class/Thing' : `class/C${index - 1}`}").`,
    );
    lines.push(`Edge("instance/${index}", "onto/instance_of", "class/C${index}").`);
  }

  lines.push('Edge("class/CycleA", "onto/subclass_of", "class/CycleB").');
  lines.push('Edge("class/CycleB", "onto/subclass_of", "class/CycleA").');
  lines.push('Edge("class/Placeholder", "onto/subclass_of", "class/C99999").');
  lines.push('Edge("instance/bad", "onto/instance_of", "class/Unknown").');
  lines.push('Edge("instance/cycle", "onto/instance_of", "class/CycleA").');
  lines.push('Edge(subject_id, "onto/subclass_of", object_id)?');

  return lines.join('\n');
}

async function writeArtifact(artifactPath, payload) {
  if (!artifactPath) {
    return;
  }

  await mkdir(path.dirname(artifactPath), { recursive: true });
  await writeFile(artifactPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

async function liquidCount(sql, program, columnsDef) {
  const rows = await sql`
    select count(*)::bigint as count
    from liquid.query(${program}) as t(${sql.unsafe(columnsDef)})
  `;
  return Number.parseInt(rows[0]?.count ?? '0', 10);
}

async function shortestDepthBySqlBfs(sql, startLiteral, targetLiteral, predicateLiteral, maxDepth) {
  const rows = await sql`
    with recursive ids as (
      select
        (select id from liquid.vertices where literal = ${startLiteral}) as start_id,
        (select id from liquid.vertices where literal = ${targetLiteral}) as target_id,
        (select id from liquid.vertices where literal = ${predicateLiteral}) as predicate_id
    ),
    bfs(node_id, depth) as (
      select ids.start_id, 0
      from ids
      union
      select e.object_id, bfs.depth + 1
      from bfs
      join ids on true
      join liquid.edges e on e.subject_id = bfs.node_id
      where e.is_deleted = false
        and e.predicate_id = ids.predicate_id
        and bfs.depth < ${maxDepth}
    ),
    best as (
      select node_id, min(depth) as depth
      from bfs
      group by node_id
    )
    select best.depth as shortest_depth
    from best
    join ids on best.node_id = ids.target_id
  `;
  return rows[0]?.shortest_depth ?? null;
}

async function main() {
  const benchDb = process.env.BENCH_DB ?? 'postgres';
  const benchN = asInt(process.env.BENCH_N, 3000);
  const chainN = asInt(process.env.CHAIN_N, 80);
  const chainNStress = asInt(process.env.CHAIN_N_STRESS, 120);
  const shortestPathWidth = asInt(process.env.SP_WIDTH, 8);
  const ontologyClassCount = asInt(process.env.ONTOLOGY_CLASS_N, 180);
  const artifactPath = process.env.BENCH_ARTIFACT_PATH ?? '';

  const sql = openSql(benchDb, 3);
  try {
    await sql`drop extension if exists pg_liquid cascade`;
    await sql`drop schema if exists liquid cascade`;
    await sql`create extension pg_liquid`;

    const warmupCount = await liquidCount(sql, WARMUP_PROGRAM, 'x text, y text');
    expectEqual(warmupCount, 1, 'warmup recursive query');

    const bulkProgram = joinPrograms([generateBulkLoadAssertions(benchN), BULK_PROJECTION_QUERY]);
    const bulk = await timedMs(async () => liquidCount(sql, bulkProgram, 'user_id text, session_id text'));
    expectEqual(bulk.value, benchN, 'bulk load query count');

    const pointLookupProgram = fillTemplate(POINT_LOOKUP_TEMPLATE, { SESSION_ID: '1' });
    const point = await timedMs(async () => {
      const rows = await sql`
        with runs as (
          select generate_series(1, ${POINT_RUNS}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint as sample_count
          from liquid.query(concat('% run ', runs.run_id::text, E'\n', ${pointLookupProgram}::text)) as t(device_id text)
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });
    expectEqual(point.value, POINT_RUNS, 'point lookup total count');

    const baseScan = await timedMs(async () => {
      const rows = await sql`
        with runs as (
          select generate_series(1, ${BASE_SCAN_RUNS}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint + (runs.run_id * 0)::bigint as sample_count
          from liquid.edges e
          join liquid.vertices p on p.id = e.predicate_id
          where e.is_deleted = false
            and p.literal = 'auth/has_session'
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });
    expectEqual(baseScan.value, benchN * BASE_SCAN_RUNS, 'base scan total count');

    const recursivePredicate = 'org/manages/base';
    const recursiveSeedProgram = joinPrograms([
      generateManagementChainAssertions(chainN, recursivePredicate),
      `Edge("employee/1", "${recursivePredicate}", seed_dst)?`,
    ]);
    const recursiveSeedCount = await liquidCount(sql, recursiveSeedProgram, 'seed_dst text');
    expectEqual(recursiveSeedCount, 1, 'recursive closure seed');

    const recursiveRules = fillTemplate(RECURSIVE_CLOSURE_TEMPLATE, {
      EDGE_PREDICATE: recursivePredicate,
    });
    const recursive = await timedMs(async () => {
      const rows = await sql`
        with runs as (
          select generate_series(1, ${RECURSIVE_BASE_RUNS}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint + (runs.run_id * 0)::bigint as sample_count
          from liquid.query(concat('% run ', runs.run_id::text, E'\n', ${recursiveRules}::text)) as t(x text, z text)
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });
    expectEqual(
      recursive.value,
      expectedChainClosureCount(chainN) * RECURSIVE_BASE_RUNS,
      'recursive closure count',
    );

    const recursiveStressPredicate = 'org/manages/stress';
    const recursiveStressSeedProgram = joinPrograms([
      generateManagementChainAssertions(chainNStress, recursiveStressPredicate),
      `Edge("employee/1", "${recursiveStressPredicate}", seed_dst)?`,
    ]);
    const recursiveStressSeedCount = await liquidCount(sql, recursiveStressSeedProgram, 'seed_dst text');
    expectEqual(recursiveStressSeedCount, 1, 'recursive closure stress seed');

    const recursiveStressRules = fillTemplate(RECURSIVE_CLOSURE_TEMPLATE, {
      EDGE_PREDICATE: recursiveStressPredicate,
    });
    const recursiveStress = await timedMs(async () => {
      const rows = await sql`
        with runs as (
          select generate_series(1, ${RECURSIVE_STRESS_RUNS}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint + (runs.run_id * 0)::bigint as sample_count
          from liquid.query(concat('% run ', runs.run_id::text, E'\n', ${recursiveStressRules}::text)) as t(x text, z text)
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });
    expectEqual(
      recursiveStress.value,
      expectedChainClosureCount(chainNStress) * RECURSIVE_STRESS_RUNS,
      'recursive closure stress count',
    );

    const shortestPathLayers = Math.max(12, chainNStress);
    const width = Math.max(3, shortestPathWidth);
    const startNode = nodeLabel(1, 1);
    const targetNode = nodeLabel(shortestPathLayers, width);
    const reachabilityRules = fillTemplate(SHORTEST_PATH_REACHABILITY_TEMPLATE, {
      START_NODE: startNode,
    });
    const shortestPathProgram = joinPrograms([
      generateTransitGraphAssertions(shortestPathLayers, width),
      reachabilityRules,
    ]);
    const shortestPath = await timedMs(async () => {
      const rows = await sql`
        select count(*)::bigint as target_hits
        from liquid.query(${shortestPathProgram}) as t(node_id text)
        where node_id = ${targetNode}
      `;
      return Number.parseInt(rows[0]?.target_hits ?? '0', 10);
    });
    expectEqual(shortestPath.value, 1, 'shortest path reachability');

    const shortestDepth = await shortestDepthBySqlBfs(
      sql,
      startNode,
      targetNode,
      'sp_next',
      shortestPathLayers + 2,
    );
    if (shortestDepth === null || shortestDepth <= 0 || shortestDepth > shortestPathLayers) {
      throw new Error(
        `shortest path depth invariant failed: depth=${shortestDepth}, layers=${shortestPathLayers}`,
      );
    }

    const compound = await timedMs(async () =>
      liquidCount(sql, COMPOUND_LOOKUP_PROGRAM, 'cid text, user_id text, channel text, value text'),
    );
    expectEqual(compound.value, 2, 'compound lookup count');

    const scopedReadSeedProgram = generateScopedReadProgram(Math.max(benchN, 1200));
    const scopedReadSeedCount = await liquidCount(
      sql,
      scopedReadSeedProgram,
      'cid text, viewer_id text, asset_id text, location text',
    );
    expectEqual(scopedReadSeedCount, Math.max(benchN, 1200), 'scoped read seed count');

    const readAsProgram =
      'AssetAccess@(cid=cid, viewer="viewer:ops", asset_id=asset_id, location=location)?';
    const readAsLookup = await timedMs(async () => {
      const rows = await sql`
        with runs as (
          select generate_series(1, ${READ_AS_RUNS}) as run_id
        )
        select sum(sample_count)::bigint as total
        from runs
        cross join lateral (
          select count(*)::bigint as sample_count
          from liquid.read_as('session:ops-console', ${readAsProgram}) as t(
            cid text,
            asset_id text,
            location text
          )
        ) as q
      `;
      return Number.parseInt(rows[0]?.total ?? '0', 10);
    });
    expectEqual(readAsLookup.value, Math.max(benchN, 1200) * READ_AS_RUNS, 'read_as lookup count');

    await sql`
      create table public.asset_registry (
        asset_id text primary key,
        workstream text not null,
        site_code text not null
      )
    `;
    const normalizerSeedRows = Array.from({ length: NORMALIZER_BENCH_ROWS }, (_, index) => ({
      asset_id: `asset-${index + 1}`,
      workstream: `workstream-${(index % 12) + 1}`,
      site_code: `site-${(index % 6) + 1}`,
    }));
    await sql`
      insert into public.asset_registry ${sql(normalizerSeedRows, 'asset_id', 'workstream', 'site_code')}
    `;

    await sql`
      select *
      from liquid.query(
        ${joinPrograms([
          'DefCompound("AssetRecord", "asset_id", "0", "liquid/string").',
          'DefCompound("AssetRecord", "site_code", "0", "liquid/string").',
          'DefCompound("AssetRecord", "workstream", "0", "liquid/string").',
          'Edge("AssetRecord", "liquid/mutable", "false").',
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?',
        ])}
      ) as t(asset_id text, site_code text, workstream text)
    `;

    const rowNormalizerBackfill = await timedMs(async () => {
      await sql`
        select liquid.create_row_normalizer(
          'public.asset_registry'::regclass,
          'asset_registry_norm',
          'AssetRecord',
          '{"asset_id":"asset_id","site_code":"site_code","workstream":"workstream"}'::jsonb,
          true
        )
      `;
      const rows = await sql`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });
    expectEqual(rowNormalizerBackfill.value, NORMALIZER_BENCH_ROWS, 'row normalizer backfill count');

    const rowNormalizerTriggerInsert = await timedMs(async () => {
      await sql`
        insert into public.asset_registry
        select
          concat('asset-insert-', series_id::text),
          concat('workstream-', ((series_id - 1) % 12 + 1)::text),
          concat('site-', ((series_id - 1) % 6 + 1)::text)
        from generate_series(1, 120) as series(series_id)
      `;
      const rows = await sql`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
        where asset_id like 'asset-insert-%'
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });
    expectEqual(rowNormalizerTriggerInsert.value, 120, 'row normalizer insert count');

    const rowNormalizerTriggerUpdate = await timedMs(async () => {
      await sql`
        update public.asset_registry
        set site_code = 'site-updated'
        where asset_id like 'asset-insert-%'
      `;
      const rows = await sql`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
        where asset_id like 'asset-insert-%'
          and site_code = 'site-updated'
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });
    expectEqual(rowNormalizerTriggerUpdate.value, 120, 'row normalizer update count');

    const rowNormalizerTriggerDelete = await timedMs(async () => {
      await sql`
        delete from public.asset_registry
        where asset_id like 'asset-insert-%'
      `;
      const rows = await sql`
        select count(*)::bigint as count
        from liquid.query(
          'AssetRecord@(asset_id=asset_id, site_code=site_code, workstream=workstream)?'
        ) as t(asset_id text, site_code text, workstream text)
        where asset_id like 'asset-insert-%'
      `;
      return Number.parseInt(rows[0]?.count ?? '0', 10);
    });
    expectEqual(rowNormalizerTriggerDelete.value, 0, 'row normalizer delete count');

    const ontologyValidationSeedProgram = generateOntologyValidationProgram(ontologyClassCount);
    const ontologySeedCount = await liquidCount(
      sql,
      ontologyValidationSeedProgram,
      'subject_id text, object_id text',
    );
    expectEqual(ontologySeedCount, ontologyClassCount + 3, 'ontology validation seed count');

    const ontologyValidation = await timedMs(async () => {
      const rows = await sql`
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
    if (ontologyValidation.value < 4) {
      throw new Error(`ontology validation benchmark expected >= 4 issues, got ${ontologyValidation.value}`);
    }

    const metrics = {
      bulk_load_ms: bulk.elapsedMs,
      point_lookup_ms: point.elapsedMs,
      base_scan_ms: baseScan.elapsedMs,
      recursive_closure_ms: recursive.elapsedMs,
      recursive_closure_stress_ms: recursiveStress.elapsedMs,
      shortest_path_stress_ms: shortestPath.elapsedMs,
      compound_lookup_ms: compound.elapsedMs,
      read_as_lookup_ms: readAsLookup.elapsedMs,
      row_normalizer_backfill_ms: rowNormalizerBackfill.elapsedMs,
      row_normalizer_trigger_insert_ms: rowNormalizerTriggerInsert.elapsedMs,
      row_normalizer_trigger_update_ms: rowNormalizerTriggerUpdate.elapsedMs,
      row_normalizer_trigger_delete_ms: rowNormalizerTriggerDelete.elapsedMs,
      ontology_validation_ms: ontologyValidation.elapsedMs,
    };

    const settingsRows = await sql`
      select config.key, config.value
      from (
        values
          ('server_version', current_setting('server_version')),
          ('server_version_num', current_setting('server_version_num')),
          ('work_mem', current_setting('work_mem')),
          ('jit', current_setting('jit'))
      ) as config(key, value)
      order by config.key
    `;

    await writeArtifact(artifactPath, {
      generated_at: new Date().toISOString(),
      bench_db: benchDb,
      workload: {
        bench_n: benchN,
        chain_n: chainN,
        chain_n_stress: chainNStress,
        sp_width: shortestPathWidth,
        ontology_class_n: ontologyClassCount,
        normalizer_rows: NORMALIZER_BENCH_ROWS,
      },
      settings: Object.fromEntries(settingsRows.map((row) => [row.key, row.value])),
      metrics: Object.fromEntries(
        METRIC_ORDER.map((metric) => [metric, Number(metrics[metric].toFixed(3))]),
      ),
    });

    for (const metric of METRIC_ORDER) {
      process.stdout.write(`${metric}|${metrics[metric].toFixed(3)}\n`);
    }
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
