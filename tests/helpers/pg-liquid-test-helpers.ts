import { execFile as execFileCallback } from 'node:child_process';
import { performance } from 'node:perf_hooks';
import { promisify } from 'node:util';

import postgres, { type Sql } from 'postgres';

const execFile = promisify(execFileCallback);

export interface PgLiquidTestDb {
  readonly dbName: string;
  readonly sql: Sql;
}

function normalizeDbName(prefix: string): string {
  return `${prefix}_${process.pid}_${Date.now()}_${Math.floor(Math.random() * 100000)}`
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .toLowerCase();
}

export async function createIsolatedPgLiquidDb(prefix = 'pg_liquid_vitest'): Promise<PgLiquidTestDb> {
  const dbName = normalizeDbName(prefix);
  await execFile('createdb', ['-T', 'template0', dbName]);

  const sql = postgres({
    database: dbName,
    max: 3,
    idle_timeout: 5,
    connect_timeout: 10,
    onnotice: () => undefined,
  });

  await resetLiquidSchema(sql);
  return { dbName, sql };
}

export async function destroyIsolatedPgLiquidDb(db: PgLiquidTestDb): Promise<void> {
  if (!db) {
    return;
  }

  await db.sql.end({ timeout: 5 });
  await execFile('dropdb', ['--if-exists', db.dbName]);
}

export async function resetLiquidSchema(sql: Sql): Promise<void> {
  await sql`drop extension if exists pg_liquid cascade`;
  await sql`drop schema if exists liquid cascade`;
  await sql`create extension pg_liquid`;
}

export function joinProgram(parts: readonly string[]): string {
  return parts
    .map((part) => part.trim())
    .filter((part) => part.length > 0)
    .join('\n');
}

export async function liquidCount(sql: Sql, program: string, columnsDef: string): Promise<number> {
  const rows = await sql<Array<{ count: string }>>`
    select count(*)::bigint as count
    from liquid.query(${program}) as t(${sql.unsafe(columnsDef)})
  `;
  return Number.parseInt(rows[0]?.count ?? '0', 10);
}

export async function timedMs<T>(fn: () => Promise<T>): Promise<{ elapsedMs: number; value: T }> {
  const started = performance.now();
  const value = await fn();
  const finished = performance.now();
  return { elapsedMs: finished - started, value };
}

export function expectedChainClosureCount(chainLength: number): number {
  return (chainLength * (chainLength + 1)) / 2;
}

export function generateBulkLoadAssertions(benchN: number): string {
  const lines: string[] = [];
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

export function generateManagementChainAssertions(
  chainLength: number,
  predicateLiteral = 'org/manages',
): string {
  const lines: string[] = [];
  for (let n = 1; n <= chainLength; n += 1) {
    lines.push(`Edge("employee/${n}", "${predicateLiteral}", "employee/${n + 1}").`);
  }
  return lines.join('\n');
}

function nodeLabel(layer: number, idx: number): string {
  return `SpNode:${layer}:${idx}`;
}

export function generateTransitGraphAssertions(layers: number, width: number): string {
  const lines: string[] = [];
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

export async function shortestDepthBySqlBfs(
  sql: Sql,
  startLiteral: string,
  targetLiteral: string,
  predicateLiteral: string,
  maxDepth: number,
): Promise<number | null> {
  const rows = await sql<Array<{ shortest_depth: number | null }>>`
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
