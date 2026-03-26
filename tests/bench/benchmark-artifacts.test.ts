// @vitest-environment node
import { execFile as execFileCallback } from 'node:child_process';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  createIsolatedPgLiquidDb,
  destroyIsolatedPgLiquidDb,
  type PgLiquidTestDb,
} from '../helpers/pg-liquid-test-helpers.js';

const execFile = promisify(execFileCallback);

describe('benchmark artifacts', () => {
  let db: PgLiquidTestDb;

  beforeAll(async () => {
    db = await createIsolatedPgLiquidDb('pg_liquid_bench_artifacts');
  });

  afterAll(async () => {
    await destroyIsolatedPgLiquidDb(db);
  });

  it('writes machine-readable benchmark metadata and metrics', async () => {
    const tmpDir = await mkdtemp(path.join(tmpdir(), 'pg-liquid-bench-artifact-'));
    const artifactPath = path.join(tmpDir, 'bench-results.json');
    const scriptPath = new URL('../../scripts/run_benchmarks.mjs', import.meta.url).pathname;

    await execFile('node', [scriptPath], {
      cwd: path.resolve(path.dirname(scriptPath), '..'),
      env: {
        ...process.env,
        BENCH_DB: db.dbName,
        BENCH_N: '120',
        CHAIN_N: '30',
        CHAIN_N_STRESS: '45',
        ONTOLOGY_CLASS_N: '40',
        SP_WIDTH: '4',
        BENCH_ARTIFACT_PATH: artifactPath,
      },
    });

    const payload = JSON.parse(await readFile(artifactPath, 'utf8')) as {
      workload: Record<string, number>;
      settings: Record<string, string>;
      metrics: Record<string, number>;
    };

    expect(payload.workload.bench_n).toBe(120);
    expect(payload.settings.server_version).toBeTruthy();
    expect(payload.metrics.bulk_load_ms).toBeGreaterThan(0);
    expect(payload.metrics.read_as_lookup_ms).toBeGreaterThan(0);
    expect(payload.metrics.row_normalizer_backfill_ms).toBeGreaterThan(0);
    expect(payload.metrics.ontology_validation_ms).toBeGreaterThan(0);
  });
});
