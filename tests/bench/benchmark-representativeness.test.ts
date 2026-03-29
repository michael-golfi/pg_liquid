// @vitest-environment node
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import {
  generateBulkLoadAssertions,
  generateManagementChainAssertions,
  generateTransitGraphAssertions,
} from '../helpers/pg-liquid-test-helpers.js';

const BENCHMARK_DIR = dirname(fileURLToPath(import.meta.url));
const CANONICAL_RECURSIVE_STRESS_CHAIN_N = 240;

function countLines(program: string, fragment: string): number {
  return program.split('\n').filter((line) => line.includes(fragment)).length;
}

function readBenchmarkFile(relativePath: string): string {
  return readFileSync(resolve(BENCHMARK_DIR, relativePath), 'utf8');
}

describe('pg_liquid benchmark workload shape', () => {
  it('uses multi-domain assertions for bulk-load realism', () => {
    const program = generateBulkLoadAssertions(1000);
    expect(countLines(program, '"auth/has_session"')).toBe(1000);
    expect(countLines(program, '"org/member_of"')).toBe(1000);
    expect(countLines(program, '"workflow/assigned_plan"')).toBe(1000);
    expect(countLines(program, '"workflow/checkpoint"')).toBe(1000);
  });

  it('keeps recursive stress deeper than baseline closure case', () => {
    const regular = generateManagementChainAssertions(80);
    const stress = generateManagementChainAssertions(CANONICAL_RECURSIVE_STRESS_CHAIN_N);
    expect(countLines(regular, '"org/manages"')).toBe(80);
    expect(countLines(stress, '"org/manages"')).toBe(CANONICAL_RECURSIVE_STRESS_CHAIN_N);
  });

  it('keeps the larger recursive stress workload on the bench-check gate path', () => {
    const makefile = readBenchmarkFile('../../Makefile');
    const benchCheck = readBenchmarkFile('../../scripts/bench_check.sh');
    const benchmarkRunner = readBenchmarkFile('../../scripts/run_benchmarks.mjs');

    expect(makefile).toMatch(/bench-check: install\s+[\s\S]*bash \$\(srcdir\)\/scripts\/bench_check\.sh/);
    expect(benchCheck).toContain(
      `CHAIN_N_STRESS="\${CHAIN_N_STRESS:-${CANONICAL_RECURSIVE_STRESS_CHAIN_N}}"`,
    );
    expect(benchCheck).toMatch(/CHAIN_N_STRESS="\$CHAIN_N_STRESS"\s*\\\s*\n\s*node scripts\/run_benchmarks\.mjs/);
    expect(benchCheck).toContain(
      'assert_metric "recursive_closure_stress_ms" "$RECURSIVE_CLOSURE_STRESS_MAX_MS"',
    );

    expect(benchmarkRunner).toContain(
      `const chainNStress = asInt(process.env.CHAIN_N_STRESS, ${CANONICAL_RECURSIVE_STRESS_CHAIN_N});`,
    );
    expect(benchmarkRunner).toContain(
      'generateManagementChainAssertions(chainNStress, recursiveStressPredicate)',
    );
    expect(benchmarkRunner).toContain(
      "expectEqual(recursiveStressSeedCount, 1, 'recursive closure stress seed');",
    );
    expect(benchmarkRunner).toContain(
      "expectedChainClosureCount(chainNStress) * RECURSIVE_STRESS_RUNS",
    );
    expect(benchmarkRunner).toContain("'recursive closure stress count'");
    expect(benchmarkRunner).toContain('recursive_closure_stress_ms: recursiveStress.elapsedMs');
  });

  it('builds a branchy transit graph for shortest-path stress', () => {
    const layers = 120;
    const width = 8;
    const graph = generateTransitGraphAssertions(layers, width);
    const spNextEdges = countLines(graph, '"sp_next"');
    const transferEdges = countLines(graph, '"sp_transfer"');
    const nodeKinds = countLines(graph, '"sp_kind"');
    const nodes = layers * width;

    expect(nodeKinds).toBe(nodes);
    expect(transferEdges).toBe((width - 1) * layers);
    expect(spNextEdges).toBeGreaterThan((layers - 1) * width * 2);
    expect(graph.includes('Edge("SpNode:1:1", "sp_next", "SpNode:120:8").')).toBe(false);
  });
});
