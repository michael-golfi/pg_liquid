// @vitest-environment node
import { describe, expect, it } from 'vitest';

import {
  generateBulkLoadAssertions,
  generateManagementChainAssertions,
  generateTransitGraphAssertions,
} from '../helpers/pg-liquid-test-helpers.js';

function countLines(program: string, fragment: string): number {
  return program.split('\n').filter((line) => line.includes(fragment)).length;
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
    const stress = generateManagementChainAssertions(240);
    expect(countLines(regular, '"org/manages"')).toBe(80);
    expect(countLines(stress, '"org/manages"')).toBe(240);
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
