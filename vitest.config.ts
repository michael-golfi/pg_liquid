import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    name: 'pg-liquid',
    watch: false,
    globals: true,
    environment: 'node',
    fileParallelism: false,
    testTimeout: 120000,
    hookTimeout: 120000,
    include: ['tests/**/*.{test,spec}.ts'],
  },
});
