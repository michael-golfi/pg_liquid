# Testing and Compatibility

Useful local checks:

```sh
npm test
make package-check
make installcheck
make bench-check
make bench-guard BENCH_GUARD_MODE=check
npm run docs:build
```

Current validated PostgreSQL versions:

- 14
- 15
- 16
- 17
- 18

Deep references:

- [6. Testing](../06_TESTING.md)
- [Benchmarks](./benchmarks.md)
