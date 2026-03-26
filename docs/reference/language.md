# Liquid Language Surface

`pg_liquid` supports the public Liquid syntax used in the Liquid blog examples.

## Supported Surface

- `%` comments
- `.`-terminated assertions and rule definitions
- one terminal `?` query
- variables such as `x`
- `_` anonymous variables
- quoted string literals
- `Edge(subject, predicate, object)`
- compounds like `Type@(cid=..., role=...)`

## Query Shape

A Liquid program is:

- zero or more assertions
- zero or more query-local rules
- one terminal query

## Example

```txt
Edge("alice", "knows", "bob").
Reach(x, y) :- Edge(x, "knows", y).
Reach("alice", target)?
```
