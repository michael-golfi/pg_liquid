\set ON_ERROR_STOP 1
\set BENCH_N 3000
\set CHAIN_N 80
\set CHAIN_N_STRESS 120
\set SP_WIDTH 2

\echo '--- pg_liquid benchmark setup ---'
drop extension if exists pg_liquid cascade;
drop schema if exists liquid cascade;
create extension pg_liquid;
\timing on

\echo '--- warmup: recursive LIquid path ---'
SELECT count(*)
FROM liquid.query($$
Edge("warm/node/1", "warm/link", "warm/node/2").
WarmReach(x, y) :- Edge(x, "warm/link", y).
WarmReach(x, z) :- WarmReach(x, y), WarmReach(y, z).
WarmReach(x, z)?
$$) AS t(x text, y text);

\echo '--- 1. Bulk LIquid assertion load (user/session/device graph) ---'
WITH RECURSIVE nums(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM nums WHERE n < :BENCH_N
),
seeded(assertion) AS (
  SELECT format('Edge("user/%s", "auth/has_session", "session/%s").', n, n) FROM nums
  UNION ALL
  SELECT format('Edge("session/%s", "auth/from_device", "device/%s").', n, n) FROM nums
  UNION ALL
  SELECT format('Edge("user/%s", "profile/lives_in", "city/%s").', n, ((n - 1) % 100) + 1) FROM nums
  UNION ALL
  SELECT format('Edge("user/%s", "org/member_of", "team/%s").', n, ((n - 1) % 50) + 1) FROM nums
  UNION ALL
  SELECT format('Edge("team/%s", "org/owns_project", "project/%s").', n, n)
  FROM generate_series(1, 50) AS g(n)
)
SELECT count(*)
FROM liquid.query(
  (SELECT string_agg(assertion, E'\n') FROM seeded)
  || E'\nEdge(user_id, "auth/has_session", session_id)?'
) AS t(user_id text, session_id text);

\echo '--- 2. Equality-heavy point lookup through liquid.query ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM liquid.query($$
Edge("session/1", "auth/from_device", device_id)?
$$) AS t(device_id text);

\echo '--- 3. Equality-heavy predicate scan in base storage ---'
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*)
FROM liquid.edges e
JOIN liquid.vertices p ON p.id = e.predicate_id
WHERE e.is_deleted = false
  AND p.literal = 'auth/has_session';

\echo '--- 4. Recursive chain closure ---'
WITH RECURSIVE nums(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM nums WHERE n < :CHAIN_N
)
SELECT count(*)
FROM liquid.query(
  (
    SELECT string_agg(
      format('Edge("employee/%s", "org/manages", "employee/%s").', n, n + 1),
      E'\n'
    )
    FROM nums
  ) || E'\n'
    || 'ManagesReach(x, y) :- Edge(x, "org/manages", y).' || E'\n'
    || 'ManagesReach(x, z) :- ManagesReach(x, y), ManagesReach(y, z).' || E'\n'
    || 'ManagesReach(x, z)?'
) AS t(x text, z text);

\echo '--- 5. Dense graph shortest-path stress ---'
WITH params AS (
  SELECT
    GREATEST(3, CEIL(:CHAIN_N_STRESS::numeric / :SP_WIDTH::numeric)::int) AS sp_layers,
    :SP_WIDTH::int AS sp_width
),
edge_defs(assertion) AS (
  SELECT format(
           'Edge("SpNode:%s:%s", "sp_link", "SpNode:%s:%s").',
           layer, src_idx, layer + 1, dst_idx
         )
  FROM params
  CROSS JOIN generate_series(1, (SELECT sp_layers FROM params) - 1) AS layer
  CROSS JOIN generate_series(1, (SELECT sp_width FROM params)) AS src_idx
  CROSS JOIN generate_series(1, (SELECT sp_width FROM params)) AS dst_idx

  UNION ALL

  SELECT format(
           'Edge("SpNode:%s:%s", "sp_kind", "kind/transit_stop").',
           layer, node_idx
         )
  FROM params
  CROSS JOIN generate_series(1, (SELECT sp_layers FROM params)) AS layer
  CROSS JOIN generate_series(1, (SELECT sp_width FROM params)) AS node_idx
),
seeded AS (
  SELECT count(*) AS start_fanout
  FROM liquid.query(
    (SELECT string_agg(assertion, E'\n') FROM edge_defs)
    || E'\nEdge("SpNode:1:1", "sp_link", seed)?'
  ) AS t(seed text)
),
graph_ids AS (
  SELECT
    (SELECT id FROM liquid.vertices WHERE literal = 'sp_link') AS sp_link_id,
    (SELECT id FROM liquid.vertices WHERE literal = 'SpNode:1:1') AS start_id,
    (
      SELECT id
      FROM liquid.vertices
      WHERE literal = format('SpNode:%s:%s', params.sp_layers, params.sp_width)
    ) AS target_id
  FROM params
),
bfs(node_id, depth) AS (
  SELECT graph_ids.start_id, 0
  FROM graph_ids
  UNION
  SELECT e.object_id, bfs.depth + 1
  FROM bfs
  JOIN graph_ids ON true
  JOIN liquid.edges e ON e.subject_id = bfs.node_id
  WHERE bfs.depth < (SELECT sp_layers FROM params)
    AND e.is_deleted = false
    AND e.predicate_id = graph_ids.sp_link_id
),
best AS (
  SELECT node_id, min(depth) AS depth
  FROM bfs
  GROUP BY node_id
)
SELECT
  seeded.start_fanout,
  best.depth AS shortest_depth,
  (params.sp_layers - 1) AS expected_depth
FROM seeded
CROSS JOIN params
CROSS JOIN graph_ids
JOIN best ON best.node_id = graph_ids.target_id;

\echo '--- 6. Compound identity and lookup ---'
SELECT count(*)
FROM liquid.query($$
DefCompound("UserContact", "contact/user", "0", "liquid/string").
DefCompound("UserContact", "contact/channel", "0", "liquid/string").
DefCompound("UserContact", "contact/value", "0", "liquid/string").
Edge("UserContact", "liquid/mutable", "false").
UserContact@(contact/user="user/1", contact/channel="email", contact/value="ops@example.com").
UserContact@(contact/user="user/2", contact/channel="sms", contact/value="+15551234567").
UserContact@(cid=cid, contact/user=user_id, contact/channel=channel, contact/value=value)?
$$) AS t(cid text, user_id text, channel text, value text);

\echo '--- 7. Installed index inventory ---'
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'liquid'
ORDER BY indexname;

\timing off
