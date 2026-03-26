\set ON_ERROR_STOP 1
\if :{?BENCH_N}
\else
\set BENCH_N 3000
\endif
\if :{?CHAIN_N}
\else
\set CHAIN_N 80
\endif
\if :{?CHAIN_N_STRESS}
\else
\set CHAIN_N_STRESS 120
\endif

drop extension if exists pg_liquid cascade;
drop schema if exists liquid cascade;
create extension pg_liquid;

set pg_liquid.bench_n = :'BENCH_N';
set pg_liquid.chain_n = :'CHAIN_N';
set pg_liquid.chain_n_stress = :'CHAIN_N_STRESS';

create temporary table bench_metrics (
  metric text primary key,
  elapsed_ms numeric not null
);

do $$
declare
  started_at timestamptz;
  finished_at timestamptz;
  result_count bigint;
  expected_count bigint;
  shortest_depth int;
  sp_width int := 2;
  sp_layers int;
  sp_target text;
  point_runs int := 50;
  base_scan_runs int := 100;
  sample_count bigint;
  loop_idx int;
  sp_link_id bigint;
  sp_start_id bigint;
  sp_target_id bigint;
begin
  sp_layers := greatest(3, ceil(current_setting('pg_liquid.chain_n_stress')::numeric / sp_width)::int);
  sp_target := format('SpNode:%s:%s', sp_layers, sp_width);

  -- Warm planner/JIT paths used by recursive Liquid evaluation before timing.
  perform count(*)
  from liquid.query($liquid$
Edge("warm/node/1", "warm/link", "warm/node/2").
WarmReach(x, y) :- Edge(x, "warm/link", y).
WarmReach(x, z) :- WarmReach(x, y), WarmReach(y, z).
WarmReach(x, z)?
$liquid$) as warm(x text, y text);

  started_at := clock_timestamp();

  select count(*) into result_count
  from liquid.query(
    (
      select string_agg(assertion, E'\n')
      from (
        select format('Edge("user/%s", "auth/has_session", "session/%s").', n, n) as assertion
        from generate_series(1, current_setting('pg_liquid.bench_n')::int) as g(n)

        union all

        select format('Edge("session/%s", "auth/from_device", "device/%s").', n, n)
        from generate_series(1, current_setting('pg_liquid.bench_n')::int) as g(n)

        union all

        select format('Edge("user/%s", "profile/lives_in", "city/%s").', n, ((n - 1) % 100) + 1)
        from generate_series(1, current_setting('pg_liquid.bench_n')::int) as g(n)

        union all

        select format('Edge("user/%s", "org/member_of", "team/%s").', n, ((n - 1) % 50) + 1)
        from generate_series(1, current_setting('pg_liquid.bench_n')::int) as g(n)

        union all

        select format('Edge("team/%s", "org/owns_project", "project/%s").', t, t)
        from generate_series(1, 50) as g(t)
      ) seeded(assertion)
    ) || E'\nEdge(user_id, "auth/has_session", session_id)?'
  ) as t(user_id text, session_id text);
  if result_count != current_setting('pg_liquid.bench_n')::int then
    raise exception 'bulk_load benchmark returned %, expected %',
      result_count, current_setting('pg_liquid.bench_n')::int;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('bulk_load_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  result_count := 0;
  for loop_idx in 1..point_runs loop
    select count(*) into sample_count
    from liquid.query($liquid$
Edge("session/1", "auth/from_device", device_id)?
$liquid$) as t(device_id text);
    result_count := result_count + coalesce(sample_count, 0);
  end loop;
  if result_count != point_runs then
    raise exception 'point lookup benchmark returned %, expected %',
      result_count, point_runs;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('point_lookup_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  result_count := 0;
  for loop_idx in 1..base_scan_runs loop
    select count(*) into sample_count
    from liquid.edges e
    join liquid.vertices p on p.id = e.predicate_id
    where e.is_deleted = false
      and p.literal = 'auth/has_session';
    result_count := result_count + coalesce(sample_count, 0);
  end loop;
  if result_count != current_setting('pg_liquid.bench_n')::int * base_scan_runs then
    raise exception 'base scan benchmark returned %, expected %',
      result_count, current_setting('pg_liquid.bench_n')::int * base_scan_runs;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('base_scan_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  select count(*) into result_count
  from liquid.query(
    (
      select string_agg(
               format('Edge("employee/%s", "org/manages", "employee/%s").', n, n + 1),
               E'\n'
             )
      from generate_series(1, current_setting('pg_liquid.chain_n')::int) as g(n)
    ) || E'\n'
      || 'ManagesReach(x, y) :- Edge(x, "org/manages", y).' || E'\n'
      || 'ManagesReach(x, z) :- ManagesReach(x, y), ManagesReach(y, z).' || E'\n'
      || 'ManagesReach(x, z)?'
  ) as t(x text, z text);
  expected_count := current_setting('pg_liquid.chain_n')::bigint
                  * (current_setting('pg_liquid.chain_n')::bigint + 1) / 2;
  if result_count != expected_count then
    raise exception 'recursive closure benchmark returned %, expected %',
      result_count, expected_count;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('recursive_closure_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  select count(*) into result_count
  from liquid.query(
    (
      select string_agg(
               format('Edge("employee/%s", "org/manages", "employee/%s").', n, n + 1),
               E'\n'
             )
      from generate_series(1, current_setting('pg_liquid.chain_n_stress')::int) as g(n)
    ) || E'\n'
      || 'ManagesReach(x, y) :- Edge(x, "org/manages", y).' || E'\n'
      || 'ManagesReach(x, z) :- ManagesReach(x, y), ManagesReach(y, z).' || E'\n'
      || 'ManagesReach(x, z)?'
  ) as t(x text, z text);
  expected_count := current_setting('pg_liquid.chain_n_stress')::bigint
                  * (current_setting('pg_liquid.chain_n_stress')::bigint + 1) / 2;
  if result_count != expected_count then
    raise exception 'recursive closure stress benchmark returned %, expected %',
      result_count, expected_count;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('recursive_closure_stress_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  select count(*) into result_count
  from liquid.query(
    (
      select string_agg(assertion, E'\n')
      from (
        select format(
                 'Edge("SpNode:%s:%s", "sp_link", "SpNode:%s:%s").',
                 layer, src_idx, layer + 1, dst_idx
               ) as assertion
        from generate_series(1, sp_layers - 1) as layer
        cross join generate_series(1, sp_width) as src_idx
        cross join generate_series(1, sp_width) as dst_idx

        union all

        select format(
                 'Edge("SpNode:%s:%s", "sp_kind", "kind/transit_stop").',
                 layer, node_idx
               )
        from generate_series(1, sp_layers) as layer
        cross join generate_series(1, sp_width) as node_idx
      ) full_graph(assertion)
    ) || E'\n'
      || 'Edge("SpNode:1:1", "sp_link", seed)?'
  ) as t(target text)
  where target is not null;
  if result_count != sp_width then
    raise exception 'shortest path stress graph load returned %, expected %',
      result_count, sp_width;
  end if;

  select id into sp_link_id
  from liquid.vertices
  where literal = 'sp_link';

  select id into sp_start_id
  from liquid.vertices
  where literal = 'SpNode:1:1';

  select id into sp_target_id
  from liquid.vertices
  where literal = sp_target;

  with recursive bfs(node_id, depth) as (
    select sp_start_id, 0
    union
    select e.object_id, bfs.depth + 1
    from bfs
    join liquid.edges e
      on e.subject_id = bfs.node_id
    where e.is_deleted = false
      and e.predicate_id = sp_link_id
      and bfs.depth < sp_layers
  ),
  best as (
    select node_id, min(depth) as depth
    from bfs
    group by node_id
  )
  select depth into shortest_depth
  from best
  where node_id = sp_target_id;
  if shortest_depth is null or shortest_depth != sp_layers - 1 then
    raise exception 'shortest path stress benchmark returned depth %, expected %',
      shortest_depth, sp_layers - 1;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('shortest_path_stress_ms', extract(epoch from finished_at - started_at) * 1000.0);

  started_at := clock_timestamp();

  select count(*) into result_count
from liquid.query($liquid$
DefCompound("UserContact", "contact/user", "0", "liquid/string").
DefCompound("UserContact", "contact/channel", "0", "liquid/string").
DefCompound("UserContact", "contact/value", "0", "liquid/string").
Edge("UserContact", "liquid/mutable", "false").
UserContact@(contact/user="user/1", contact/channel="email", contact/value="ops@example.com").
UserContact@(contact/user="user/2", contact/channel="sms", contact/value="+15551234567").
UserContact@(cid=cid, contact/user=user_id, contact/channel=channel, contact/value=value)?
$liquid$) as t(cid text, user_id text, channel text, value text);
  if result_count != 2 then
    raise exception 'compound lookup benchmark returned %, expected 2', result_count;
  end if;

  finished_at := clock_timestamp();
  insert into bench_metrics(metric, elapsed_ms)
  values ('compound_lookup_ms', extract(epoch from finished_at - started_at) * 1000.0);
end
$$;

copy (
  select metric || '|' || to_char(elapsed_ms, 'FM999999990.000')
  from bench_metrics
  order by metric
) to stdout;
