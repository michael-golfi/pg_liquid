set client_min_messages = warning;
drop extension if exists pg_liquid cascade;
reset client_min_messages;
create extension pg_liquid;

select count(*) as liquid_table_count
from pg_tables
where schemaname = 'liquid'
  and tablename in ('vertices', 'edges');

select cid1, cid2
from liquid.query($$
DefCompound("Email", "user", "0", "liquid/string").
DefCompound("Email", "domain", "0", "liquid/string").
Edge("Email", "liquid/mutable", "false").
Email@(user="root", domain="example.com").
SameEmail(a, b) :-
  Email@(cid=a, user="root", domain="example.com"),
  Email@(cid=b, domain="example.com", user="root").
SameEmail(cid1, cid2)?
$$) as t(cid1 text, cid2 text);

select predicate_literal,
       object_literal
from liquid.query($$
% comment before facts
Edge("A", "p", "B").
Edge("A", "q", "C").
% anonymous subject query
Edge("A", predicate_literal, object_literal)?
$$) as t(predicate_literal text, object_literal text)
order by 1, 2;

select to_json(subject_literal)::text as subject_escaped,
       to_json(object_literal)::text as object_escaped
from liquid.query($$
Edge("line\nbreak", "path", "C:\\temp\\\"file").
Edge(subject_literal, "path", object_literal)?
$$) as t(subject_literal text, object_literal text);

select count(*) as compiled_direct_edge_batch_count
from liquid.query($$
Edge("batch/a", "batch/p", "batch/b").
Edge("batch/c", "batch/p", "batch/d").
Edge("batch/e", "batch/p", "batch/f").
Edge(subject_literal, "batch/p", object_literal)?
$$) as t(subject_literal text, object_literal text);

select count(*) as mixed_ordered_assertion_edge_count
from liquid.query($$
Edge("mix/before", "mix/tag", "before").
DefPred("mix/link", "1", "liquid/node", "0", "liquid/node").
DefCompound("MixType", "label", "0", "liquid/string").
MixType@(label="node").
Edge("mix/after", "mix/tag", "after").
Edge(subject_literal, "mix/tag", object_literal)?
$$) as t(subject_literal text, object_literal text);

select cid_literal, label
from liquid.query($$
Edge("mix/before", "mix/tag", "before").
DefPred("mix/link", "1", "liquid/node", "0", "liquid/node").
DefCompound("MixType", "label", "0", "liquid/string").
MixType@(label="node").
Edge("mix/after", "mix/tag", "after").
MixType@(cid=cid_literal, label=label)?
$$) as t(cid_literal text, label text);

select to_json(subject_literal)::text as subject_escaped,
       to_json(object_literal)::text as object_escaped
from liquid.query($$
Edge("batch\nline", "batch/path", "C:\\batch\\one").
Edge("batch\tline", "batch/path", "D:\\batch\\two").
Edge(subject_literal, "batch/path", object_literal)?
$$) as t(subject_literal text, object_literal text)
order by 1, 2;

select count(*) as fallback_variable_clause_count
from liquid.query($$
DefCompound("FallbackType", "label", "0", "liquid/string").
FallbackType@(cid=x, label="node"), Edge(x, "fallback/second", "fallback/leaf").
Edge(subject_literal, predicate_literal, object_literal)?
$$) as t(subject_literal text, predicate_literal text, object_literal text)
where predicate_literal = 'fallback/second'
  and object_literal = 'fallback/leaf';

select count(*) as post_assert_predicate_resolution_count
from liquid.query($$
DefPred("late/pred", "1", "liquid/node", "0", "liquid/node").
Edge("late/source", "late/pred", "late/target").
LateReach(x, y) :- Edge(x, "late/pred", y).
LateReach("late/source", target)?
$$) as t(target text);

select count(*) as post_assert_compound_resolution_count
from liquid.query($$
DefCompound("LateType", "label", "0", "liquid/string").
LateType@(label="value").
LateType@(cid=cid_literal, label=label)?
$$) as t(cid_literal text, label text);

select count(*) as generated_rule_match_count
from liquid.query(
  (
    select
      (select string_agg(
                format('Edge("S", "p%s", "O%s").', n, n),
                E'\n'
              )
         from generate_series(1, 20) as g(n))
      || E'\n'
      || (select string_agg(
                format('R%s(x, y) :- Edge(x, "p%s", y).', n, n),
                E'\n'
              )
            from generate_series(1, 20) as g(n))
      || E'\nR20(x, y)?'
  )
) as t(x text, y text);

select count(*) as large_predicate_union_count
from liquid.query(
  (
    select string_agg(
             format('Edge("M", "m%s", "V%s").', n, n),
             E'\n'
           )
    from generate_series(1, 300) as g(n)
  ) || E'\n' ||
  (
    select string_agg(
             format('Any(x, y) :- Edge(x, "m%s", y).', n),
             E'\n'
           )
    from generate_series(1, 300) as g(n)
  ) || E'\nAny(x, y)?'
) as t(x text, y text);

select count(*) as unknown_head_result_count
from liquid.query($$
Edge("uh/a", "uh/p", "uh/b").
UnknownHead("__missing_head_literal__") :- Edge("uh/a", "uh/p", "uh/b").
UnknownHead(result)?
$$) as t(result text);

select reachable
from liquid.query($$
Edge("ra", "next", "rb").
Edge("rb", "next", "rc").
Edge("rc", "next", "rd").
Reach(x, y) :- Edge(x, "next", y).
Reach(x, z) :- Reach(x, y), Reach(y, z).
Reach("ra", reachable)?
$$) as t(reachable text)
order by 1;

select source
from liquid.query($$
Edge("ra", "next", "rb").
Edge("rb", "next", "rc").
Edge("rc", "next", "rd").
Reach(x, y) :- Edge(x, "next", y).
Reach(x, z) :- Reach(x, y), Reach(y, z).
Reach(source, "rd")?
$$) as t(source text)
order by 1;

select source, target
from liquid.query($$
Edge("ma", "next2", "mb").
Edge("mb", "next2", "mc").
Edge("mc", "next2", "md").
Path(x, y) :- Edge(x, "next2", y).
Path(x, z) :- Path(x, y), Path(y, z).
Path(source, target)?
$$) as t(source text, target text)
order by 1, 2;

select source, target
from liquid.query($$
Edge("ca", "loop", "cb").
Edge("cb", "loop", "ca").
Cycle(x, y) :- Edge(x, "loop", y).
Cycle(x, z) :- Cycle(x, y), Cycle(y, z).
Cycle(source, target)?
$$) as t(source text, target text)
order by 1, 2;

select target
from liquid.query($$
Edge("ua", "next3", "ub").
Edge("ub", "next3", "uc").
Edge("uc", "next3", "ud").
Even(x, y) :- Edge(x, "next3", y).
Odd(x, z) :- Even(x, y), Edge(y, "next3", z).
Even(x, z) :- Odd(x, y), Edge(y, "next3", z).
Even("ua", target)?
$$) as t(target text)
order by 1;

select target
from liquid.query($$
Edge("ua", "next3", "ub").
Edge("ub", "next3", "uc").
Edge("uc", "next3", "ud").
Even(x, y) :- Edge(x, "next3", y).
Odd(x, z) :- Even(x, y), Edge(y, "next3", z).
Even(x, z) :- Odd(x, y), Edge(y, "next3", z).
Odd("ua", target)?
$$) as t(target text)
order by 1;

set pg_liquid.edge_cache_budget_kb = 64;

select count(*) as low_budget_edge_cache_count
from liquid.query(
  (
    select string_agg(
             format('Edge("W:%s", "wide", "V:%s").', n, n),
             E'\n'
           )
    from generate_series(1, 4000) as g(n)
  ) || E'\nEdge("W:1", "wide", object_literal)?'
) as t(object_literal text);

reset pg_liquid.edge_cache_budget_kb;

do $$
declare
  s_id bigint;
  p_id bigint;
  o_id bigint;
begin
  select id into s_id from liquid.vertices where literal = 'A';
  select id into p_id from liquid.vertices where literal = 'p';
  select id into o_id from liquid.vertices where literal = 'B';

  update liquid.edges
     set is_deleted = true
   where subject_id = s_id
     and predicate_id = p_id
     and object_id = o_id;

  if exists (
    select 1
    from liquid.query($liquid$
Edge("A", "p", object_literal)?
$liquid$) as t(object_literal text)
    where object_literal = 'B'
  ) then
    raise exception 'tombstoned edge should not be visible';
  end if;
end $$;

select count(*) as mutual_recursive_reach_count
from liquid.query($$
Edge("mr:a", "mr:next", "mr:b").
Edge("mr:b", "mr:next", "mr:c").
Edge("mr:c", "mr:next", "mr:d").
StepA(x, y) :- Edge(x, "mr:next", y).
StepB(x, y) :- StepA(x, y).
StepA(x, z) :- StepB(x, y), Edge(y, "mr:next", z).
StepA("mr:a", target)?
$$) as t(target text);

select count(*) as predicate_edge_reverse_reach_count
from liquid.query($$
Edge("re:a", "re:next", "re:b").
Edge("re:b", "re:next", "re:c").
Edge("re:c", "re:next", "re:d").
ReachRev(x, y) :- Edge(x, "re:next", y).
ReachRev(x, z) :- Edge(x, "re:next", y), ReachRev(y, z).
ReachRev("re:a", target)?
$$) as t(target text);

select count(*) as cycle_closure_dedup_count
from liquid.query($$
Edge("cy:a", "cy:next", "cy:b").
Edge("cy:b", "cy:next", "cy:c").
Edge("cy:c", "cy:next", "cy:a").
CycleReach(x, y) :- Edge(x, "cy:next", y).
CycleReach(x, z) :- CycleReach(x, y), CycleReach(y, z).
CycleReach(source, target)?
$$) as t(source text, target text);

select s, p, o
from liquid.query($$
Edge("A", "p", "B").
Edge(s, p, o)?
$$) as t(s text, p text, o text)
where s = 'A' and p = 'p' and o = 'B';

do $$
begin
  begin
    perform *
    from liquid.query($liquid$
Edge("A", "p", "B")
Edge(x, "p", y)?
$liquid$) as t(x text, y text);
    raise exception 'expected parse failure for missing statement terminator';
  exception
    when syntax_error then
      raise notice 'missing terminator rejected';
  end;

  begin
    perform *
    from liquid.query($liquid$
DefCompound("Email", "user", "0", "liquid/string").
Email@(user "root")?
$liquid$) as t(x text);
    raise exception 'expected parse failure for malformed atom';
  exception
    when syntax_error then
      raise notice 'malformed atom rejected';
  end;
end $$;

do $$
begin
  begin
    perform *
    from liquid.query($liquid$
UnknownSchema@(user="root").
UnknownSchema@(cid=cid, user=account_user)?
$liquid$) as t(cid text, account_user text);
    raise exception 'expected unknown compound type validation';
  exception
    when invalid_parameter_value then
      null;
  end;

  begin
    perform *
    from liquid.query($liquid$
DefCompound("ValidatedEmail", "user", "0", "liquid/string").
DefCompound("ValidatedEmail", "domain", "0", "liquid/string").
ValidatedEmail@(user="root").
ValidatedEmail@(cid=cid, user=account_user, domain=domain)?
$liquid$) as t(cid text, account_user text, domain text);
    raise exception 'expected missing compound role validation';
  exception
    when invalid_parameter_value then
      null;
  end;

  begin
    perform *
    from liquid.query($liquid$
DefCompound("ValidatedEmailExtra", "user", "0", "liquid/string").
DefCompound("ValidatedEmailExtra", "domain", "0", "liquid/string").
ValidatedEmailExtra@(user="root", domain="example.com", extra="x").
ValidatedEmailExtra@(cid=cid, user=account_user, domain=domain)?
$liquid$) as t(cid text, account_user text, domain text);
    raise exception 'expected extra compound role validation';
  exception
    when invalid_parameter_value then
      null;
  end;
end $$;

select count(*) as wildcard_compound_exact_match_count
from liquid.query($$
DefCompound("Email_", "user", "0", "liquid/string").
DefCompound("Email_", "domain", "0", "liquid/string").
Edge("Email_", "liquid/mutable", "false").
DefCompound("Email_x", "user", "0", "liquid/string").
DefCompound("Email_x", "domain", "0", "liquid/string").
Edge("Email_x", "liquid/mutable", "false").
Email_@(user="root", domain="underscore.example").
Email_x@(user="root", domain="x.example").
Email_@(cid=cid, user=account_user, domain=domain)?
$$) as t(cid text, account_user text, domain text);

select count(*) as fresh_rule_constant_count
from liquid.query($$
Edge("seed/source", "seed/p", "seed/target").
FreshLiteral("__fresh_rule_literal__") :- Edge("seed/source", "seed/p", "seed/target").
FreshLiteral(result)?
$$) as t(result text);

do $$
begin
  begin
    perform *
    from liquid.query($liquid$
Edge("arity/source", "arity/p", "arity/object").
Edge("arity/source", "arity/p", object_literal)?
$liquid$) as t(object_literal text, extra_column text);
    raise exception 'expected wide output arity rejection';
  exception
    when datatype_mismatch then
      raise notice 'wide output arity rejected';
  end;

  begin
    perform *
    from liquid.query($liquid$
Edge("arity/source", "arity/p", "arity/object").
Edge(subject_literal, predicate_literal, object_literal)?
$liquid$) as t(subject_literal text, predicate_literal text);
    raise exception 'expected narrow output arity rejection';
  exception
    when datatype_mismatch then
      raise notice 'narrow output arity rejected';
  end;

  begin
    perform *
    from liquid.query($liquid$
Edge("arity/source", "arity/p", "arity/object").
Edge("arity/source", "arity/p", _)?
$liquid$) as t(dummy text);
    raise exception 'expected zero-output query rejection';
  exception
    when invalid_parameter_value then
      raise notice 'zero-output query rejected';
  end;
end $$;

do $$
declare
  program text;
begin
  begin
    perform *
    from liquid.query($liquid$
DefCompound("Empty", "user", "0", "liquid/string").
Empty@(cid=cid)?
$liquid$) as t(cid text);
    raise exception 'expected zero-role compound rejection';
  exception
    when program_limit_exceeded then
      raise notice 'zero-role compound rejected';
  end;

  begin
    program := (select string_agg(format('P(v%s)', n), ', ')
                from generate_series(1, 33) as g(n)) || '?';
    perform *
    from liquid.query(program) as t(dummy text);
    raise exception 'expected variable limit rejection';
  exception
    when program_limit_exceeded then
      raise notice 'variable limit rejected';
  end;

  begin
    program := 'Goal(x) :- '
      || (select string_agg(
                   case
                     when n = 1 then format('Edge(x, "p%s", "o%s")', n, n)
                     else format('Edge("s%s", "p%s", "o%s")', n, n, n)
                   end,
                   ', '
                 )
          from generate_series(1, 65) as g(n))
      || '. Goal(result)?';
    perform *
    from liquid.query(program) as t(result text);
    raise exception 'expected body-atom limit rejection';
  exception
    when program_limit_exceeded then
      raise notice 'body atom limit rejected';
  end;

  begin
    program := 'TooWide@('
      || (select string_agg(format('r%s="v%s"', n, n), ', ')
          from generate_series(1, 17) as g(n))
      || ')?';
    perform *
    from liquid.query(program) as t(dummy text);
    raise exception 'expected compound role limit rejection';
  exception
    when program_limit_exceeded then
      raise notice 'compound role limit rejected';
  end;
end $$;
