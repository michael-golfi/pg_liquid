-- pg_liquid 1.0 -> 1.1
-- Reset the extension to the LIquid blog parity surface.

drop function if exists liquid.query_subgraph(text);
drop function if exists liquid.query(text, bigint);
drop function if exists liquid.query_subgraph(text, bigint);
drop function if exists liquid.query_text(text);
drop function if exists liquid.insert_edge(text, text, text);
drop function if exists liquid.insert_edge(text, text, text, bigint);
drop function if exists liquid.delete_edge(text, text, text);
drop function if exists liquid.delete_edge(text, text, text, bigint);
drop function if exists liquid.assert_rule(text);
drop function if exists liquid.insert_compound(text, jsonb);
drop function if exists liquid.get_compound_id(text, jsonb);
drop function if exists liquid.define_predicate(text, integer);
drop function if exists liquid.define_compound(text, text[]);
drop function if exists liquid.set_predicate_policy(text, text, text);
drop function if exists liquid.run_parser_tests();
drop function if exists liquid.run_tests();

create or replace function liquid.query(program text)
returns setof record
as 'MODULE_PATHNAME', 'liquid_query'
language c
strict
parallel restricted;

do $$
begin
  insert into liquid.vertices (literal)
  values
    ('liquid/type'),
    ('liquid/cardinality'),
    ('liquid/subject_meta'),
    ('liquid/object_meta'),
    ('liquid/compound_predicate'),
    ('liquid/mutable'),
    ('liquid/can_read_predicate'),
    ('liquid/can_read_compound'),
    ('liquid/readable_if_subject_has'),
    ('liquid/readable_if_object_has'),
    ('liquid/readable_compound_if_role_has'),
    ('liquid/node'),
    ('liquid/string'),
    ('0'),
    ('1'),
    ('false'),
    ('Smeta'),
    ('Ometa'),
    ('ReadTriple'),
    ('user'),
    ('subject'),
    ('predicate'),
    ('object')
  on conflict (literal) do nothing;
end $$;

do $$
declare
  smeta_id bigint;
  ometa_id bigint;
  compound_predicate_id bigint;
  mutable_id bigint;
  false_id bigint;
  subject_meta_id bigint;
  object_meta_id bigint;
begin
  select id into smeta_id from liquid.vertices where literal = 'Smeta';
  select id into ometa_id from liquid.vertices where literal = 'Ometa';
  select id into compound_predicate_id from liquid.vertices where literal = 'liquid/compound_predicate';
  select id into mutable_id from liquid.vertices where literal = 'liquid/mutable';
  select id into false_id from liquid.vertices where literal = 'false';
  select id into subject_meta_id from liquid.vertices where literal = 'liquid/subject_meta';
  select id into object_meta_id from liquid.vertices where literal = 'liquid/object_meta';

  insert into liquid.edges (subject_id, predicate_id, object_id)
  values
    (smeta_id, compound_predicate_id, subject_meta_id),
    (smeta_id, mutable_id, false_id),
    (ometa_id, compound_predicate_id, object_meta_id),
    (ometa_id, mutable_id, false_id)
  on conflict (subject_id, predicate_id, object_id)
  do update set is_deleted = false, tx_id = txid_current();
end $$;

do $$
begin
  perform *
  from liquid.query($liquid$
DefCompound("ReadTriple", "user", "0", "liquid/string").
DefCompound("ReadTriple", "subject", "0", "liquid/node").
DefCompound("ReadTriple", "predicate", "0", "liquid/node").
DefCompound("ReadTriple", "object", "0", "liquid/node").
Edge("ReadTriple", "liquid/mutable", "false").
$liquid$) as t(ignored text);
exception
  when invalid_parameter_value then
    null;
end $$;
