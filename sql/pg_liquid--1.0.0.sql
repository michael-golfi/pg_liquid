-- pg_liquid 1.0.0 compatibility install
--
-- This version exists only to support extension upgrade testing on the
-- current binary. It installs the same storage surface as 1.1 so that
-- ALTER EXTENSION ... UPDATE TO '1.1' can run cleanly.

create table if not exists liquid.vertices (
  id bigserial primary key,
  literal text not null unique
);

create table if not exists liquid.edges (
  subject_id bigint not null references liquid.vertices(id),
  predicate_id bigint not null references liquid.vertices(id),
  object_id bigint not null references liquid.vertices(id),
  tx_id bigint not null default txid_current(),
  is_deleted boolean not null default false,
  primary key (subject_id, predicate_id, object_id)
);

create index if not exists idx_edges_subject_predicate
  on liquid.edges (subject_id, predicate_id)
  where is_deleted = false;

create index if not exists idx_edges_predicate_object
  on liquid.edges (predicate_id, object_id)
  where is_deleted = false;

create index if not exists idx_edges_predicate
  on liquid.edges (predicate_id)
  where is_deleted = false;

create index if not exists idx_edges_subject
  on liquid.edges (subject_id)
  where is_deleted = false;

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
