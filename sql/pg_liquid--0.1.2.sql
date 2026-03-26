-- pg_liquid 0.1.2 — public PGXN release

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

create or replace function liquid.query_as(principal text, program text)
returns setof record
as 'MODULE_PATHNAME', 'liquid_query_as'
language c
strict
parallel restricted;

create or replace function liquid.read_as(principal text, program text)
returns setof record
as 'MODULE_PATHNAME', 'liquid_read_as'
language c
strict
security definer
set search_path = pg_catalog, public, pg_temp
parallel restricted;

revoke all on function liquid.query_as(text, text) from public;
revoke all on function liquid.read_as(text, text) from public;

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
    ('liquid/acts_for'),
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
    ('ReadPredicate'),
    ('ReadCompound'),
    ('ReadTriple'),
    ('PredicateReadBySubject'),
    ('PredicateReadByObject'),
    ('CompoundReadByRole'),
    ('Principal'),
    ('id'),
    ('kind'),
    ('principal'),
    ('user'),
    ('subject'),
    ('predicate'),
    ('object'),
    ('compound_type'),
    ('relation'),
    ('role')
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
DefCompound("ReadPredicate", "principal", "0", "liquid/string").
DefCompound("ReadPredicate", "predicate", "0", "liquid/node").
Edge("ReadPredicate", "liquid/mutable", "false").
DefCompound("ReadCompound", "principal", "0", "liquid/string").
DefCompound("ReadCompound", "compound_type", "0", "liquid/node").
Edge("ReadCompound", "liquid/mutable", "false").
DefCompound("ReadTriple", "principal", "0", "liquid/string").
DefCompound("ReadTriple", "user", "0", "liquid/string").
DefCompound("ReadTriple", "subject", "0", "liquid/node").
DefCompound("ReadTriple", "predicate", "0", "liquid/node").
DefCompound("ReadTriple", "object", "0", "liquid/node").
Edge("ReadTriple", "liquid/mutable", "false").
DefCompound("PredicateReadBySubject", "predicate", "0", "liquid/node").
DefCompound("PredicateReadBySubject", "relation", "0", "liquid/node").
Edge("PredicateReadBySubject", "liquid/mutable", "false").
DefCompound("PredicateReadByObject", "predicate", "0", "liquid/node").
DefCompound("PredicateReadByObject", "relation", "0", "liquid/node").
Edge("PredicateReadByObject", "liquid/mutable", "false").
DefCompound("CompoundReadByRole", "compound_type", "0", "liquid/node").
DefCompound("CompoundReadByRole", "role", "0", "liquid/node").
Edge("CompoundReadByRole", "liquid/mutable", "false").
DefCompound("Principal", "id", "0", "liquid/string").
DefCompound("Principal", "kind", "0", "liquid/string").
Edge("Principal", "liquid/mutable", "false").
$liquid$) as t(ignored text);
exception
  when invalid_parameter_value then
    null;
end $$;

create table if not exists liquid.row_normalizers (
  id bigserial primary key,
  source_table oid not null,
  normalizer_name text not null,
  compound_type text not null,
  role_columns jsonb not null,
  primary_key_columns text[] not null,
  unique (source_table, normalizer_name),
  check (jsonb_typeof(role_columns) = 'object')
);

create table if not exists liquid.row_normalizer_bindings (
  normalizer_id bigint not null references liquid.row_normalizers(id) on delete cascade,
  source_row_key jsonb not null,
  subject_literal text not null,
  predicate_literal text not null,
  object_literal text not null,
  primary key (normalizer_id, source_row_key, subject_literal, predicate_literal, object_literal)
);

create index if not exists idx_row_normalizer_bindings_triple
  on liquid.row_normalizer_bindings (subject_literal, predicate_literal, object_literal);

create or replace function liquid._compound_roles(compound_type text)
returns text[]
language sql
stable
set search_path = pg_catalog, public, pg_temp
as $$
  select coalesce(array_agg(role_v.literal order by role_v.literal), array[]::text[])
  from liquid.vertices compound_v
  join liquid.edges edge
    on edge.subject_id = compound_v.id
   and edge.is_deleted = false
  join liquid.vertices predicate_v
    on predicate_v.id = edge.predicate_id
  join liquid.vertices role_v
    on role_v.id = edge.object_id
  where compound_v.literal = $1
    and predicate_v.literal = 'liquid/compound_predicate'
$$;

create or replace function liquid.compound_identity_literal(compound_type text, role_values jsonb)
returns text
language sql
stable
strict
set search_path = pg_catalog, public, pg_temp
as $$
  select format(
    '%s@(%s)',
    $1,
    coalesce(
      (
        select string_agg(format('%s=%L', key, value), ', ' order by key)
        from jsonb_each_text($2)
      ),
      ''
    )
  )
$$;

create or replace function liquid.project_compound_edges(compound_type text, role_values jsonb)
returns table(subject_literal text, predicate_literal text, object_literal text)
language plpgsql
stable
strict
set search_path = pg_catalog, public, pg_temp
as $$
declare
  expected_roles text[];
  provided_roles text[];
  compound_literal text;
begin
  if jsonb_typeof(role_values) <> 'object' then
    raise exception 'role_values must be a JSON object';
  end if;

  expected_roles := liquid._compound_roles(compound_type);
  if coalesce(array_length(expected_roles, 1), 0) = 0 then
    raise exception 'unknown compound type: %', compound_type;
  end if;

  select coalesce(array_agg(key order by key), array[]::text[])
    into provided_roles
  from jsonb_object_keys(role_values) as keys(key);

  if expected_roles <> provided_roles then
    raise exception 'compound role mismatch for %: expected %, got %',
      compound_type,
      expected_roles,
      provided_roles;
  end if;

  compound_literal := liquid.compound_identity_literal(compound_type, role_values);

  return query
  select compound_literal, projected.key, projected.value
  from jsonb_each_text(role_values) as projected
  order by projected.key;
end;
$$;

create or replace function liquid._ensure_edge(subject_literal text, predicate_literal text, object_literal text)
returns void
language plpgsql
volatile
strict
security definer
set search_path = pg_catalog, public, pg_temp
as $$
declare
  v_subject_id bigint;
  v_predicate_id bigint;
  v_object_id bigint;
begin
  insert into liquid.vertices (literal)
  values (subject_literal), (predicate_literal), (object_literal)
  on conflict (literal) do nothing;

  select id into v_subject_id
  from liquid.vertices
  where literal = subject_literal;

  select id into v_predicate_id
  from liquid.vertices
  where literal = predicate_literal;

  select id into v_object_id
  from liquid.vertices
  where literal = object_literal;

  insert into liquid.edges (subject_id, predicate_id, object_id)
  values (v_subject_id, v_predicate_id, v_object_id)
  on conflict (subject_id, predicate_id, object_id)
  do update set is_deleted = false, tx_id = txid_current();
end;
$$;

create or replace function liquid._tombstone_edge(subject_literal text, predicate_literal text, object_literal text)
returns void
language plpgsql
volatile
strict
security definer
set search_path = pg_catalog, public, pg_temp
as $$
declare
  v_subject_id bigint;
  v_predicate_id bigint;
  v_object_id bigint;
begin
  select id into v_subject_id
  from liquid.vertices
  where literal = subject_literal;

  select id into v_predicate_id
  from liquid.vertices
  where literal = predicate_literal;

  select id into v_object_id
  from liquid.vertices
  where literal = object_literal;

  if v_subject_id is null or v_predicate_id is null or v_object_id is null then
    return;
  end if;

  update liquid.edges
     set is_deleted = true,
         tx_id = txid_current()
   where liquid.edges.subject_id = v_subject_id
     and liquid.edges.predicate_id = v_predicate_id
     and liquid.edges.object_id = v_object_id;
end;
$$;

create or replace function liquid._build_source_row_key(row_data jsonb, primary_key_columns text[])
returns jsonb
language plpgsql
stable
set search_path = pg_catalog, public, pg_temp
as $$
declare
  result jsonb := '{}'::jsonb;
  column_name text;
begin
  if row_data is null then
    return null;
  end if;

  foreach column_name in array primary_key_columns loop
    if not (row_data ? column_name) then
      raise exception 'source row does not contain primary key column %', column_name;
    end if;

    result := result || jsonb_build_object(column_name, row_data -> column_name);
  end loop;

  return result;
end;
$$;

create or replace function liquid._build_role_values(row_data jsonb, role_columns jsonb)
returns jsonb
language plpgsql
stable
set search_path = pg_catalog, public, pg_temp
as $$
declare
  result jsonb := '{}'::jsonb;
  role_name text;
  column_name text;
  role_value text;
begin
  if row_data is null then
    return null;
  end if;

  for role_name, column_name in
    select key, value
    from jsonb_each_text(role_columns)
    order by key
  loop
    if not (row_data ? column_name) then
      raise exception 'source row does not contain mapped column %', column_name;
    end if;

    role_value := row_data ->> column_name;
    if role_value is null then
      return null;
    end if;

    result := result || jsonb_build_object(role_name, role_value);
  end loop;

  return result;
end;
$$;

create or replace function liquid._project_row_normalizer_bindings(p_normalizer_id bigint, row_data jsonb)
returns table(source_row_key jsonb, subject_literal text, predicate_literal text, object_literal text)
language plpgsql
stable
set search_path = pg_catalog, public, pg_temp
as $$
declare
  normalizer liquid.row_normalizers%rowtype;
  role_values jsonb;
  projected_key jsonb;
begin
  if row_data is null then
    return;
  end if;

  select *
    into strict normalizer
  from liquid.row_normalizers
  where id = p_normalizer_id;

  projected_key := liquid._build_source_row_key(row_data, normalizer.primary_key_columns);
  role_values := liquid._build_role_values(row_data, normalizer.role_columns);

  if role_values is null then
    return;
  end if;

  return query
  select projected_key, projected.subject_literal, projected.predicate_literal, projected.object_literal
  from liquid.project_compound_edges(normalizer.compound_type, role_values) as projected;
end;
$$;

create or replace function liquid._deproject_normalizer(p_normalizer_id bigint)
returns void
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, pg_temp
as $$
declare
  binding record;
begin
  for binding in
    with deleted as (
      delete from liquid.row_normalizer_bindings
      where normalizer_id = p_normalizer_id
      returning subject_literal, predicate_literal, object_literal
    )
    select distinct subject_literal, predicate_literal, object_literal
    from deleted
  loop
    if not exists (
      select 1
      from liquid.row_normalizer_bindings remaining
      where remaining.subject_literal = binding.subject_literal
        and remaining.predicate_literal = binding.predicate_literal
        and remaining.object_literal = binding.object_literal
    ) then
      perform liquid._tombstone_edge(binding.subject_literal, binding.predicate_literal, binding.object_literal);
    end if;
  end loop;
end;
$$;

create or replace function liquid._apply_row_normalizer_change(p_normalizer_id bigint, old_row jsonb, new_row jsonb)
returns void
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, pg_temp
as $$
declare
  binding record;
  old_key jsonb;
  primary_key_columns text[];
begin
  if new_row is not null then
    for binding in
      select projected.source_row_key,
             projected.subject_literal,
             projected.predicate_literal,
             projected.object_literal
      from liquid._project_row_normalizer_bindings(p_normalizer_id, new_row) as projected
    loop
      insert into liquid.row_normalizer_bindings (
        normalizer_id,
        source_row_key,
        subject_literal,
        predicate_literal,
        object_literal
      )
      values (
        p_normalizer_id,
        binding.source_row_key,
        binding.subject_literal,
        binding.predicate_literal,
        binding.object_literal
      )
      on conflict do nothing;

      if found then
        perform liquid._ensure_edge(binding.subject_literal, binding.predicate_literal, binding.object_literal);
      end if;
    end loop;
  end if;

  if old_row is null then
    return;
  end if;

  select row_normalizers.primary_key_columns
    into strict primary_key_columns
  from liquid.row_normalizers
  where id = p_normalizer_id;

  old_key := liquid._build_source_row_key(old_row, primary_key_columns);

  for binding in
    with old_projected as (
      select projected.subject_literal,
             projected.predicate_literal,
             projected.object_literal
      from liquid._project_row_normalizer_bindings(p_normalizer_id, old_row) as projected
    ),
    new_projected as (
      select projected.subject_literal,
             projected.predicate_literal,
             projected.object_literal
      from liquid._project_row_normalizer_bindings(p_normalizer_id, new_row) as projected
    ),
    deleted as (
      delete from liquid.row_normalizer_bindings bindings
      using old_projected old_binding
      where bindings.normalizer_id = p_normalizer_id
        and bindings.source_row_key = old_key
        and bindings.subject_literal = old_binding.subject_literal
        and bindings.predicate_literal = old_binding.predicate_literal
        and bindings.object_literal = old_binding.object_literal
        and not exists (
          select 1
          from new_projected new_binding
          where new_binding.subject_literal = old_binding.subject_literal
            and new_binding.predicate_literal = old_binding.predicate_literal
            and new_binding.object_literal = old_binding.object_literal
        )
      returning bindings.subject_literal, bindings.predicate_literal, bindings.object_literal
    )
    select distinct subject_literal, predicate_literal, object_literal
    from deleted
  loop
    if not exists (
      select 1
      from liquid.row_normalizer_bindings remaining
      where remaining.subject_literal = binding.subject_literal
        and remaining.predicate_literal = binding.predicate_literal
        and remaining.object_literal = binding.object_literal
    ) then
      perform liquid._tombstone_edge(binding.subject_literal, binding.predicate_literal, binding.object_literal);
    end if;
  end loop;
end;
$$;

create or replace function liquid.tg_apply_row_normalizer()
returns trigger
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, pg_temp
as $$
begin
  if tg_nargs < 1 then
    raise exception 'tg_apply_row_normalizer requires normalizer id in TG_ARGV[0]';
  end if;

  if tg_op = 'INSERT' then
    perform liquid._apply_row_normalizer_change(tg_argv[0]::bigint, null, to_jsonb(new));
    return new;
  elsif tg_op = 'UPDATE' then
    perform liquid._apply_row_normalizer_change(tg_argv[0]::bigint, to_jsonb(old), to_jsonb(new));
    return new;
  elsif tg_op = 'DELETE' then
    perform liquid._apply_row_normalizer_change(tg_argv[0]::bigint, to_jsonb(old), null);
    return old;
  end if;

  return coalesce(new, old);
end;
$$;

create or replace function liquid.create_row_normalizer(
  p_source_table regclass,
  p_normalizer_name text,
  p_compound_type text,
  p_role_columns jsonb,
  p_backfill boolean default true
)
returns void
language plpgsql
volatile
set search_path = pg_catalog, public, pg_temp
as $$
declare
  relation_kind "char";
  primary_key_columns text[];
  expected_roles text[];
  provided_roles text[];
  column_name text;
  existing_normalizer_id bigint;
  normalizer_id bigint;
  trigger_name text;
begin
  if jsonb_typeof(p_role_columns) <> 'object' then
    raise exception 'role_columns must be a JSON object';
  end if;

  select class.relkind
    into relation_kind
  from pg_class class
  where class.oid = p_source_table;

  if relation_kind is distinct from 'r' then
    raise exception 'source_table must be an ordinary base table: %', p_source_table::text;
  end if;

  select coalesce(array_agg(attribute.attname order by pk.ord), array[]::text[])
    into primary_key_columns
  from pg_index index_info
  join unnest(index_info.indkey) with ordinality as pk(attnum, ord)
    on true
  join pg_attribute attribute
    on attribute.attrelid = index_info.indrelid
   and attribute.attnum = pk.attnum
  where index_info.indrelid = p_source_table
    and index_info.indisprimary
    and attribute.attnum > 0
    and not attribute.attisdropped;

  if coalesce(array_length(primary_key_columns, 1), 0) = 0 then
    raise exception 'source_table must have a primary key: %', p_source_table::text;
  end if;

  expected_roles := liquid._compound_roles(p_compound_type);
  if coalesce(array_length(expected_roles, 1), 0) = 0 then
    raise exception 'compound_type must already exist with declared roles: %', p_compound_type;
  end if;

  select coalesce(array_agg(key order by key), array[]::text[])
    into provided_roles
  from jsonb_object_keys(p_role_columns) as keys(key);

  if expected_roles <> provided_roles then
    raise exception 'role_columns must cover compound roles exactly: expected %, got %',
      expected_roles,
      provided_roles;
  end if;

  for column_name in
    select value
    from jsonb_each_text(p_role_columns)
  loop
    if not exists (
      select 1
      from pg_attribute attribute
      where attribute.attrelid = p_source_table
        and attribute.attname = column_name
        and attribute.attnum > 0
        and not attribute.attisdropped
    ) then
      raise exception 'mapped source column does not exist on %: %', p_source_table::text, column_name;
    end if;
  end loop;

  select id
    into existing_normalizer_id
  from liquid.row_normalizers
  where row_normalizers.source_table = p_source_table
    and row_normalizers.normalizer_name = p_normalizer_name;

  if existing_normalizer_id is not null then
    raise exception 'row normalizer already exists for %: %', p_source_table::text, p_normalizer_name;
  end if;

  insert into liquid.row_normalizers (
    source_table,
    normalizer_name,
    compound_type,
    role_columns,
    primary_key_columns
  )
  values (
    p_source_table,
    p_normalizer_name,
    p_compound_type,
    p_role_columns,
    primary_key_columns
  )
  returning id into normalizer_id;

  trigger_name := format('liquid_row_normalizer_%s', normalizer_id);

  execute format(
    'create trigger %I after insert or update or delete on %s for each row execute function liquid.tg_apply_row_normalizer(%L)',
    trigger_name,
    p_source_table,
    normalizer_id::text
  );

  if p_backfill then
    perform liquid.rebuild_row_normalizer(p_source_table, p_normalizer_name);
  end if;
end;
$$;

create or replace function liquid.drop_row_normalizer(
  p_source_table regclass,
  p_normalizer_name text,
  purge boolean default true
)
returns void
language plpgsql
volatile
set search_path = pg_catalog, public, pg_temp
as $$
declare
  normalizer liquid.row_normalizers%rowtype;
  trigger_name text;
begin
  select *
    into normalizer
  from liquid.row_normalizers
  where row_normalizers.source_table = p_source_table
    and row_normalizers.normalizer_name = p_normalizer_name;

  if normalizer.id is null then
    raise exception 'row normalizer not found for %: %', p_source_table::text, p_normalizer_name;
  end if;

  trigger_name := format('liquid_row_normalizer_%s', normalizer.id);

  execute format(
    'drop trigger if exists %I on %s',
    trigger_name,
    p_source_table
  );

  if purge then
    perform liquid._deproject_normalizer(normalizer.id);
  end if;

  delete from liquid.row_normalizers
  where id = normalizer.id;
end;
$$;

create or replace function liquid.rebuild_row_normalizer(
  p_source_table regclass,
  p_normalizer_name text
)
returns void
language plpgsql
volatile
set search_path = pg_catalog, public, pg_temp
as $$
declare
  normalizer liquid.row_normalizers%rowtype;
  row_data jsonb;
begin
  select *
    into normalizer
  from liquid.row_normalizers
  where row_normalizers.source_table = p_source_table
    and row_normalizers.normalizer_name = p_normalizer_name;

  if normalizer.id is null then
    raise exception 'row normalizer not found for %: %', p_source_table::text, p_normalizer_name;
  end if;

  perform liquid._deproject_normalizer(normalizer.id);

  for row_data in
    execute format('select to_jsonb(row_data) from %s as row_data', normalizer.source_table::regclass)
  loop
    perform liquid._apply_row_normalizer_change(normalizer.id, null, row_data);
  end loop;
end;
$$;
