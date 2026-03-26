#include "postgres.h"
#include "pg_liquid.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "access/xact.h"
#include "utils/fmgroids.h"
#include "common/hashfn.h"
#include "utils/array.h"

typedef struct RoleBinding
{
    const char *role_name;
    const char *role_value;
} RoleBinding;

static SPIPlanPtr catalog_plan_select_identity_id = NULL;
static SPIPlanPtr catalog_plan_select_identity_literal = NULL;
static SPIPlanPtr catalog_plan_insert_identity = NULL;
static SPIPlanPtr catalog_plan_insert_identities_batch = NULL;
static SPIPlanPtr catalog_plan_select_identities_batch = NULL;
static SPIPlanPtr catalog_plan_select_compound_roles = NULL;
static SPIPlanPtr catalog_plan_insert_edge = NULL;
static SPIPlanPtr catalog_plan_undelete_edge = NULL;
static SPIPlanPtr catalog_plan_insert_edges_batch = NULL;
static SPIPlanPtr catalog_plan_undelete_edges_batch = NULL;
static bool       catalog_plans_initialized = false;

typedef struct LiteralIdCacheEntry
{
    uint32                   hash;
    char                    *literal;
    int64                    id;
    struct LiteralIdCacheEntry *next;
} LiteralIdCacheEntry;

typedef struct IdLiteralCacheEntry
{
    int64                    id;
    char                    *literal;
    struct IdLiteralCacheEntry *next;
} IdLiteralCacheEntry;

typedef struct CompoundSchemaCacheEntry
{
    int64       compound_type_id;
    int         expected_count;
    int         expected_capacity;
    int64      *expected_role_ids;
    const char **expected_roles;
} CompoundSchemaCacheEntry;

#define CATALOG_CACHE_BUCKETS 8192

static bool                catalog_caches_initialized = false;
static LiteralIdCacheEntry *catalog_literal_id_cache[CATALOG_CACHE_BUCKETS];
static IdLiteralCacheEntry *catalog_id_literal_cache[CATALOG_CACHE_BUCKETS];
static HTAB               *catalog_compound_schema_cache = NULL;
static MemoryContext       catalog_cache_mcxt = NULL;
static bool                catalog_xact_callbacks_registered = false;

static void catalog_xact_callback(XactEvent event, void *arg);
static void catalog_subxact_callback(SubXactEvent event,
                                     SubTransactionId mySubid,
                                     SubTransactionId parentSubid,
                                     void *arg);

void
reset_catalog_caches(void)
{
    if (catalog_cache_mcxt != NULL)
        MemoryContextReset(catalog_cache_mcxt);

    memset(catalog_literal_id_cache, 0, sizeof(catalog_literal_id_cache));
    memset(catalog_id_literal_cache, 0, sizeof(catalog_id_literal_cache));
    catalog_compound_schema_cache = NULL;
    catalog_caches_initialized = false;
}

void
register_catalog_xact_callbacks(void)
{
    if (catalog_xact_callbacks_registered)
        return;

    RegisterXactCallback(catalog_xact_callback, NULL);
    RegisterSubXactCallback(catalog_subxact_callback, NULL);
    catalog_xact_callbacks_registered = true;
}

static void
catalog_xact_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_COMMIT:
        case XACT_EVENT_PARALLEL_ABORT:
            reset_catalog_caches();
            break;
        default:
            break;
    }
}

static void
catalog_subxact_callback(SubXactEvent event,
                         SubTransactionId mySubid,
                         SubTransactionId parentSubid,
                         void *arg)
{
    if (event == SUBXACT_EVENT_ABORT_SUB)
        reset_catalog_caches();
}

static void
ensure_catalog_caches(void)
{
    if (catalog_caches_initialized)
        return;

    if (catalog_cache_mcxt == NULL)
        catalog_cache_mcxt = AllocSetContextCreate(TopMemoryContext,
                                                   "LIquid Catalog Cache",
                                                   ALLOCSET_SMALL_SIZES);

    memset(catalog_literal_id_cache, 0, sizeof(catalog_literal_id_cache));
    memset(catalog_id_literal_cache, 0, sizeof(catalog_id_literal_cache));
    if (catalog_compound_schema_cache == NULL)
    {
        HASHCTL ctl;

        memset(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(int64);
        ctl.entrysize = sizeof(CompoundSchemaCacheEntry);
        ctl.hcxt = catalog_cache_mcxt;
        catalog_compound_schema_cache = hash_create("LIquid Compound Schema Cache",
                                                    64,
                                                    &ctl,
                                                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }
    catalog_caches_initialized = true;
}

static uint32
catalog_literal_hash(const char *literal)
{
    return DatumGetUInt32(hash_any((const unsigned char *) literal,
                                   (int) strlen(literal)));
}

static bool
catalog_cache_lookup_literal_id(const char *literal, int64 *id_out)
{
    uint32              hash = catalog_literal_hash(literal);
    uint32              bucket = hash % CATALOG_CACHE_BUCKETS;
    LiteralIdCacheEntry *entry = catalog_literal_id_cache[bucket];

    while (entry != NULL)
    {
        if (entry->hash == hash && strcmp(entry->literal, literal) == 0)
        {
            *id_out = entry->id;
            return true;
        }
        entry = entry->next;
    }

    return false;
}

static bool
catalog_cache_lookup_id_literal(int64 id, const char **literal_out)
{
    uint32               bucket = ((uint32) id) % CATALOG_CACHE_BUCKETS;
    IdLiteralCacheEntry *entry = catalog_id_literal_cache[bucket];

    while (entry != NULL)
    {
        if (entry->id == id)
        {
            *literal_out = entry->literal;
            return true;
        }
        entry = entry->next;
    }

    return false;
}

static void
catalog_cache_store(int64 id, const char *literal)
{
    uint32               hash;
    uint32               lit_bucket;
    uint32               id_bucket;
    LiteralIdCacheEntry *lit_entry;
    IdLiteralCacheEntry *id_entry;
    MemoryContext        oldcxt;

    if (id < 0 || literal == NULL)
        return;

    ensure_catalog_caches();

    if (catalog_cache_lookup_literal_id(literal, &id))
        return;

    hash = catalog_literal_hash(literal);
    lit_bucket = hash % CATALOG_CACHE_BUCKETS;
    id_bucket = ((uint32) id) % CATALOG_CACHE_BUCKETS;

    oldcxt = MemoryContextSwitchTo(catalog_cache_mcxt);

    lit_entry = MemoryContextAlloc(catalog_cache_mcxt, sizeof(LiteralIdCacheEntry));
    lit_entry->hash = hash;
    lit_entry->literal = pstrdup(literal);
    lit_entry->id = id;
    lit_entry->next = catalog_literal_id_cache[lit_bucket];
    catalog_literal_id_cache[lit_bucket] = lit_entry;

    id_entry = MemoryContextAlloc(catalog_cache_mcxt, sizeof(IdLiteralCacheEntry));
    id_entry->id = id;
    id_entry->literal = lit_entry->literal;
    id_entry->next = catalog_id_literal_cache[id_bucket];
    catalog_id_literal_cache[id_bucket] = id_entry;

    MemoryContextSwitchTo(oldcxt);
}

static void
ensure_catalog_plans(void)
{
    static const Oid one_text_arg[1] = {TEXTOID};
    static const Oid one_int8_arg[1] = {INT8OID};
    static const Oid one_text_array_arg[1] = {TEXTARRAYOID};
    static const Oid three_int8_args[3] = {INT8OID, INT8OID, INT8OID};
    static const Oid three_int8_array_args[3] = {INT8ARRAYOID, INT8ARRAYOID, INT8ARRAYOID};

    if (catalog_plans_initialized)
        return;

    catalog_plan_select_identity_id = SPI_prepare(
        "SELECT id FROM liquid.vertices WHERE literal = $1 LIMIT 1",
        1,
        (Oid *) one_text_arg);
    if (catalog_plan_select_identity_id != NULL)
        SPI_keepplan(catalog_plan_select_identity_id);

    catalog_plan_select_identity_literal = SPI_prepare(
        "SELECT literal FROM liquid.vertices WHERE id = $1 LIMIT 1",
        1,
        (Oid *) one_int8_arg);
    if (catalog_plan_select_identity_literal != NULL)
        SPI_keepplan(catalog_plan_select_identity_literal);

    catalog_plan_insert_identity = SPI_prepare(
        "INSERT INTO liquid.vertices (literal) VALUES ($1) "
        "ON CONFLICT (literal) DO NOTHING "
        "RETURNING id",
        1,
        (Oid *) one_text_arg);
    if (catalog_plan_insert_identity != NULL)
        SPI_keepplan(catalog_plan_insert_identity);

    catalog_plan_insert_identities_batch = SPI_prepare(
        "INSERT INTO liquid.vertices (literal) "
        "SELECT DISTINCT literal "
        "FROM unnest($1::text[]) AS t(literal) "
        "ON CONFLICT (literal) DO NOTHING",
        1,
        (Oid *) one_text_array_arg);
    if (catalog_plan_insert_identities_batch != NULL)
        SPI_keepplan(catalog_plan_insert_identities_batch);

    catalog_plan_select_identities_batch = SPI_prepare(
        "SELECT id, literal "
        "FROM liquid.vertices "
        "WHERE literal = ANY($1::text[])",
        1,
        (Oid *) one_text_array_arg);
    if (catalog_plan_select_identities_batch != NULL)
        SPI_keepplan(catalog_plan_select_identities_batch);

    catalog_plan_select_compound_roles = SPI_prepare(
        "SELECT role_v.id, role_v.literal "
        "FROM liquid.edges edge "
        "JOIN liquid.vertices predicate_v "
        "  ON predicate_v.id = edge.predicate_id "
        "JOIN liquid.vertices role_v "
        "  ON role_v.id = edge.object_id "
        "WHERE edge.subject_id = $1 "
        "  AND edge.is_deleted = false "
        "  AND predicate_v.literal = 'liquid/compound_predicate' "
        "ORDER BY role_v.literal",
        1,
        (Oid *) one_int8_arg);
    if (catalog_plan_select_compound_roles != NULL)
        SPI_keepplan(catalog_plan_select_compound_roles);

    catalog_plan_insert_edge = SPI_prepare(
        "INSERT INTO liquid.edges (subject_id, predicate_id, object_id) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (subject_id, predicate_id, object_id) "
        "DO NOTHING",
        3,
        (Oid *) three_int8_args);
    if (catalog_plan_insert_edge != NULL)
        SPI_keepplan(catalog_plan_insert_edge);

    catalog_plan_undelete_edge = SPI_prepare(
        "UPDATE liquid.edges "
        "SET is_deleted = false, tx_id = txid_current() "
        "WHERE subject_id = $1 "
        "  AND predicate_id = $2 "
        "  AND object_id = $3 "
        "  AND is_deleted = true",
        3,
        (Oid *) three_int8_args);
    if (catalog_plan_undelete_edge != NULL)
        SPI_keepplan(catalog_plan_undelete_edge);

    catalog_plan_insert_edges_batch = SPI_prepare(
        "INSERT INTO liquid.edges (subject_id, predicate_id, object_id) "
        "SELECT subject_id, predicate_id, object_id "
        "FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) "
        "AS t(subject_id, predicate_id, object_id) "
        "ON CONFLICT (subject_id, predicate_id, object_id) "
        "DO NOTHING",
        3,
        (Oid *) three_int8_array_args);
    if (catalog_plan_insert_edges_batch != NULL)
        SPI_keepplan(catalog_plan_insert_edges_batch);

    catalog_plan_undelete_edges_batch = SPI_prepare(
        "UPDATE liquid.edges e "
        "SET is_deleted = false, tx_id = txid_current() "
        "FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) "
        "AS t(subject_id, predicate_id, object_id) "
        "WHERE e.subject_id = t.subject_id "
        "  AND e.predicate_id = t.predicate_id "
        "  AND e.object_id = t.object_id "
        "  AND e.is_deleted = true",
        3,
        (Oid *) three_int8_array_args);
    if (catalog_plan_undelete_edges_batch != NULL)
        SPI_keepplan(catalog_plan_undelete_edges_batch);

    catalog_plans_initialized = true;
}

static void
record_compound_schema_role(int64 compound_type_id,
                            int64 role_id,
                            const char *role_literal)
{
    CompoundSchemaCacheEntry *entry;
    bool                      found;
    MemoryContext             oldcxt;
    int                       i;

    ensure_catalog_caches();
    if (catalog_compound_schema_cache == NULL ||
        compound_type_id < 0 ||
        role_id < 0 ||
        role_literal == NULL)
        return;

    entry = hash_search(catalog_compound_schema_cache,
                        &compound_type_id,
                        HASH_ENTER,
                        &found);
    if (!found)
    {
        entry->compound_type_id = compound_type_id;
        entry->expected_count = 0;
        entry->expected_capacity = 0;
        entry->expected_role_ids = NULL;
        entry->expected_roles = NULL;
    }

    for (i = 0; i < entry->expected_count; i++)
    {
        if (entry->expected_role_ids[i] == role_id)
            return;
    }

    oldcxt = MemoryContextSwitchTo(catalog_cache_mcxt);
    if (entry->expected_capacity <= entry->expected_count)
    {
        int new_capacity = (entry->expected_capacity == 0)
            ? 4
            : entry->expected_capacity * 2;

        if (entry->expected_role_ids == NULL)
            entry->expected_role_ids = MemoryContextAlloc(catalog_cache_mcxt,
                                                          new_capacity * sizeof(int64));
        else
            entry->expected_role_ids = repalloc(entry->expected_role_ids,
                                                new_capacity * sizeof(int64));

        if (entry->expected_roles == NULL)
            entry->expected_roles = MemoryContextAlloc(catalog_cache_mcxt,
                                                       new_capacity * sizeof(char *));
        else
            entry->expected_roles = repalloc(entry->expected_roles,
                                             new_capacity * sizeof(char *));

        entry->expected_capacity = new_capacity;
    }

    entry->expected_role_ids[entry->expected_count] = role_id;
    entry->expected_roles[entry->expected_count] = MemoryContextStrdup(catalog_cache_mcxt,
                                                                       role_literal);
    entry->expected_count++;
    MemoryContextSwitchTo(oldcxt);
}

static void
load_compound_schema_roles(int64 compound_type_id,
                           const char *compound_type,
                           const int64 **expected_role_ids_out,
                           const char ***expected_roles_out,
                           int *expected_count_out)
{
    CompoundSchemaCacheEntry *entry;
    bool                      found;
    Datum                     args[1];
    char                      nulls[1] = {' '};
    MemoryContext             oldcxt;
    int                       i;

    ensure_catalog_caches();
    ensure_catalog_plans();
    if (catalog_compound_schema_cache == NULL || catalog_plan_select_compound_roles == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("compound schema lookup cache is unavailable")));

    entry = hash_search(catalog_compound_schema_cache,
                        &compound_type_id,
                        HASH_ENTER,
                        &found);
    if (!found)
    {
        entry->compound_type_id = compound_type_id;
        entry->expected_count = 0;
        entry->expected_capacity = 0;
        entry->expected_role_ids = NULL;
        entry->expected_roles = NULL;
    }

    if (entry->expected_role_ids == NULL || entry->expected_roles == NULL)
    {
        args[0] = Int64GetDatum(compound_type_id);
        if (SPI_execute_plan(catalog_plan_select_compound_roles,
                             args,
                             nulls,
                             false,
                             0) != SPI_OK_SELECT)
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to load schema for compound type %s",
                            compound_type)));

        entry->expected_count = (int) SPI_processed;
        if (entry->expected_count <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("unknown compound type: %s", compound_type)));

        oldcxt = MemoryContextSwitchTo(catalog_cache_mcxt);
        entry->expected_capacity = entry->expected_count;
        entry->expected_role_ids = MemoryContextAlloc(catalog_cache_mcxt,
                                                      sizeof(int64) * entry->expected_count);
        entry->expected_roles = MemoryContextAlloc(catalog_cache_mcxt,
                                                   sizeof(char *) * entry->expected_count);
        for (i = 0; i < entry->expected_count; i++)
        {
            bool  isnull_id;
            char *value = SPI_getvalue(SPI_tuptable->vals[i],
                                       SPI_tuptable->tupdesc,
                                       2);

            entry->expected_role_ids[i] = DatumGetInt64(
                SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc,
                              1,
                              &isnull_id));
            entry->expected_roles[i] = MemoryContextStrdup(catalog_cache_mcxt,
                                                           value ? value : "");
            if (value != NULL)
                pfree(value);
        }
        MemoryContextSwitchTo(oldcxt);
    }

    *expected_role_ids_out = entry->expected_role_ids;
    *expected_roles_out = entry->expected_roles;
    *expected_count_out = entry->expected_count;
}

static int
compare_role_bindings(const void *lhs, const void *rhs)
{
    const RoleBinding *left  = (const RoleBinding *) lhs;
    const RoleBinding *right = (const RoleBinding *) rhs;
    int               cmp;

    cmp = strcmp(left->role_name, right->role_name);
    if (cmp != 0)
        return cmp;
    return strcmp(left->role_value, right->role_value);
}

static int
compare_string_ptrs(const void *lhs, const void *rhs)
{
    const char *const *left = (const char *const *) lhs;
    const char *const *right = (const char *const *) rhs;

    return strcmp(*left, *right);
}

char *
build_compound_identity_literal(const char *compound_type,
                                int num_roles,
                                const char *const *role_names,
                                const char *const *role_values)
{
    StringInfoData literal;
    RoleBinding   *bindings;
    int            i;

    bindings = palloc(sizeof(RoleBinding) * num_roles);
    for (i = 0; i < num_roles; i++)
    {
        bindings[i].role_name  = role_names[i];
        bindings[i].role_value = role_values[i];
    }
    qsort(bindings, num_roles, sizeof(RoleBinding), compare_role_bindings);

    initStringInfo(&literal);
    appendStringInfo(&literal, "%s@(", compound_type);
    for (i = 0; i < num_roles; i++)
    {
        if (i > 0)
            appendStringInfoString(&literal, ", ");
        appendStringInfo(&literal, "%s=%s",
                         bindings[i].role_name,
                         quote_literal_cstr(bindings[i].role_value));
    }
    appendStringInfoChar(&literal, ')');

    pfree(bindings);
    return literal.data;
}

static void
validate_compound_schema(const char *compound_type,
                         int num_roles,
                         const char *const *role_names,
                         const int64 *role_ids)
{
    int64            compound_type_id;
    const int64     *expected_role_ids;
    const char     **expected_roles;
    int64            provided_role_ids[MAX_COMPOUND_ARGS];
    int              expected_count;
    bool             mismatch = false;
    int              i;
    StringInfoData   expected_list;
    StringInfoData   provided_list;

    if (num_roles <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("compound %s requires at least one role value",
                        compound_type)));

    compound_type_id = lookup_identity(compound_type);
    if (compound_type_id < 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("unknown compound type: %s", compound_type)));

    load_compound_schema_roles(compound_type_id,
                               compound_type,
                               &expected_role_ids,
                               &expected_roles,
                               &expected_count);

    for (i = 0; i < num_roles; i++)
    {
        if (role_ids != NULL && role_ids[i] >= 0)
            provided_role_ids[i] = role_ids[i];
        else
            provided_role_ids[i] = lookup_identity(role_names[i]);
    }

    mismatch = expected_count != num_roles;
    if (!mismatch)
    {
        for (i = 0; i < num_roles; i++)
        {
            int   j;
            bool  found = false;

            if (provided_role_ids[i] < 0)
            {
                mismatch = true;
                break;
            }

            for (j = 0; j < i; j++)
            {
                if (provided_role_ids[j] == provided_role_ids[i])
                {
                    mismatch = true;
                    break;
                }
            }
            if (mismatch)
                break;

            for (j = 0; j < expected_count; j++)
            {
                if (expected_role_ids[j] == provided_role_ids[i])
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                mismatch = true;
                break;
            }
        }
    }

    if (!mismatch)
        return;

    initStringInfo(&expected_list);
    for (i = 0; i < expected_count; i++)
    {
        if (i > 0)
            appendStringInfoString(&expected_list, ", ");
        appendStringInfoString(&expected_list, expected_roles[i]);
    }

    initStringInfo(&provided_list);
    for (i = 0; i < num_roles; i++)
    {
        if (i > 0)
            appendStringInfoString(&provided_list, ", ");
        appendStringInfoString(&provided_list, role_names[i]);
    }

    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("compound role mismatch for %s: expected (%s), got (%s)",
                    compound_type,
                    expected_list.data,
                    provided_list.data)));
}

static int64
select_identity_id(const char *literal)
{
    Datum values[1];
    char  nulls[1] = {' '};
    bool  isnull;
    int64 cached_id;

    ensure_catalog_caches();
    ensure_catalog_plans();

    if (catalog_cache_lookup_literal_id(literal, &cached_id))
        return cached_id;

    if (catalog_plan_select_identity_id == NULL)
        return -1;

    values[0] = CStringGetTextDatum(literal);

    if (SPI_execute_plan(catalog_plan_select_identity_id,
                         values,
                         nulls,
                         false,
                         1) == SPI_OK_SELECT &&
        SPI_processed > 0)
    {
        int64 id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                               SPI_tuptable->tupdesc, 1, &isnull));

        catalog_cache_store(id, literal);
        return id;
    }

    return -1;
}

static const char *
select_identity_literal_ref(int64 id)
{
    Datum values[1];
    char  nulls[1] = {' '};
    const char *cached_literal;

    ensure_catalog_caches();
    ensure_catalog_plans();

    if (catalog_cache_lookup_id_literal(id, &cached_literal))
        return cached_literal;

    if (catalog_plan_select_identity_literal == NULL)
        return NULL;

    values[0] = Int64GetDatum(id);

    if (SPI_execute_plan(catalog_plan_select_identity_literal,
                         values,
                         nulls,
                         false,
                         1) == SPI_OK_SELECT &&
        SPI_processed > 0)
    {
        char *value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
        if (value != NULL)
        {
            catalog_cache_store(id, value);
            pfree(value);
            if (catalog_cache_lookup_id_literal(id, &cached_literal))
                return cached_literal;
        }
    }

    return NULL;
}

int64
lookup_identity(const char *literal)
{
    return select_identity_id(literal);
}

char *
lookup_identity_literal(int64 id)
{
    const char *literal = select_identity_literal_ref(id);

    return literal ? pstrdup(literal) : NULL;
}

const char *
lookup_identity_literal_ref(int64 id)
{
    return select_identity_literal_ref(id);
}

int64
get_or_create_identity(const char *literal)
{
    Datum   values[1];
    char    nulls[1] = {' '};
    bool    isnull;
    int64   cached_id;

    ensure_catalog_caches();
    ensure_catalog_plans();

    if (catalog_cache_lookup_literal_id(literal, &cached_id))
        return cached_id;

    values[0] = CStringGetTextDatum(literal);

    if (catalog_plan_insert_identity != NULL &&
        SPI_execute_plan(catalog_plan_insert_identity,
                         values,
                         nulls,
                         false,
                         1) == SPI_OK_INSERT_RETURNING &&
        SPI_processed > 0)
    {
        int64 id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                               SPI_tuptable->tupdesc, 1, &isnull));

        catalog_cache_store(id, literal);
        return id;
    }

    return select_identity_id(literal);
}

void
preload_identities_batch(const char **literals, int count)
{
    Datum      args[1];
    char       nulls[1] = {' '};
    Datum     *literal_datums;
    ArrayType *literal_array;
    const char **pending_literals;
    int64       cached_id;
    int         pending_count = 0;
    int         unique_count = 0;
    int        i;

    if (count <= 0)
        return;

    ensure_catalog_caches();
    ensure_catalog_plans();

    pending_literals = palloc(sizeof(char *) * count);
    for (i = 0; i < count; i++)
    {
        if (literals[i] == NULL)
            continue;
        if (catalog_cache_lookup_literal_id(literals[i], &cached_id))
            continue;
        pending_literals[pending_count++] = literals[i];
    }

    if (pending_count <= 0)
    {
        pfree(pending_literals);
        return;
    }

    qsort(pending_literals,
          pending_count,
          sizeof(char *),
          compare_string_ptrs);

    for (i = 0; i < pending_count; i++)
    {
        if (i > 0 && strcmp(pending_literals[i - 1], pending_literals[i]) == 0)
            continue;
        pending_literals[unique_count++] = pending_literals[i];
    }

    literal_datums = palloc(sizeof(Datum) * unique_count);
    for (i = 0; i < unique_count; i++)
        literal_datums[i] = CStringGetTextDatum(pending_literals[i]);

    literal_array = construct_array(literal_datums,
                                    unique_count,
                                    TEXTOID,
                                    -1,
                                    false,
                                    TYPALIGN_INT);
    args[0] = PointerGetDatum(literal_array);

    if (catalog_plan_insert_identities_batch != NULL)
        SPI_execute_plan(catalog_plan_insert_identities_batch, args, nulls, false, 0);
    else
    {
        Oid argtypes[1] = {TEXTARRAYOID};

        SPI_execute_with_args(
            "INSERT INTO liquid.vertices (literal) "
            "SELECT DISTINCT literal "
            "FROM unnest($1::text[]) AS t(literal) "
            "ON CONFLICT (literal) DO NOTHING",
            1,
            argtypes,
            args,
            nulls,
            false,
            0);
    }

    if (catalog_plan_select_identities_batch != NULL &&
        SPI_execute_plan(catalog_plan_select_identities_batch, args, nulls, false, 0) == SPI_OK_SELECT)
    {
        for (i = 0; i < (int) SPI_processed; i++)
        {
            bool  isnull_id;
            int64 id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[i],
                                                   SPI_tuptable->tupdesc,
                                                   1,
                                                   &isnull_id));
            char *literal = SPI_getvalue(SPI_tuptable->vals[i],
                                         SPI_tuptable->tupdesc,
                                         2);

            if (!isnull_id && literal != NULL)
            {
                catalog_cache_store(id, literal);
                pfree(literal);
            }
        }
    }

    pfree(literal_datums);
    pfree(pending_literals);
}

void
insert_edge_internal(int64 subject_id, int64 predicate_id, int64 object_id)
{
    Datum values[3];
    char  nulls[3] = {' ', ' ', ' '};
    int   ret;

    ensure_catalog_plans();

    values[0] = Int64GetDatum(subject_id);
    values[1] = Int64GetDatum(predicate_id);
    values[2] = Int64GetDatum(object_id);

    if (catalog_plan_insert_edge != NULL && catalog_plan_undelete_edge != NULL)
    {
        ret = SPI_execute_plan(catalog_plan_insert_edge, values, nulls, false, 0);
        if (ret == SPI_OK_INSERT && SPI_processed == 0)
            SPI_execute_plan(catalog_plan_undelete_edge, values, nulls, false, 0);
    }
    else
    {
        Oid argtypes[3] = {INT8OID, INT8OID, INT8OID};

        ret = SPI_execute_with_args(
            "INSERT INTO liquid.edges (subject_id, predicate_id, object_id) "
            "VALUES ($1, $2, $3) "
            "ON CONFLICT (subject_id, predicate_id, object_id) "
            "DO NOTHING",
            3, argtypes, values, nulls, false, 0);
        if (ret == SPI_OK_INSERT && SPI_processed == 0)
        {
            SPI_execute_with_args(
                "UPDATE liquid.edges "
                "SET is_deleted = false, tx_id = txid_current() "
                "WHERE subject_id = $1 "
                "  AND predicate_id = $2 "
                "  AND object_id = $3 "
                "  AND is_deleted = true",
                3, argtypes, values, nulls, false, 0);
        }
    }
}

void
insert_edges_batch(const int64 *subject_ids,
                   const int64 *predicate_ids,
                   const int64 *object_ids,
                   int count)
{
    Datum      args[3];
    char       nulls[3] = {' ', ' ', ' '};
    Datum     *subject_datums;
    Datum     *predicate_datums;
    Datum     *object_datums;
    ArrayType *subject_array;
    ArrayType *predicate_array;
    ArrayType *object_array;
    int        ret;
    int        i;

    if (count <= 0)
        return;

    ensure_catalog_plans();

    subject_datums = palloc(sizeof(Datum) * count);
    predicate_datums = palloc(sizeof(Datum) * count);
    object_datums = palloc(sizeof(Datum) * count);

    for (i = 0; i < count; i++)
    {
        subject_datums[i] = Int64GetDatum(subject_ids[i]);
        predicate_datums[i] = Int64GetDatum(predicate_ids[i]);
        object_datums[i] = Int64GetDatum(object_ids[i]);
    }

    subject_array = construct_array(subject_datums, count, INT8OID, 8, true, TYPALIGN_INT);
    predicate_array = construct_array(predicate_datums, count, INT8OID, 8, true, TYPALIGN_INT);
    object_array = construct_array(object_datums, count, INT8OID, 8, true, TYPALIGN_INT);

    args[0] = PointerGetDatum(subject_array);
    args[1] = PointerGetDatum(predicate_array);
    args[2] = PointerGetDatum(object_array);

    if (catalog_plan_insert_edges_batch != NULL &&
        catalog_plan_undelete_edges_batch != NULL)
    {
        ret = SPI_execute_plan(catalog_plan_insert_edges_batch, args, nulls, false, 0);
        if (ret == SPI_OK_INSERT && (int) SPI_processed < count)
            SPI_execute_plan(catalog_plan_undelete_edges_batch, args, nulls, false, 0);
    }
    else
    {
        Oid argtypes[3] = {INT8ARRAYOID, INT8ARRAYOID, INT8ARRAYOID};

        ret = SPI_execute_with_args(
            "INSERT INTO liquid.edges (subject_id, predicate_id, object_id) "
            "SELECT subject_id, predicate_id, object_id "
            "FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) "
            "AS t(subject_id, predicate_id, object_id) "
            "ON CONFLICT (subject_id, predicate_id, object_id) "
            "DO NOTHING",
            3,
            argtypes,
            args,
            nulls,
            false,
            0);
        if (ret == SPI_OK_INSERT && (int) SPI_processed < count)
        {
            SPI_execute_with_args(
                "UPDATE liquid.edges e "
                "SET is_deleted = false, tx_id = txid_current() "
                "FROM unnest($1::bigint[], $2::bigint[], $3::bigint[]) "
                "AS t(subject_id, predicate_id, object_id) "
                "WHERE e.subject_id = t.subject_id "
                "  AND e.predicate_id = t.predicate_id "
                "  AND e.object_id = t.object_id "
                "  AND e.is_deleted = true",
                3,
                argtypes,
                args,
                nulls,
                false,
                0);
        }
    }

    pfree(subject_datums);
    pfree(predicate_datums);
    pfree(object_datums);
}

static int64
insert_compound_unchecked(const char *compound_type,
                          int num_roles,
                          const char *const *role_names,
                          const int64 *role_ids,
                          const char *const *role_values)
{
    char   *identity_literal;
    int64   compound_id;

    identity_literal = build_compound_identity_literal(compound_type,
                                                       num_roles,
                                                       role_names,
                                                       role_values);
    compound_id = get_or_create_identity(identity_literal);
    pfree(identity_literal);

    if (num_roles == 1)
    {
        int64 role_id = (role_ids != NULL && role_ids[0] >= 0)
            ? role_ids[0]
            : get_or_create_identity(role_names[0]);
        int64 value_id = get_or_create_identity(role_values[0]);

        insert_edge_internal(compound_id, role_id, value_id);
    }
    else if (num_roles > 1)
    {
        int64 subject_ids[MAX_COMPOUND_ARGS];
        int64 predicate_ids[MAX_COMPOUND_ARGS];
        int64 object_ids[MAX_COMPOUND_ARGS];
        int   i;

        for (i = 0; i < num_roles; i++)
        {
            subject_ids[i] = compound_id;
            predicate_ids[i] = (role_ids != NULL && role_ids[i] >= 0)
                ? role_ids[i]
                : get_or_create_identity(role_names[i]);
            object_ids[i] = get_or_create_identity(role_values[i]);
        }

        insert_edges_batch(subject_ids, predicate_ids, object_ids, num_roles);
    }

    return compound_id;
}

void
define_predicate(const char *name,
                 const char *subject_cardinality,
                 const char *subject_type,
                 const char *object_cardinality,
                 const char *object_type)
{
    const char *subject_role_names[1] = {"liquid/subject_meta"};
    const char *object_role_names[1]  = {"liquid/object_meta"};
    const char *subject_role_values[1];
    const char *object_role_values[1];
    int64       subject_role_ids[1];
    int64       object_role_ids[1];
    int64       predicate_id;
    int64       subject_meta_pred_id;
    int64       object_meta_pred_id;
    int64       subject_meta_id;
    int64       object_meta_id;
    int64       type_pred_id;
    int64       card_pred_id;
    int64       subject_type_id;
    int64       subject_card_id;
    int64       object_type_id;
    int64       object_card_id;
    int64       subject_ids[6];
    int64       predicate_ids[6];
    int64       object_ids[6];

    predicate_id = get_or_create_identity(name);
    subject_meta_pred_id = get_or_create_identity("liquid/subject_meta");
    object_meta_pred_id  = get_or_create_identity("liquid/object_meta");
    type_pred_id = get_or_create_identity("liquid/type");
    card_pred_id = get_or_create_identity("liquid/cardinality");
    subject_type_id = get_or_create_identity(subject_type);
    subject_card_id = get_or_create_identity(subject_cardinality);
    object_type_id = get_or_create_identity(object_type);
    object_card_id = get_or_create_identity(object_cardinality);
    subject_role_values[0] = name;
    object_role_values[0]  = name;
    subject_role_ids[0] = subject_meta_pred_id;
    object_role_ids[0] = object_meta_pred_id;

    subject_meta_id = insert_compound_unchecked("Smeta",
                                                1,
                                                subject_role_names,
                                                subject_role_ids,
                                                subject_role_values);
    object_meta_id  = insert_compound_unchecked("Ometa",
                                                1,
                                                object_role_names,
                                                object_role_ids,
                                                object_role_values);

    subject_ids[0] = predicate_id;
    predicate_ids[0] = subject_meta_pred_id;
    object_ids[0] = subject_meta_id;

    subject_ids[1] = predicate_id;
    predicate_ids[1] = object_meta_pred_id;
    object_ids[1] = object_meta_id;

    subject_ids[2] = subject_meta_id;
    predicate_ids[2] = type_pred_id;
    object_ids[2] = subject_type_id;

    subject_ids[3] = subject_meta_id;
    predicate_ids[3] = card_pred_id;
    object_ids[3] = subject_card_id;

    subject_ids[4] = object_meta_id;
    predicate_ids[4] = type_pred_id;
    object_ids[4] = object_type_id;

    subject_ids[5] = object_meta_id;
    predicate_ids[5] = card_pred_id;
    object_ids[5] = object_card_id;

    insert_edges_batch(subject_ids, predicate_ids, object_ids, 6);
}

void
define_compound(const char *name,
                const char *predicate_name,
                const char *object_cardinality,
                const char *object_type)
{
    int64 compound_id = get_or_create_identity(name);
    int64 predicate_id = get_or_create_identity(predicate_name);
    int64 comp_pred_id = get_or_create_identity("liquid/compound_predicate");

    define_predicate(predicate_name, "1", "liquid/node",
                     object_cardinality, object_type);
    insert_edge_internal(compound_id, comp_pred_id, predicate_id);
    record_compound_schema_role(compound_id, predicate_id, predicate_name);
}

int64
insert_compound(const char *compound_type,
                int num_roles,
                const char *const *role_names,
                const int64 *role_ids,
                const char *const *role_values)
{
    validate_compound_schema(compound_type, num_roles, role_names, role_ids);
    return insert_compound_unchecked(compound_type,
                                     num_roles,
                                     role_names,
                                     role_ids,
                                     role_values);
}
