/*
 * engine.c — Datalog evaluation engine.
 *
 * Improvements in this version:
 *   C1: build_predicate_stats → O(1) HTAB instead of O(n) linear scan per edge.
 *   C2: compute_relevant_rules already_in check → HTAB (was O(n²)).
 *   C3: run_rule computes actual num_vars from body atoms (not MAX_DATALOG_VARS).
 *   C4: evaluate_rules deduplicates pids[] before building edge cache.
 *   C5: CHECK_FOR_INTERRUPTS() in semi-naive fixpoint loop.
 *   C6: scan_edge_cache returns EdgeCacheRow* pointers, no per-row palloc.
 *   C7: SCC stratification: rules evaluated in topological order by dependency.
 */

#include "postgres.h"
#include "pg_liquid.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "miscadmin.h"       /* CHECK_FOR_INTERRUPTS */
#include "utils/guc.h"

extern PGDLLIMPORT int work_mem;

/* ================================================================
 * Binding dedup hash
 * ================================================================ */

typedef struct BindingHashKey
{
    int64   values[MAX_DATALOG_VARS];
    bool    is_bound[MAX_DATALOG_VARS];
} BindingHashKey;

typedef struct BindingHashEntry
{
    BindingHashKey  key;
    char            status;
} BindingHashEntry;

typedef struct Int64IndexEntry
{
    int64   key;
    int     index;
} Int64IndexEntry;

typedef struct IntArray
{
    int    *values;
    int     count;
    int     capacity;
} IntArray;

typedef struct Int64Array
{
    int64  *values;
    int     count;
    int     capacity;
} Int64Array;

typedef struct CompoundTypeMatchEntry
{
    int64 subject_id;
    bool  matches;
} CompoundTypeMatchEntry;

typedef struct TransitiveClosurePlan
{
    bool                valid;
    int64               predicate_id;
    LiquidCompiledRule *recursive_rule;
} TransitiveClosurePlan;

static SPIPlanPtr compound_scan_plans[MAX_COMPOUND_ARGS + 1] = {0};

static inline LiquidAtomExecState *
atom_exec_state(LiquidScanState *lss, LiquidCompiledAtom *atom)
{
    return &lss->atom_state[atom->plan_index];
}

static inline bool
atom_is_satisfied(LiquidScanState *lss, LiquidCompiledAtom *atom)
{
    return atom_exec_state(lss, atom)->satisfied;
}

static inline void
atom_set_satisfied(LiquidScanState *lss, LiquidCompiledAtom *atom, bool value)
{
    atom_exec_state(lss, atom)->satisfied = value;
}

static inline bool
atom_is_idb(LiquidScanState *lss, LiquidCompiledAtom *atom)
{
    return atom_exec_state(lss, atom)->is_idb;
}

static inline bool
atom_uses_delta_scan(LiquidScanState *lss, LiquidCompiledAtom *atom)
{
    return atom_exec_state(lss, atom)->use_delta_scan;
}

static inline void
atom_set_use_delta_scan(LiquidScanState *lss, LiquidCompiledAtom *atom, bool value)
{
    atom_exec_state(lss, atom)->use_delta_scan = value;
}

static void
initialize_atom_exec_states(LiquidScanState *lss)
{
    int atom_index;

    if (lss->plan == NULL || lss->atom_state == NULL)
        return;

    memset(lss->atom_state,
           0,
           sizeof(LiquidAtomExecState) * Max(1, lss->plan->atom_count));
    for (atom_index = 0; atom_index < lss->plan->atom_count; atom_index++)
        lss->atom_state[atom_index].is_idb = lss->plan->atoms[atom_index].statically_is_idb;
}

static HTAB *
create_binding_htab(MemoryContext mcxt)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(BindingHashKey);
    ctl.entrysize = sizeof(BindingHashEntry);
    ctl.hcxt      = mcxt;
    return hash_create("Bindings", 256, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * binding_dedup_add — returns true if this binding is new.
 *
 * IMPORTANT: only hashes the first `num_vars` slots, not the full
 * MAX_DATALOG_VARS array.  This prevents false dedup when two bindings
 * only differ in unused (zero-initialized) slots above num_vars.
 */
static bool
binding_dedup_add(HTAB *htab, LiquidBinding *b, int num_vars)
{
    BindingHashKey  key;
    bool            found;

    memset(&key, 0, sizeof(key));
    if (num_vars > MAX_DATALOG_VARS)
        num_vars = MAX_DATALOG_VARS;
    memcpy(key.values,   b->values,   num_vars * sizeof(int64));
    memcpy(key.is_bound, b->is_bound, num_vars * sizeof(bool));

    hash_search(htab, &key, HASH_ENTER, &found);
    return !found;
}

static HTAB *
create_int64_index(MemoryContext mcxt, const char *name, long initial_size)
{
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(Int64IndexEntry);
    ctl.hcxt = mcxt;
    return hash_create(name, initial_size, &ctl,
                       HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
int_array_add(IntArray *array, MemoryContext mcxt, int value)
{
    if (array->capacity == 0)
    {
        array->capacity = 8;
        array->values = MemoryContextAlloc(mcxt, array->capacity * sizeof(int));
    }
    else if (array->count >= array->capacity)
    {
        array->capacity *= 2;
        array->values = repalloc(array->values, array->capacity * sizeof(int));
    }

    array->values[array->count++] = value;
}

static void
int64_array_add(Int64Array *array, MemoryContext mcxt, int64 value)
{
    if (array->capacity == 0)
    {
        array->capacity = 8;
        array->values = MemoryContextAlloc(mcxt, array->capacity * sizeof(int64));
    }
    else if (array->count >= array->capacity)
    {
        array->capacity *= 2;
        array->values = repalloc(array->values, array->capacity * sizeof(int64));
    }

    array->values[array->count++] = value;
}

static int
int64_index_get_or_add(HTAB *htab,
                       Int64Array *values,
                       MemoryContext mcxt,
                       int64 key)
{
    Int64IndexEntry *entry;
    bool             found;

    entry = hash_search(htab, &key, HASH_ENTER, &found);
    if (!found)
    {
        entry->key = key;
        entry->index = values->count;
        int64_array_add(values, mcxt, key);
    }

    return entry->index;
}

/* ================================================================
 * Edge Cache: Indexed in-memory store with O(1) hash lookups.
 *
 * C6: scan_edge_cache returns List of (EdgeCacheRow *) pointers
 * into the rows[] array — zero heap allocation per result row.
 * ================================================================ */

static HTAB *
create_edge_index(MemoryContext mcxt, const char *name, int init_size)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(EdgeIndexKey);
    ctl.entrysize = sizeof(EdgeIndexEntry);
    ctl.hcxt      = mcxt;
    return hash_create(name, init_size, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
edge_index_add(HTAB *idx, MemoryContext mcxt, int64 k1, int64 k2, int row_idx)
{
    EdgeIndexKey    key;
    EdgeIndexEntry *entry;
    bool            found;

    key.k1 = k1;
    key.k2 = k2;
    entry  = hash_search(idx, &key, HASH_ENTER, &found);
    if (!found)
    {
        entry->capacity    = 8;
        entry->count       = 0;
        entry->row_indices = MemoryContextAlloc(mcxt, entry->capacity * sizeof(int));
    }
    if (entry->count >= entry->capacity)
    {
        entry->capacity   *= 2;
        entry->row_indices = repalloc(entry->row_indices,
                                      entry->capacity * sizeof(int));
    }
    entry->row_indices[entry->count++] = row_idx;
}

/*
 * build_edge_cache_selective — loads edges for the given predicate set.
 * If predicate_ids is NULL or count 0, loads all visible edges.
 *
 * C4: caller is responsible for dedup'ing pids before calling.
 */
static EdgeCache *
build_edge_cache_selective(LiquidScanState *lss,
                           MemoryContext mcxt,
                           int64 *predicate_ids, int num_preds)
{
    EdgeCache  *cache;
    bool        enforce_edge_visibility;
    MemoryContext old = MemoryContextSwitchTo(mcxt);
    StringInfoData  q;

    cache          = palloc0(sizeof(EdgeCache));
    cache->idx_sp  = create_edge_index(mcxt, "EdgeIdx_SP", 256);
    cache->idx_po  = create_edge_index(mcxt, "EdgeIdx_PO", 256);
    cache->idx_so  = create_edge_index(mcxt, "EdgeIdx_SO", 256);
    cache->idx_p   = create_edge_index(mcxt, "EdgeIdx_P",  64);
    cache->idx_s   = create_edge_index(mcxt, "EdgeIdx_S",  256);
    cache->idx_o   = create_edge_index(mcxt, "EdgeIdx_O",  256);

    liquid_ensure_policy_context(lss);
    enforce_edge_visibility = lss->policy_context.enabled ||
                              lss->policy_context.principal_forced;

    initStringInfo(&q);
    appendStringInfo(&q,
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false");

    if (num_preds > 0)
    {
        int i;
        appendStringInfo(&q, " AND predicate_id IN (");
        for (i = 0; i < num_preds; i++)
        {
            if (i > 0) appendStringInfoChar(&q, ',');
            appendStringInfo(&q, "%ld", (long) predicate_ids[i]);
        }
        appendStringInfoChar(&q, ')');
    }

    if (SPI_execute(q.data, false, 0) == SPI_OK_SELECT)
    {
        int r;
        int source_count = (int) SPI_processed;
        cache->count    = 0;
        cache->capacity = source_count + 64;
        cache->rows     = palloc(cache->capacity * sizeof(EdgeCacheRow));

        for (r = 0; r < source_count; r++)
        {
            bool isnull;
            int64 s = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[r],
                                                   SPI_tuptable->tupdesc, 1, &isnull));
            int64 p = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[r],
                                                   SPI_tuptable->tupdesc, 2, &isnull));
            int64 o = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[r],
                                                   SPI_tuptable->tupdesc, 3, &isnull));
            if (enforce_edge_visibility && !liquid_edge_visible(lss, s, p, o))
                continue;
            cache->rows[cache->count].s = s;
            cache->rows[cache->count].p = p;
            cache->rows[cache->count].o = o;
            edge_index_add(cache->idx_sp, mcxt, s, p, cache->count);
            edge_index_add(cache->idx_po, mcxt, p, o, cache->count);
            edge_index_add(cache->idx_so, mcxt, s, o, cache->count);
            edge_index_add(cache->idx_p,  mcxt, p, -1, cache->count);
            edge_index_add(cache->idx_s,  mcxt, s, -1, cache->count);
            edge_index_add(cache->idx_o,  mcxt, o, -1, cache->count);
            cache->count++;
        }
    }
    pfree(q.data);

    MemoryContextSwitchTo(old);
    return cache;
}

static bool
should_materialize_edge_cache(int64 *predicate_ids, int num_preds)
{
    StringInfoData  q;
    Size            budget_bytes;
    double          estimated_bytes_per_row;
    int64           row_count = 0;

    if (pg_liquid_edge_cache_budget_kb > 0)
        budget_bytes = (Size) pg_liquid_edge_cache_budget_kb * (Size) 1024;
    else
        budget_bytes = Max((Size) 262144, (Size) work_mem * (Size) 1024 / 2);

    estimated_bytes_per_row = (double) pg_liquid_edge_cache_row_estimate_bytes;

    initStringInfo(&q);
    appendStringInfoString(&q,
                           "SELECT count(*) "
                           "FROM liquid.edges WHERE is_deleted = false");

    if (num_preds > 0)
    {
        int i;

        appendStringInfoString(&q, " AND predicate_id IN (");
        for (i = 0; i < num_preds; i++)
        {
            if (i > 0)
                appendStringInfoChar(&q, ',');
            appendStringInfo(&q, "%ld", (long) predicate_ids[i]);
        }
        appendStringInfoChar(&q, ')');
    }

    if (SPI_execute(q.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;

        row_count = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                SPI_tuptable->tupdesc, 1, &isnull));
    }

    pfree(q.data);

    return ((double) row_count * estimated_bytes_per_row) <= (double) budget_bytes;
}

typedef void (*EdgeCacheRowVisitor)(EdgeCacheRow *row, void *ctx);

static int
edge_index_row_count(HTAB *idx, int64 k1, int64 k2)
{
    EdgeIndexKey    key;
    EdgeIndexEntry *entry;

    if (idx == NULL)
        return 0;

    key.k1 = k1;
    key.k2 = k2;
    entry  = hash_search(idx, &key, HASH_FIND, NULL);
    return entry ? entry->count : 0;
}

static EdgeIndexEntry *
edge_index_find_entry(HTAB *idx, int64 k1, int64 k2)
{
    EdgeIndexKey    key;

    if (idx == NULL)
        return NULL;

    key.k1 = k1;
    key.k2 = k2;
    return hash_search(idx, &key, HASH_FIND, NULL);
}

static void
scan_edge_cache_visit(EdgeCache *cache,
                      int64 s_id,
                      int64 p_id,
                      int64 o_id,
                      EdgeCacheRowVisitor visitor,
                      void *ctx)
{
    EdgeIndexEntry *best_entry = NULL;
    int             bound_mask = 0;
    int             i;

    if (s_id != -1)
        bound_mask |= 1;
    if (p_id != -1)
        bound_mask |= 2;
    if (o_id != -1)
        bound_mask |= 4;

    /*
     * Use the most selective index shape implied by bound terms.
     * This avoids multiple hash lookups per scan on hot recursive paths.
     */
    switch (bound_mask)
    {
        case 7:
        case 3:
            best_entry = edge_index_find_entry(cache->idx_sp, s_id, p_id);
            break;
        case 6:
            best_entry = edge_index_find_entry(cache->idx_po, p_id, o_id);
            break;
        case 5:
            best_entry = edge_index_find_entry(cache->idx_so, s_id, o_id);
            break;
        case 2:
            best_entry = edge_index_find_entry(cache->idx_p, p_id, -1);
            break;
        case 1:
            best_entry = edge_index_find_entry(cache->idx_s, s_id, -1);
            break;
        case 4:
            best_entry = edge_index_find_entry(cache->idx_o, o_id, -1);
            break;
        default:
            break;
    }

    if (best_entry == NULL && bound_mask != 0)
        return;

    if (best_entry != NULL)
    {
        switch (bound_mask)
        {
            case 3:
            case 6:
            case 5:
            case 2:
            case 1:
            case 4:
                for (i = 0; i < best_entry->count; i++)
                    visitor(&cache->rows[best_entry->row_indices[i]], ctx);
                return;

            case 7:
                for (i = 0; i < best_entry->count; i++)
                {
                    EdgeCacheRow *e = &cache->rows[best_entry->row_indices[i]];

                    if (e->o == o_id)
                        visitor(e, ctx);
                }
                return;

            default:
                break;
        }
    }

    for (i = 0; i < cache->count; i++)
    {
        EdgeCacheRow *e = &cache->rows[i];

        if (s_id != -1 && e->s != s_id)
            continue;
        if (p_id != -1 && e->p != p_id)
            continue;
        if (o_id != -1 && e->o != o_id)
            continue;
        visitor(e, ctx);
    }
}

static bool
subject_matches_compound_type(int64 subject_id, const char *compound_type)
{
    const char *literal;
    int         prefix_len;

    literal = lookup_identity_literal_ref(subject_id);
    if (literal == NULL)
        return false;

    prefix_len = strlen(compound_type);
    return strncmp(literal, compound_type, prefix_len) == 0 &&
           literal[prefix_len] == '@' &&
           literal[prefix_len + 1] == '(';
}

static bool
subject_matches_compound_type_cached(HTAB *cache,
                                     int64 subject_id,
                                     const char *compound_type)
{
    CompoundTypeMatchEntry *entry;
    bool                    found;

    if (cache == NULL)
        return subject_matches_compound_type(subject_id, compound_type);

    entry = hash_search(cache, &subject_id, HASH_ENTER, &found);
    if (!found)
    {
        entry->subject_id = subject_id;
        entry->matches = subject_matches_compound_type(subject_id, compound_type);
    }

    return entry->matches;
}

/* ================================================================
 * Derived fact dedup
 * ================================================================ */

typedef struct DerivedFactPredicateEntry
{
    int64       predicate_id;
    LiquidFact **facts;
    int         count;
    int         capacity;
} DerivedFactPredicateEntry;

typedef struct DerivedFactLookupKey
{
    int64   predicate_id;
    int     position;
    int64   value;
} DerivedFactLookupKey;

typedef struct DerivedFactLookupEntry
{
    DerivedFactLookupKey key;
    LiquidFact         **facts;
    int                 count;
    int                 capacity;
} DerivedFactLookupEntry;

typedef struct DerivedFactPairLookupKey
{
    int64   predicate_id;
    int     position_a;
    int     position_b;
    int64   value_a;
    int64   value_b;
} DerivedFactPairLookupKey;

typedef struct DerivedFactPairLookupEntry
{
    DerivedFactPairLookupKey key;
    LiquidFact             **facts;
    int                     count;
    int                     capacity;
} DerivedFactPairLookupEntry;

struct DerivedFactStore
{
    MemoryContext storage_context;
    HTAB *by_predicate;
    HTAB *by_term_value;
    HTAB *by_term_pair;
    int   fact_count;
};

typedef struct DerivedFactCandidateSet
{
    LiquidFact **facts;
    int         count;
} DerivedFactCandidateSet;

static void
derived_fact_entry_add(LiquidFact ***facts, int *count, int *capacity,
                       MemoryContext mcxt, LiquidFact *fact)
{
    if (*capacity == 0)
    {
        *capacity = 8;
        *facts = MemoryContextAlloc(mcxt, *capacity * sizeof(LiquidFact *));
    }
    else if (*count >= *capacity)
    {
        *capacity *= 2;
        *facts = repalloc(*facts, *capacity * sizeof(LiquidFact *));
    }

    (*facts)[(*count)++] = fact;
}

static DerivedFactStore *
create_derived_fact_store(MemoryContext mcxt, bool enable_pair_index)
{
    DerivedFactStore *store;
    HASHCTL           ctl;
    MemoryContext     old = MemoryContextSwitchTo(mcxt);

    store = palloc0(sizeof(DerivedFactStore));

    memset(&ctl, 0, sizeof(ctl));
    store->storage_context = mcxt;
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(DerivedFactPredicateEntry);
    ctl.hcxt = mcxt;
    store->by_predicate = hash_create("DerivedFactByPredicate", 64, &ctl,
                                      HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(DerivedFactLookupKey);
    ctl.entrysize = sizeof(DerivedFactLookupEntry);
    ctl.hcxt = mcxt;
    store->by_term_value = hash_create("DerivedFactByTermValue", 256, &ctl,
                                       HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    if (enable_pair_index)
    {
        memset(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(DerivedFactPairLookupKey);
        ctl.entrysize = sizeof(DerivedFactPairLookupEntry);
        ctl.hcxt = mcxt;
        store->by_term_pair = hash_create("DerivedFactByTermPair", 256, &ctl,
                                          HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    }
    store->fact_count = 0;

    MemoryContextSwitchTo(old);
    return store;
}

static void
derived_fact_store_add(DerivedFactStore *store, MemoryContext mcxt, LiquidFact *fact)
{
    DerivedFactPredicateEntry *predicate_entry;
    bool                      found;
    int                       pos;

    predicate_entry = hash_search(store->by_predicate,
                                  &fact->predicate_id,
                                  HASH_ENTER,
                                  &found);
    if (!found)
    {
        predicate_entry->predicate_id = fact->predicate_id;
        predicate_entry->facts = NULL;
        predicate_entry->count = 0;
        predicate_entry->capacity = 0;
    }

    derived_fact_entry_add(&predicate_entry->facts,
                           &predicate_entry->count,
                           &predicate_entry->capacity,
                           mcxt,
                           fact);

    for (pos = 0; pos < fact->num_terms; pos++)
    {
        DerivedFactLookupKey    key;
        DerivedFactLookupEntry *lookup_entry;

        memset(&key, 0, sizeof(key));
        key.predicate_id = fact->predicate_id;
        key.position = pos;
        key.value = fact->terms[pos];

        lookup_entry = hash_search(store->by_term_value, &key, HASH_ENTER, &found);
        if (!found)
        {
            lookup_entry->key = key;
            lookup_entry->facts = NULL;
            lookup_entry->count = 0;
            lookup_entry->capacity = 0;
        }

        derived_fact_entry_add(&lookup_entry->facts,
                               &lookup_entry->count,
                               &lookup_entry->capacity,
                               mcxt,
                               fact);
    }

    if (store->by_term_pair != NULL && fact->num_terms >= 2)
    {
        int pos_a;

        for (pos_a = 0; pos_a < fact->num_terms - 1; pos_a++)
        {
            int pos_b;

            for (pos_b = pos_a + 1; pos_b < fact->num_terms; pos_b++)
            {
                DerivedFactPairLookupKey    pair_key;
                DerivedFactPairLookupEntry *pair_entry;

                memset(&pair_key, 0, sizeof(pair_key));
                pair_key.predicate_id = fact->predicate_id;
                pair_key.position_a = pos_a;
                pair_key.position_b = pos_b;
                pair_key.value_a = fact->terms[pos_a];
                pair_key.value_b = fact->terms[pos_b];

                pair_entry = hash_search(store->by_term_pair, &pair_key, HASH_ENTER, &found);
                if (!found)
                {
                    pair_entry->key = pair_key;
                    pair_entry->facts = NULL;
                    pair_entry->count = 0;
                    pair_entry->capacity = 0;
                }

                derived_fact_entry_add(&pair_entry->facts,
                                       &pair_entry->count,
                                       &pair_entry->capacity,
                                       mcxt,
                                       fact);
            }
        }
    }

    store->fact_count++;
}

static int
derived_fact_store_predicate_count(DerivedFactStore *store, int64 predicate_id)
{
    DerivedFactPredicateEntry *entry;

    if (store == NULL)
        return 0;

    entry = hash_search(store->by_predicate, &predicate_id, HASH_FIND, NULL);
    return entry ? entry->count : 0;
}

static DerivedFactCandidateSet
derived_fact_store_candidates(DerivedFactStore *store,
                              int64 predicate_id,
                              int num_terms,
                              int64 *scan_vals)
{
    DerivedFactCandidateSet    candidates = {0};
    DerivedFactPredicateEntry *predicate_entry;
    int                        bound_positions[MAX_DATALOG_VARS];
    int                        bound_count = 0;
    int                        pos;

    if (store == NULL)
        return candidates;

    predicate_entry = hash_search(store->by_predicate, &predicate_id, HASH_FIND, NULL);
    if (predicate_entry == NULL)
        return candidates;

    candidates.facts = predicate_entry->facts;
    candidates.count = predicate_entry->count;

    for (pos = 0; pos < num_terms; pos++)
    {
        DerivedFactLookupKey    key;
        DerivedFactLookupEntry *lookup_entry;

        if (scan_vals[pos] == -1)
            continue;

        if (bound_count < MAX_DATALOG_VARS)
            bound_positions[bound_count++] = pos;

        memset(&key, 0, sizeof(key));
        key.predicate_id = predicate_id;
        key.position = pos;
        key.value = scan_vals[pos];

        lookup_entry = hash_search(store->by_term_value, &key, HASH_FIND, NULL);
        if (lookup_entry == NULL)
        {
            candidates.facts = NULL;
            candidates.count = 0;
            return candidates;
        }

        if (lookup_entry->count < candidates.count)
        {
            candidates.facts = lookup_entry->facts;
            candidates.count = lookup_entry->count;
        }
    }

    if (store->by_term_pair != NULL && bound_count >= 2)
    {
        int idx_a;

        for (idx_a = 0; idx_a < bound_count - 1; idx_a++)
        {
            int idx_b;

            for (idx_b = idx_a + 1; idx_b < bound_count; idx_b++)
            {
                int                        pos_a = bound_positions[idx_a];
                int                        pos_b = bound_positions[idx_b];
                DerivedFactPairLookupKey    pair_key;
                DerivedFactPairLookupEntry *pair_entry;

                memset(&pair_key, 0, sizeof(pair_key));
                pair_key.predicate_id = predicate_id;
                pair_key.position_a = pos_a;
                pair_key.position_b = pos_b;
                pair_key.value_a = scan_vals[pos_a];
                pair_key.value_b = scan_vals[pos_b];

                pair_entry = hash_search(store->by_term_pair, &pair_key, HASH_FIND, NULL);
                if (pair_entry == NULL)
                {
                    candidates.facts = NULL;
                    candidates.count = 0;
                    return candidates;
                }

                if (pair_entry->count < candidates.count)
                {
                    candidates.facts = pair_entry->facts;
                    candidates.count = pair_entry->count;
                }
            }
        }
    }

    return candidates;
}

static HTAB *
create_derived_facts_htab(MemoryContext mcxt)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(DerivedFactKey);
    ctl.entrysize = sizeof(DerivedFactEntry);
    ctl.hcxt      = mcxt;
    return hash_create("Derived Facts", 256, &ctl,
                       HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
derived_fact_try_insert_key(HTAB *htab,
                            int64 predicate_id,
                            int num_terms,
                            const int64 *terms,
                            DerivedFactEntry **entry_out)
{
    DerivedFactKey   key;
    bool             found;
    DerivedFactEntry *entry;

    memset(&key, 0, sizeof(key));
    key.predicate_id = predicate_id;
    key.num_terms = num_terms;
    if (num_terms > 0)
        memcpy(key.terms, terms, num_terms * sizeof(int64));

    entry = hash_search(htab, &key, HASH_ENTER, &found);
    if (entry_out != NULL)
        *entry_out = entry;
    return !found;
}

/* ================================================================
 * Indexed Compound Scanning
 * ================================================================ */

typedef struct CompoundSubjectEntry
{
    int64 subject_id;
    char  status;
} CompoundSubjectEntry;

typedef void (*CompoundRowVisitor)(LiquidCompiledAtom *atom, int64 *row_values, void *ctx);

typedef struct CompoundListBuildContext
{
    List **results;
} CompoundListBuildContext;

typedef struct CompoundRoleExpandContext
{
    EdgeCache          *cache;
    LiquidCompiledAtom *atom;
    int                 role_index;
    int64               subject_id;
    int64              *scan_vals;
    int64              *row_values;
    CompoundRowVisitor  visitor;
    void               *visitor_ctx;
} CompoundRoleExpandContext;

typedef struct CompoundSeedContext
{
    EdgeCache          *cache;
    LiquidCompiledAtom *atom;
    int64              *arg_vals;
    HTAB               *subjects_seen;
    HTAB               *type_match_cache;
    CompoundRowVisitor  visitor;
    void               *visitor_ctx;
} CompoundSeedContext;

static void
append_compound_result_row(LiquidCompiledAtom *atom, int64 *row_values, void *ctx)
{
    CompoundListBuildContext *build_ctx = (CompoundListBuildContext *) ctx;
    int64 *row = palloc(sizeof(int64) * atom->num_terms);

    memcpy(row, row_values, sizeof(int64) * atom->num_terms);
    *build_ctx->results = lappend(*build_ctx->results, row);
}

static void
expand_compound_role_values_visit(EdgeCache *cache,
                                  LiquidCompiledAtom *atom,
                                  int role_index,
                                  int64 subject_id,
                                  int64 *scan_vals,
                                  int64 *row_values,
                                  CompoundRowVisitor visitor,
                                  void *visitor_ctx);

static void
visit_compound_role_row(EdgeCacheRow *edge, void *ctx)
{
    CompoundRoleExpandContext *expand_ctx = (CompoundRoleExpandContext *) ctx;

    expand_ctx->row_values[expand_ctx->role_index + 1] = edge->o;
    if (expand_ctx->role_index + 1 == expand_ctx->atom->num_named_args)
        expand_ctx->visitor(expand_ctx->atom, expand_ctx->row_values, expand_ctx->visitor_ctx);
    else
        expand_compound_role_values_visit(expand_ctx->cache,
                                          expand_ctx->atom,
                                          expand_ctx->role_index + 1,
                                          expand_ctx->subject_id,
                                          expand_ctx->scan_vals,
                                          expand_ctx->row_values,
                                          expand_ctx->visitor,
                                          expand_ctx->visitor_ctx);
}

static void
expand_compound_role_values_visit(EdgeCache *cache,
                                  LiquidCompiledAtom *atom,
                                  int role_index,
                                  int64 subject_id,
                                  int64 *scan_vals,
                                  int64 *row_values,
                                  CompoundRowVisitor visitor,
                                  void *visitor_ctx)
{
    CompoundRoleExpandContext ctx;

    ctx.cache = cache;
    ctx.atom = atom;
    ctx.role_index = role_index;
    ctx.subject_id = subject_id;
    ctx.scan_vals = scan_vals;
    ctx.row_values = row_values;
    ctx.visitor = visitor;
    ctx.visitor_ctx = visitor_ctx;

    scan_edge_cache_visit(cache,
                          subject_id,
                          atom->named_arg_ids[role_index],
                          scan_vals[role_index + 1],
                          visit_compound_role_row,
                          &ctx);
}

static void
visit_compound_seed_row(EdgeCacheRow *seed, void *ctx)
{
    CompoundSeedContext    *seed_ctx = (CompoundSeedContext *) ctx;
    CompoundSubjectEntry   *entry;
    bool                    found;
    int64                   row_values[MAX_COMPOUND_ARGS + 1];

    entry = hash_search(seed_ctx->subjects_seen, &seed->s, HASH_ENTER, &found);
    if (found)
        return;
    entry->subject_id = seed->s;

    if (!subject_matches_compound_type_cached(seed_ctx->type_match_cache,
                                              seed->s,
                                              seed_ctx->atom->name))
        return;

    row_values[0] = seed->s;
    expand_compound_role_values_visit(seed_ctx->cache,
                                      seed_ctx->atom,
                                      0,
                                      seed->s,
                                      seed_ctx->arg_vals,
                                      row_values,
                                      seed_ctx->visitor,
                                      seed_ctx->visitor_ctx);
}

static void
scan_compound_from_edge_cache_visit(EdgeCache *cache,
                                    LiquidCompiledAtom *atom,
                                    int64 *arg_vals,
                                    CompoundRowVisitor visitor,
                                    void *visitor_ctx)
{
    int             seed_index = 0;
    int             i;
    int             best_count = INT_MAX;
    HASHCTL         ctl;
    HTAB           *subjects_seen;
    HTAB           *type_match_cache;

    if (atom->num_named_args == 0)
    {
        if (arg_vals[0] != -1 && subject_matches_compound_type(arg_vals[0], atom->name))
        {
            int64 row_values[MAX_COMPOUND_ARGS + 1];

            row_values[0] = arg_vals[0];
            visitor(atom, row_values, visitor_ctx);
        }
        return;
    }

    if (arg_vals[0] != -1)
    {
        int64 row_values[MAX_COMPOUND_ARGS + 1];

        if (!subject_matches_compound_type(arg_vals[0], atom->name))
            return;

        row_values[0] = arg_vals[0];
        expand_compound_role_values_visit(cache,
                                          atom,
                                          0,
                                          arg_vals[0],
                                          arg_vals,
                                          row_values,
                                          visitor,
                                          visitor_ctx);
        return;
    }

    for (i = 0; i < atom->num_named_args; i++)
    {
        int row_count;

        if (arg_vals[i + 1] != -1)
            row_count = edge_index_row_count(cache->idx_po,
                                             atom->named_arg_ids[i],
                                             arg_vals[i + 1]);
        else
            row_count = edge_index_row_count(cache->idx_p,
                                             atom->named_arg_ids[i],
                                             -1);

        if (row_count < best_count)
        {
            best_count = row_count;
            seed_index = i;
        }
    }

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(CompoundSubjectEntry);
    ctl.hcxt = CurrentMemoryContext;
    subjects_seen = hash_create("CompoundSubjects", 64, &ctl,
                                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(CompoundTypeMatchEntry);
    ctl.hcxt = CurrentMemoryContext;
    type_match_cache = hash_create("CompoundTypeMatch", 128, &ctl,
                                   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    {
        CompoundSeedContext seed_ctx;

        seed_ctx.cache = cache;
        seed_ctx.atom = atom;
        seed_ctx.arg_vals = arg_vals;
        seed_ctx.subjects_seen = subjects_seen;
        seed_ctx.type_match_cache = type_match_cache;
        seed_ctx.visitor = visitor;
        seed_ctx.visitor_ctx = visitor_ctx;

        scan_edge_cache_visit(cache,
                              -1,
                              atom->named_arg_ids[seed_index],
                              arg_vals[seed_index + 1],
                              visit_compound_seed_row,
                              &seed_ctx);
    }

    hash_destroy(subjects_seen);
    hash_destroy(type_match_cache);
}

static List *
scan_compound_from_edge_cache(EdgeCache *cache, LiquidCompiledAtom *atom, int64 *arg_vals)
{
    List           *res = NIL;
    CompoundListBuildContext build_ctx;

    build_ctx.results = &res;
    scan_compound_from_edge_cache_visit(cache,
                                        atom,
                                        arg_vals,
                                        append_compound_result_row,
                                        &build_ctx);
    return res;
}

static bool
compound_row_edges_visible(LiquidScanState *lss, LiquidCompiledAtom *atom, int64 *row)
{
    int i;

    if (liquid_compound_visible(lss, row[0], atom->predicate_id))
        return true;

    for (i = 0; i < atom->num_named_args; i++)
    {
        if (!liquid_edge_visible(lss,
                                 row[0],
                                 atom->named_arg_ids[i],
                                 row[i + 1]))
            return false;
    }

    return atom->num_named_args > 0;
}

static bool
can_scan_compound_from_edge_cache(LiquidScanState *lss, LiquidCompiledAtom *atom)
{
    int i;

    if (atom->num_named_args == 0)
        return false;

    if (atom->predicate_id == LIQUID_UNKNOWN_ID)
        return false;

    for (i = 0; i < atom->num_named_args; i++)
    {
        if (atom->named_arg_ids[i] == LIQUID_UNKNOWN_ID)
            return false;
    }

    liquid_ensure_policy_context(lss);

    return lss->edge_cache != NULL &&
           !lss->policy_context.enabled &&
           !lss->policy_context.principal_forced &&
           !liquid_compound_granted(lss, atom->predicate_id);
}

static SPIPlanPtr
ensure_compound_scan_plan(int num_named_args)
{
    StringInfoData query;
    Oid           *argtypes;
    SPIPlanPtr     plan;
    int            nargs;
    int            i;

    if (num_named_args < 0 || num_named_args > MAX_COMPOUND_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("compound arity %d exceeds supported max %d",
                        num_named_args,
                        MAX_COMPOUND_ARGS)));

    if (compound_scan_plans[num_named_args] != NULL)
        return compound_scan_plans[num_named_args];

    nargs = 3 + (num_named_args * 2);
    argtypes = palloc(sizeof(Oid) * nargs);
    argtypes[0] = INT4OID;
    argtypes[1] = TEXTOID;
    argtypes[2] = INT8OID;
    for (i = 0; i < num_named_args; i++)
    {
        argtypes[3 + (i * 2)] = INT8OID;
        argtypes[4 + (i * 2)] = INT8OID;
    }

    initStringInfo(&query);

    if (num_named_args == 0)
    {
        appendStringInfoString(&query,
                               "SELECT cid_v.id "
                               "FROM liquid.vertices cid_v "
                               "WHERE substr(cid_v.literal, 1, $1) = $2 "
                               "AND ($3 < 0 OR cid_v.id = $3)");
    }
    else
    {
        appendStringInfoString(&query, "SELECT e0.subject_id, e0.object_id");
        for (i = 1; i < num_named_args; i++)
            appendStringInfo(&query, ", e%d.object_id", i);
        appendStringInfoString(&query,
                               " FROM liquid.edges e0 "
                               "JOIN liquid.vertices cid_v ON e0.subject_id = cid_v.id");
        for (i = 1; i < num_named_args; i++)
            appendStringInfo(&query,
                             " JOIN liquid.edges e%d ON e0.subject_id = e%d.subject_id",
                             i, i);

        appendStringInfoString(&query,
                               " WHERE substr(cid_v.literal, 1, $1) = $2 "
                               "AND e0.is_deleted = false "
                               "AND ($3 < 0 OR e0.subject_id = $3) "
                               "AND e0.predicate_id = $4 "
                               "AND ($5 < 0 OR e0.object_id = $5)");
        for (i = 1; i < num_named_args; i++)
        {
            int predicate_param = 4 + (i * 2);
            int object_param = predicate_param + 1;

            appendStringInfo(&query,
                             " AND e%d.is_deleted = false "
                             "AND e%d.predicate_id = $%d "
                             "AND ($%d < 0 OR e%d.object_id = $%d)",
                             i,
                             i,
                             predicate_param,
                             object_param,
                             i,
                             object_param);
        }
    }

    plan = SPI_prepare(query.data, nargs, argtypes);
    if (plan == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to prepare compound scan plan")));
    SPI_keepplan(plan);
    compound_scan_plans[num_named_args] = plan;

    pfree(argtypes);
    pfree(query.data);

    return plan;
}

static List *
scan_compound(LiquidScanState *lss, LiquidCompiledAtom *atom, int64 *arg_vals)
{
    List       *res = NIL;
    Datum       args[3 + (MAX_COMPOUND_ARGS * 2)];
    char        nulls[3 + (MAX_COMPOUND_ARGS * 2)];
    SPIPlanPtr  plan;
    bool        isnull;
    int         i;
    int         row_index;
    char       *type_prefix;
    int         type_prefix_len;

    if (can_scan_compound_from_edge_cache(lss, atom))
        return scan_compound_from_edge_cache(lss->edge_cache, atom, arg_vals);

    type_prefix = psprintf("%s@(", atom->name);
    type_prefix_len = strlen(type_prefix);

    memset(nulls, ' ', sizeof(nulls));
    args[0] = Int32GetDatum(type_prefix_len);
    args[1] = CStringGetTextDatum(type_prefix);
    args[2] = Int64GetDatum(arg_vals[0]);
    for (i = 0; i < atom->num_named_args; i++)
    {
        args[3 + (i * 2)] = Int64GetDatum(atom->named_arg_ids[i]);
        args[4 + (i * 2)] = Int64GetDatum(arg_vals[i + 1]);
    }

    plan = ensure_compound_scan_plan(atom->num_named_args);

    if (SPI_execute_plan(plan, args, nulls, false, 0) == SPI_OK_SELECT)
    {
        for (row_index = 0; row_index < (int) SPI_processed; row_index++)
        {
            int64 *row = palloc(sizeof(int64) * atom->num_terms);

            for (i = 0; i < atom->num_terms; i++)
            {
                row[i] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                     SPI_tuptable->tupdesc,
                                                     i + 1, &isnull));
            }

            if (compound_row_edges_visible(lss, atom, row))
                res = lappend(res, row);
            else
                pfree(row);
        }
    }

    pfree(type_prefix);
    return res;
}

static EdgeCache *
build_edge_cache_for_plan(LiquidScanState *lss,
                          MemoryContext mcxt,
                          LiquidCompiledAtom **constraints,
                          int constraint_count,
                          LiquidCompiledRule **rules,
                          int rule_count)
{
    int64      pids[256];
    int        npids = 0;
    bool       load_all = false;
    HASHCTL    ctl;
    HTAB      *pid_htab;
    int        constraint_index;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(int64);
    ctl.entrysize = sizeof(int64) + 1;
    ctl.hcxt      = mcxt;
    pid_htab = hash_create("PidDedup", 64, &ctl,
                           HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

#define ADD_PID(pid_value) do { \
    int64 _pid = (pid_value); \
    if (_pid > 0) { \
        bool _found; \
        hash_search(pid_htab, &_pid, HASH_ENTER, &_found); \
        if (!_found) { \
            if (npids < 256) \
                pids[npids++] = _pid; \
            else \
                load_all = true; \
        } \
    } \
} while (0)

    for (constraint_index = 0; constraint_index < constraint_count; constraint_index++)
    {
        LiquidCompiledAtom *atom = constraints[constraint_index];
        int                 i;

        if (atom->type == LIQUID_ATOM_EDGE)
        {
            if (atom->num_terms < 2 || atom->terms[1].is_var)
            {
                load_all = true;
                break;
            }
            ADD_PID(atom->terms[1].id);
        }
        else if (atom->type == LIQUID_ATOM_COMPOUND)
        {
            for (i = 0; i < atom->num_named_args; i++)
            {
                if (atom->named_arg_ids[i] == LIQUID_UNKNOWN_ID)
                    continue;
                ADD_PID(atom->named_arg_ids[i]);
            }
        }

        if (load_all)
            break;
    }

    if (!load_all)
    {
        int rule_index;

        for (rule_index = 0; rule_index < rule_count; rule_index++)
        {
            LiquidCompiledRule *rule = rules[rule_index];
            int                 body_index;

            for (body_index = 0; body_index < rule->body_count; body_index++)
            {
                LiquidCompiledAtom *atom = rule->body_atoms[body_index];
                int                 i;

                if (atom->type == LIQUID_ATOM_EDGE)
                {
                    if (atom->num_terms < 2 || atom->terms[1].is_var)
                    {
                        load_all = true;
                        break;
                    }
                    ADD_PID(atom->terms[1].id);
                }
                else if (atom->type == LIQUID_ATOM_COMPOUND)
                {
                    for (i = 0; i < atom->num_named_args; i++)
                    {
                        if (atom->named_arg_ids[i] == LIQUID_UNKNOWN_ID)
                            continue;
                        ADD_PID(atom->named_arg_ids[i]);
                    }
                }
            }

            if (load_all)
                break;
        }
    }

#undef ADD_PID

    hash_destroy(pid_htab);

    if (!should_materialize_edge_cache(load_all ? NULL : pids, load_all ? 0 : npids))
        return NULL;

    return build_edge_cache_selective(lss,
                                      mcxt,
                                      load_all || npids == 0 ? NULL : pids,
                                      load_all ? 0 : npids);
}

/* ================================================================
 * Adaptive atom cost estimator
 * ================================================================ */

static double
estimate_atom_cost(LiquidScanState *lss, LiquidCompiledAtom *atom,
                   LiquidBinding *sample_binding)
{
    int     bound_count = 0;
    int     i;
    double  selectivity;
    int     frontier_size;

    for (i = 0; i < atom->num_terms; i++)
    {
        if (!atom->terms[i].is_var)
            bound_count++;
        else if (sample_binding && sample_binding->is_bound[atom->terms[i].var_idx])
            bound_count++;
    }

    if (atom->type == LIQUID_ATOM_EDGE && lss->edge_cache && atom->num_terms == 3)
    {
        int64 p_id = -1;
        int64 s_id = -1;
        int64 o_id = -1;
        int   bound_mask = 0;
        int    i2;

        for (i2 = 0; i2 < atom->num_terms; i2++)
        {
            if (!atom->terms[i2].is_var)
            {
                if (i2 == 0) s_id = atom->terms[i2].id;
                if (i2 == 1) p_id = atom->terms[i2].id;
                if (i2 == 2) o_id = atom->terms[i2].id;
            }
            else if (sample_binding && sample_binding->is_bound[atom->terms[i2].var_idx])
            {
                if (i2 == 0) s_id = sample_binding->values[atom->terms[i2].var_idx];
                if (i2 == 1) p_id = sample_binding->values[atom->terms[i2].var_idx];
                if (i2 == 2) o_id = sample_binding->values[atom->terms[i2].var_idx];
            }
        }
        if (s_id >= 0)
            bound_mask |= 1;
        if (p_id >= 0)
            bound_mask |= 2;
        if (o_id >= 0)
            bound_mask |= 4;

        switch (bound_mask)
        {
            case 7:
            case 3:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_sp,
                                                                   s_id, p_id));
                break;
            case 6:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_po,
                                                                   p_id, o_id));
                break;
            case 5:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_so,
                                                                   s_id, o_id));
                break;
            case 2:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_p,
                                                                   p_id, -1));
                break;
            case 1:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_s,
                                                                   s_id, -1));
                break;
            case 4:
                selectivity = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_o,
                                                                   o_id, -1));
                break;
            default:
                selectivity = (double) Max(1, lss->edge_cache->count);
                break;
        }
    }
    else if (atom->type == LIQUID_ATOM_COMPOUND && lss->edge_cache && atom->num_named_args > 0)
    {
        int64 cid_id = -1;
        int   i2;

        if (!atom->terms[0].is_var)
            cid_id = atom->terms[0].id;
        else if (sample_binding && sample_binding->is_bound[atom->terms[0].var_idx])
            cid_id = sample_binding->values[atom->terms[0].var_idx];

        if (cid_id >= 0)
        {
            selectivity = 1.0;

            for (i2 = 0; i2 < atom->num_named_args; i2++)
            {
                int64 role_value = -1;
                int   role_count;

                if (!atom->terms[i2 + 1].is_var)
                    role_value = atom->terms[i2 + 1].id;
                else if (sample_binding && sample_binding->is_bound[atom->terms[i2 + 1].var_idx])
                    role_value = sample_binding->values[atom->terms[i2 + 1].var_idx];

                role_count = edge_index_row_count(lss->edge_cache->idx_sp,
                                                  cid_id,
                                                  atom->named_arg_ids[i2]);
                if (role_value >= 0)
                    role_count = Min(role_count,
                                     edge_index_row_count(lss->edge_cache->idx_po,
                                                          atom->named_arg_ids[i2],
                                                          role_value));
                selectivity *= (double) Max(1, role_count);
            }
        }
        else
        {
            selectivity = 1e9;

            for (i2 = 0; i2 < atom->num_named_args; i2++)
            {
                int64 role_value = -1;
                double role_cost;

                if (!atom->terms[i2 + 1].is_var)
                    role_value = atom->terms[i2 + 1].id;
                else if (sample_binding && sample_binding->is_bound[atom->terms[i2 + 1].var_idx])
                    role_value = sample_binding->values[atom->terms[i2 + 1].var_idx];

                if (role_value >= 0)
                    role_cost = (double) Max(1, edge_index_row_count(lss->edge_cache->idx_po,
                                                                     atom->named_arg_ids[i2],
                                                                     role_value));
                else
                    role_cost = (double) Max(1,
                                             edge_index_row_count(lss->edge_cache->idx_p,
                                                                  atom->named_arg_ids[i2],
                                                                  -1));

                if (role_cost < selectivity)
                    selectivity = role_cost;
            }
        }
    }
    else if (atom_is_idb(lss, atom))
    {
        int64 scan_vals[MAX_DATALOG_VARS];
        DerivedFactStore *store;
        DerivedFactCandidateSet candidates;
        int i2;

        for (i2 = 0; i2 < atom->num_terms && i2 < MAX_DATALOG_VARS; i2++)
        {
            scan_vals[i2] = -1;
            if (!atom->terms[i2].is_var)
                scan_vals[i2] = atom->terms[i2].id;
            else if (sample_binding && sample_binding->is_bound[atom->terms[i2].var_idx])
                scan_vals[i2] = sample_binding->values[atom->terms[i2].var_idx];
        }

        store = (atom_uses_delta_scan(lss, atom) && lss->delta_fact_store != NULL)
            ? lss->delta_fact_store
            : lss->derived_fact_store;
        candidates = derived_fact_store_candidates(store,
                                                   atom->predicate_id,
                                                   atom->num_terms,
                                                   scan_vals);
        selectivity = (candidates.count > 0)
            ? (double) candidates.count
            : (double) Max(1, derived_fact_store_predicate_count(store, atom->predicate_id));
    }
    else
        selectivity = 500.0;

    /*
     * Frontier-size multiplier: when the current frontier is large, an
     * expensive atom becomes dramatically more costly to expand (it will
     * be evaluated once per frontier binding).  Scale accordingly so the
     * planner picks cheaper atoms first when the frontier grows.
     */
    frontier_size = lss->current_frontier_size;
    if (frontier_size > 1)
        selectivity *= (double) frontier_size;

    return selectivity / (double) (bound_count + 1) +
           (atom_is_idb(lss, atom) ? 0.01 : 0.0);
}

/* ================================================================
 * Solver Step with Binding Dedup
 *
 * C6: EdgeCacheRow* result lists don't allocate per-row — direct pointers.
 * ================================================================ */

static void
try_append_binding(LiquidScanState *lss,
                   LiquidCompiledAtom *atom,
                   LiquidBinding *base_binding,
                   int64 *row_vals,
                   HTAB *dedup_htab,
                   List **next_frontier,
                   int *next_frontier_size)
{
    LiquidBinding  tmp;
    LiquidBinding *nb;
    bool           match = true;
    int            k;
    MemoryContext  oldcxt;

    memcpy(&tmp, base_binding, sizeof(LiquidBinding));

    for (k = 0; k < atom->num_terms; k++)
    {
        if (atom->terms[k].is_var)
        {
            int vi = atom->terms[k].var_idx;

            if (tmp.is_bound[vi] && tmp.values[vi] != row_vals[k])
            {
                match = false;
                break;
            }
            tmp.values[vi] = row_vals[k];
            tmp.is_bound[vi] = true;
        }
        else if (atom->terms[k].id != row_vals[k])
        {
            match = false;
            break;
        }
    }

    if (match &&
        (dedup_htab == NULL || binding_dedup_add(dedup_htab, &tmp, lss->num_vars)))
    {
        oldcxt = MemoryContextSwitchTo(lss->solver_context);
        nb = MemoryContextAlloc(lss->solver_context, sizeof(LiquidBinding));
        memcpy(nb, &tmp, sizeof(LiquidBinding));
        *next_frontier = lappend(*next_frontier, nb);
        if (next_frontier_size != NULL)
            (*next_frontier_size)++;
        MemoryContextSwitchTo(oldcxt);
    }
}

typedef struct EdgeBindingContext
{
    LiquidScanState *lss;
    LiquidCompiledAtom *atom;
    LiquidBinding   *binding;
    HTAB            *dedup_htab;
    List           **next_frontier;
    int             *next_frontier_size;
} EdgeBindingContext;

static void
append_edge_binding(EdgeCacheRow *row, void *ctx)
{
    EdgeBindingContext *binding_ctx = (EdgeBindingContext *) ctx;
    int64               row_vals[3];

    row_vals[0] = row->s;
    row_vals[1] = row->p;
    row_vals[2] = row->o;

    try_append_binding(binding_ctx->lss,
                       binding_ctx->atom,
                       binding_ctx->binding,
                       row_vals,
                       binding_ctx->dedup_htab,
                       binding_ctx->next_frontier,
                       binding_ctx->next_frontier_size);
}

static void
append_scan_fact_binding(int64 s_id, int64 p_id, int64 o_id, void *ctx)
{
    EdgeBindingContext *binding_ctx = (EdgeBindingContext *) ctx;
    int64               row_vals[3];

    row_vals[0] = s_id;
    row_vals[1] = p_id;
    row_vals[2] = o_id;

    try_append_binding(binding_ctx->lss,
                       binding_ctx->atom,
                       binding_ctx->binding,
                       row_vals,
                       binding_ctx->dedup_htab,
                       binding_ctx->next_frontier,
                       binding_ctx->next_frontier_size);
}

typedef struct CompoundBindingContext
{
    LiquidScanState *lss;
    LiquidCompiledAtom *atom;
    LiquidBinding   *binding;
    HTAB            *dedup_htab;
    List           **next_frontier;
    int             *next_frontier_size;
} CompoundBindingContext;

static void
append_compound_binding(LiquidCompiledAtom *atom, int64 *row_values, void *ctx)
{
    CompoundBindingContext *binding_ctx = (CompoundBindingContext *) ctx;

    try_append_binding(binding_ctx->lss,
                       atom,
                       binding_ctx->binding,
                       row_values,
                       binding_ctx->dedup_htab,
                       binding_ctx->next_frontier,
                       binding_ctx->next_frontier_size);
}

static void
solver_step(LiquidScanState *lss)
{
    LiquidCompiledAtom *best_atom = NULL;
    double         min_cost  = 1e30;
    List          *next_frontier = NIL;
    int            next_frontier_size = 0;
    HTAB          *dedup_htab;
    MemoryContext  oldcxt;
    MemoryContext  step_context;
    int            constraint_index;
    ListCell      *lc;

    LiquidBinding *sample = (lss->current_frontier != NIL)
        ? (LiquidBinding *) linitial(lss->current_frontier)
        : NULL;

    for (constraint_index = 0; constraint_index < lss->constraint_count; constraint_index++)
    {
        LiquidCompiledAtom *atom = lss->constraints[constraint_index];
        double              cost;

        if (atom_is_satisfied(lss, atom))
            continue;
        cost = estimate_atom_cost(lss, atom, sample);
        if (cost < min_cost)
        {
            min_cost = cost;
            best_atom = atom;
        }
    }

    if (!best_atom) { lss->execution_done = true; return; }

    step_context = AllocSetContextCreate(lss->solver_context,
                                         "Liquid Step Context",
                                         ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(step_context);

    dedup_htab = create_binding_htab(step_context);

    foreach(lc, lss->current_frontier)
    {
        LiquidBinding  *b = (LiquidBinding *) lfirst(lc);
        int64           scan_vals[MAX_DATALOG_VARS];
        int             i;
        List           *scan_res = NIL;
        ListCell       *res_lc;
        bool            has_unknown = false;

        for (i = 0; i < best_atom->num_terms; i++)
        {
            if (!best_atom->terms[i].is_var &&
                best_atom->terms[i].id == LIQUID_UNKNOWN_ID)
            { has_unknown = true; break; }
        }
        if (has_unknown)
            continue;

        for (i = 0; i < best_atom->num_terms; i++)
        {
            scan_vals[i] = -1;
            if (best_atom->terms[i].is_var)
            {
                if (b->is_bound[best_atom->terms[i].var_idx])
                    scan_vals[i] = b->values[best_atom->terms[i].var_idx];
            }
            else
                scan_vals[i] = best_atom->terms[i].id;
        }

        if (best_atom->type == LIQUID_ATOM_EDGE)
        {
            if (lss->edge_cache)
            {
                EdgeBindingContext ctx;

                ctx.lss = lss;
                ctx.atom = best_atom;
                ctx.binding = b;
                ctx.dedup_htab = dedup_htab;
                ctx.next_frontier = &next_frontier;
                ctx.next_frontier_size = &next_frontier_size;

                scan_edge_cache_visit(lss->edge_cache,
                                      scan_vals[0],
                                      scan_vals[1],
                                      scan_vals[2],
                                      append_edge_binding,
                                      &ctx);
                continue;
            }
            else
            {
                EdgeBindingContext ctx;

                ctx.lss = lss;
                ctx.atom = best_atom;
                ctx.binding = b;
                ctx.dedup_htab = dedup_htab;
                ctx.next_frontier = &next_frontier;
                ctx.next_frontier_size = &next_frontier_size;

                scan_facts_visit(lss,
                                 scan_vals[0],
                                 scan_vals[1],
                                 scan_vals[2],
                                 append_scan_fact_binding,
                                 &ctx);
                continue;
            }
        }
        else if (best_atom->type == LIQUID_ATOM_COMPOUND)
        {
            if (can_scan_compound_from_edge_cache(lss, best_atom))
            {
                CompoundBindingContext ctx;

                ctx.lss = lss;
                ctx.atom = best_atom;
                ctx.binding = b;
                ctx.dedup_htab = dedup_htab;
                ctx.next_frontier = &next_frontier;
                ctx.next_frontier_size = &next_frontier_size;

                scan_compound_from_edge_cache_visit(lss->edge_cache,
                                                    best_atom,
                                                    scan_vals,
                                                    append_compound_binding,
                                                    &ctx);
                continue;
            }

            scan_res = scan_compound(lss, best_atom, scan_vals);
        }
        else if (best_atom->type == LIQUID_ATOM_PREDICATE)
        {
            if (atom_is_idb(lss, best_atom))
            {
                DerivedFactStore *store = (atom_uses_delta_scan(lss, best_atom) &&
                                           lss->delta_fact_store != NULL)
                    ? lss->delta_fact_store : lss->derived_fact_store;
                DerivedFactCandidateSet candidates = derived_fact_store_candidates(store,
                                                                                   best_atom->predicate_id,
                                                                                   best_atom->num_terms,
                                                                                   scan_vals);
                int idx;

                for (idx = 0; idx < candidates.count; idx++)
                {
                    LiquidFact *df = candidates.facts[idx];
                    bool        match = true;
                    int         k;
                    int64       row_vals[MAX_DATALOG_VARS];

                    if (df->predicate_id != best_atom->predicate_id)
                        continue;
                    if (df->num_terms != best_atom->num_terms)
                        continue;

                    for (k = 0; k < df->num_terms; k++)
                    {
                        if (scan_vals[k] != -1 && scan_vals[k] != df->terms[k])
                        {
                            match = false;
                            break;
                        }
                        row_vals[k] = df->terms[k];
                    }

                    if (match)
                        try_append_binding(lss,
                                           best_atom,
                                           b,
                                           row_vals,
                                           dedup_htab,
                                           &next_frontier,
                                           &next_frontier_size);
                }

                continue;
            }
        }

        foreach(res_lc, scan_res)
        {
            /*
             * For EdgeCacheRow pointers (EDB scan), extract s/p/o.
             * For IDB and compound results, it's int64* directly.
             */
            int64          row_vals[MAX_DATALOG_VARS];

            if (best_atom->type == LIQUID_ATOM_EDGE && lss->edge_cache)
            {
                EdgeCacheRow *ecr = (EdgeCacheRow *) lfirst(res_lc);
                row_vals[0] = ecr->s;
                row_vals[1] = ecr->p;
                row_vals[2] = ecr->o;
            }
            else
            {
                int64 *row = (int64 *) lfirst(res_lc);
                int    n   = best_atom->num_terms;
                if (n > MAX_DATALOG_VARS) n = MAX_DATALOG_VARS;
                memcpy(row_vals, row, n * sizeof(int64));
            }

            try_append_binding(lss,
                               best_atom,
                               b,
                               row_vals,
                               dedup_htab,
                               &next_frontier,
                               &next_frontier_size);
        }
        list_free(scan_res);
    }
    atom_set_satisfied(lss, best_atom, true);

    MemoryContextSwitchTo(lss->solver_context);
    lss->current_frontier = next_frontier;
    lss->current_frontier_size = next_frontier_size;

    MemoryContextDelete(step_context);
    MemoryContextSwitchTo(oldcxt);
}

/* ================================================================
 * Semi-Naive Rule Evaluation
 *
 * Evaluation order: stratified by SCC dependency (C7).
 * Fixpoint loop includes CHECK_FOR_INTERRUPTS (C5).
 * ================================================================ */

static void
reset_rule_body_state(LiquidScanState *lss, LiquidCompiledRule *rule)
{
    int body_index;

    for (body_index = 0; body_index < rule->body_count; body_index++)
    {
        LiquidCompiledAtom *atom = rule->body_atoms[body_index];

        atom_set_satisfied(lss, atom, false);
        atom_set_use_delta_scan(lss, atom, false);
    }
}

static bool
unify_atom_row(LiquidCompiledAtom *atom, int64 *row_vals, int64 *values, bool *bound)
{
    int k;

    for (k = 0; k < atom->num_terms; k++)
    {
        if (atom->terms[k].is_var)
        {
            int vi = atom->terms[k].var_idx;

            if (bound[vi] && values[vi] != row_vals[k])
                return false;

            values[vi] = row_vals[k];
            bound[vi] = true;
        }
        else if (atom->terms[k].id != row_vals[k])
        {
            return false;
        }
    }

    return true;
}

static bool
build_head_terms(LiquidCompiledRule *rule, int64 *values, bool *bound, int64 *terms_out)
{
    int k;

    for (k = 0; k < rule->head->num_terms; k++)
    {
        if (rule->head->terms[k].is_var)
        {
            int vi = rule->head->terms[k].var_idx;

            if (!bound[vi])
                return false;

            terms_out[k] = values[vi];
        }
        else
        {
            terms_out[k] = rule->head->terms[k].id;
        }

        if (terms_out[k] == LIQUID_UNKNOWN_ID)
            return false;
    }

    return true;
}

static void
emit_fact_terms(LiquidScanState *parent_lss,
                int64 predicate_id,
                int num_terms,
                int64 *terms,
                bool *changed)
{
    LiquidFact        *df;
    DerivedFactEntry  *entry = NULL;
    MemoryContext      old;

    if (!derived_fact_try_insert_key(parent_lss->derived_facts_htab,
                                     predicate_id,
                                     num_terms,
                                     terms,
                                     &entry))
        return;

    old = MemoryContextSwitchTo(parent_lss->solver_context);
    df = MemoryContextAlloc(parent_lss->solver_context, sizeof(LiquidFact));
    df->predicate_id = predicate_id;
    df->num_terms = num_terms;
    df->terms = MemoryContextAlloc(parent_lss->solver_context,
                                   num_terms * sizeof(int64));
    memcpy(df->terms, terms, num_terms * sizeof(int64));
    entry->fact = df;
    derived_fact_store_add(parent_lss->derived_fact_store,
                           parent_lss->derived_fact_store->storage_context,
                           df);
    if (parent_lss->delta_fact_store != NULL)
        derived_fact_store_add(parent_lss->delta_fact_store,
                               parent_lss->delta_fact_store->storage_context,
                               df);
    MemoryContextSwitchTo(old);
    *changed = true;
}

static void
emit_derived_fact(LiquidScanState *parent_lss,
                  LiquidCompiledRule *rule,
                  int64 *values,
                  bool *bound,
                  bool *changed)
{
    int64       terms[MAX_DATALOG_VARS];

    if (!build_head_terms(rule, values, bound, terms))
        return;

    emit_fact_terms(parent_lss,
                    rule->head->predicate_id,
                    rule->head->num_terms,
                    terms,
                    changed);
}

static void
run_transitive_closure_dense_bitset(LiquidScanState *lss,
                                    TransitiveClosurePlan *plan,
                                    MemoryContext tc_context,
                                    Int64Array *node_values,
                                    IntArray *adjacency,
                                    bool *changed)
{
    int     node_count = node_values->count;
    int     words = (node_count + 63) / 64;
    uint64 *reach;
    int     source_idx;
    int64   terms[2];

    reach = MemoryContextAllocZero(tc_context,
                                   (Size) node_count * (Size) words * sizeof(uint64));

    for (source_idx = 0; source_idx < node_count; source_idx++)
    {
        int edge_idx;

        for (edge_idx = 0; edge_idx < adjacency[source_idx].count; edge_idx++)
        {
            int    target_idx = adjacency[source_idx].values[edge_idx];
            int    word_idx = target_idx / 64;
            int    bit_idx = target_idx % 64;
            uint64 bit = ((uint64) 1) << bit_idx;

            reach[source_idx * words + word_idx] |= bit;
        }
    }

    for (source_idx = 0; source_idx < node_count; source_idx++)
    {
        int    word_idx = source_idx / 64;
        int    bit_idx = source_idx % 64;
        uint64 bit = ((uint64) 1) << bit_idx;
        int    row_idx;

        CHECK_FOR_INTERRUPTS();

        for (row_idx = 0; row_idx < node_count; row_idx++)
        {
            uint64 *row_bits;
            uint64 *src_bits;
            int     w;

            if ((reach[row_idx * words + word_idx] & bit) == 0)
                continue;

            row_bits = &reach[row_idx * words];
            src_bits = &reach[source_idx * words];
            for (w = 0; w < words; w++)
                row_bits[w] |= src_bits[w];
        }
    }

    for (source_idx = 0; source_idx < node_count; source_idx++)
    {
        uint64 *row_bits = &reach[source_idx * words];
        int     target_idx;

        CHECK_FOR_INTERRUPTS();

        for (target_idx = 0; target_idx < node_count; target_idx++)
        {
            int    word_idx = target_idx / 64;
            int    bit_idx = target_idx % 64;
            uint64 bit = ((uint64) 1) << bit_idx;

            if ((row_bits[word_idx] & bit) == 0)
                continue;

            terms[0] = node_values->values[source_idx];
            terms[1] = node_values->values[target_idx];
            emit_fact_terms(lss, plan->predicate_id, 2, terms, changed);
        }
    }
}

static void
run_transitive_closure_plan(LiquidScanState *lss,
                            TransitiveClosurePlan *plan,
                            bool *changed)
{
    int64                   scan_vals[2] = {-1, -1};
    DerivedFactCandidateSet seed_facts;
    MemoryContext           tc_context;
    HTAB                   *node_index;
    Int64Array              node_values = {0};
    IntArray                edge_sources = {0};
    IntArray                edge_targets = {0};
    IntArray               *adjacency;
    int                    *queue;
    int                    *visit_marks;
    int                     visit_token = 0;
    int                     source_idx;
    bool                    use_dense_bitset = false;
    int64                   terms[2];

    if (!plan->valid ||
        derived_fact_store_predicate_count(lss->derived_fact_store,
                                           plan->predicate_id) == 0)
        return;

    seed_facts = derived_fact_store_candidates(lss->derived_fact_store,
                                               plan->predicate_id,
                                               2,
                                               scan_vals);
    if (seed_facts.count == 0)
        return;

    tc_context = AllocSetContextCreate(lss->solver_context,
                                       "Liquid Transitive Closure",
                                       ALLOCSET_DEFAULT_SIZES);
    node_index = create_int64_index(tc_context,
                                    "TC Node Index",
                                    seed_facts.count * 2 + 1);

    for (source_idx = 0; source_idx < seed_facts.count; source_idx++)
    {
        LiquidFact *fact = seed_facts.facts[source_idx];
        int         from_idx;
        int         to_idx;

        if (fact->predicate_id != plan->predicate_id || fact->num_terms != 2)
            continue;

        from_idx = int64_index_get_or_add(node_index,
                                          &node_values,
                                          tc_context,
                                          fact->terms[0]);
        to_idx = int64_index_get_or_add(node_index,
                                        &node_values,
                                        tc_context,
                                        fact->terms[1]);

        int_array_add(&edge_sources, tc_context, from_idx);
        int_array_add(&edge_targets, tc_context, to_idx);
    }

    if (node_values.count == 0 || edge_sources.count == 0)
    {
        MemoryContextDelete(tc_context);
        return;
    }

    adjacency = MemoryContextAllocZero(tc_context,
                                       node_values.count * sizeof(IntArray));
    for (source_idx = 0; source_idx < edge_sources.count; source_idx++)
    {
        int from_idx = edge_sources.values[source_idx];
        int to_idx = edge_targets.values[source_idx];

        int_array_add(&adjacency[from_idx], tc_context, to_idx);
    }

    queue = MemoryContextAlloc(tc_context, node_values.count * sizeof(int));
    visit_marks = MemoryContextAllocZero(tc_context, node_values.count * sizeof(int));

    if (node_values.count <= 1024 &&
        edge_sources.count > node_values.count * 8)
        use_dense_bitset = true;

    if (use_dense_bitset)
    {
        run_transitive_closure_dense_bitset(lss,
                                            plan,
                                            tc_context,
                                            &node_values,
                                            adjacency,
                                            changed);
        MemoryContextDelete(tc_context);
        return;
    }

    for (source_idx = 0; source_idx < node_values.count; source_idx++)
    {
        int head = 0;
        int tail = 0;
        int edge_idx;

        CHECK_FOR_INTERRUPTS();
        if (visit_token == INT_MAX)
        {
            memset(visit_marks, 0, node_values.count * sizeof(int));
            visit_token = 1;
        }
        else
            visit_token++;

        for (edge_idx = 0; edge_idx < adjacency[source_idx].count; edge_idx++)
        {
            int next_idx = adjacency[source_idx].values[edge_idx];

            if (visit_marks[next_idx] == visit_token)
                continue;

            visit_marks[next_idx] = visit_token;
            queue[tail++] = next_idx;
            terms[0] = node_values.values[source_idx];
            terms[1] = node_values.values[next_idx];
            emit_fact_terms(lss, plan->predicate_id, 2, terms, changed);
        }

        while (head < tail)
        {
            int current_idx = queue[head++];

            for (edge_idx = 0; edge_idx < adjacency[current_idx].count; edge_idx++)
            {
                int next_idx = adjacency[current_idx].values[edge_idx];

                if (visit_marks[next_idx] == visit_token)
                    continue;

                visit_marks[next_idx] = visit_token;
                queue[tail++] = next_idx;
                terms[0] = node_values.values[source_idx];
                terms[1] = node_values.values[next_idx];
                emit_fact_terms(lss, plan->predicate_id, 2, terms, changed);
            }
        }
    }

    MemoryContextDelete(tc_context);
}

typedef struct SingleEdgeRuleContext
{
    LiquidScanState *parent_lss;
    LiquidCompiledRule *rule;
    LiquidCompiledAtom *atom;
    bool            *changed;
} SingleEdgeRuleContext;

static void
run_rule_single_edge_emit_row(SingleEdgeRuleContext *ctx,
                              int64 s_id,
                              int64 p_id,
                              int64 o_id)
{
    int64 row_vals[3];
    int64 values[MAX_DATALOG_VARS];
    bool  bound[MAX_DATALOG_VARS];

    row_vals[0] = s_id;
    row_vals[1] = p_id;
    row_vals[2] = o_id;

    memset(values, 0, ctx->rule->num_vars * sizeof(int64));
    memset(bound, 0, ctx->rule->num_vars * sizeof(bool));

    if (unify_atom_row(ctx->atom, row_vals, values, bound))
        emit_derived_fact(ctx->parent_lss, ctx->rule, values, bound, ctx->changed);
}

static void
run_rule_single_edge_scan_visit(int64 s_id, int64 p_id, int64 o_id, void *ctx_ptr)
{
    SingleEdgeRuleContext *ctx = (SingleEdgeRuleContext *) ctx_ptr;

    run_rule_single_edge_emit_row(ctx, s_id, p_id, o_id);
}

static void
run_rule_single_edge_cache_visit(EdgeCacheRow *row, void *ctx_ptr)
{
    SingleEdgeRuleContext *ctx = (SingleEdgeRuleContext *) ctx_ptr;

    run_rule_single_edge_emit_row(ctx, row->s, row->p, row->o);
}

static void
run_rule_single_edge(LiquidScanState *parent_lss,
                     LiquidCompiledRule *rule,
                     bool *changed)
{
    LiquidCompiledAtom *atom = rule->body_atoms[0];
    int64               scan_vals[3] = {-1, -1, -1};
    SingleEdgeRuleContext ctx;

    if (!atom->terms[0].is_var)
        scan_vals[0] = atom->terms[0].id;
    if (!atom->terms[1].is_var)
        scan_vals[1] = atom->terms[1].id;
    if (!atom->terms[2].is_var)
        scan_vals[2] = atom->terms[2].id;

    ctx.parent_lss = parent_lss;
    ctx.rule = rule;
    ctx.atom = atom;
    ctx.changed = changed;

    if (parent_lss->edge_cache != NULL)
        scan_edge_cache_visit(parent_lss->edge_cache,
                              scan_vals[0],
                              scan_vals[1],
                              scan_vals[2],
                              run_rule_single_edge_cache_visit,
                              &ctx);
    else
        scan_facts_visit(parent_lss,
                         scan_vals[0],
                         scan_vals[1],
                         scan_vals[2],
                         run_rule_single_edge_scan_visit,
                         &ctx);
}

typedef struct PredicateEdgeRuleContext
{
    LiquidScanState *parent_lss;
    LiquidCompiledRule *rule;
    LiquidCompiledAtom *edge_atom;
    int64           *base_values;
    bool            *base_bound;
    bool            *changed;
} PredicateEdgeRuleContext;

static void
run_rule_predicate_edge_emit_row(PredicateEdgeRuleContext *ctx,
                                 int64 s_id,
                                 int64 p_id,
                                 int64 o_id)
{
    int64 row_vals[3];
    int64 values[MAX_DATALOG_VARS];
    bool  bound[MAX_DATALOG_VARS];

    row_vals[0] = s_id;
    row_vals[1] = p_id;
    row_vals[2] = o_id;

    memcpy(values, ctx->base_values, ctx->rule->num_vars * sizeof(int64));
    memcpy(bound, ctx->base_bound, ctx->rule->num_vars * sizeof(bool));

    if (unify_atom_row(ctx->edge_atom, row_vals, values, bound))
        emit_derived_fact(ctx->parent_lss, ctx->rule, values, bound, ctx->changed);
}

static void
run_rule_predicate_edge_scan_visit(int64 s_id, int64 p_id, int64 o_id, void *ctx_ptr)
{
    PredicateEdgeRuleContext *ctx = (PredicateEdgeRuleContext *) ctx_ptr;

    run_rule_predicate_edge_emit_row(ctx, s_id, p_id, o_id);
}

static void
run_rule_predicate_edge_cache_visit(EdgeCacheRow *row, void *ctx_ptr)
{
    PredicateEdgeRuleContext *ctx = (PredicateEdgeRuleContext *) ctx_ptr;

    run_rule_predicate_edge_emit_row(ctx, row->s, row->p, row->o);
}

static void
run_rule_predicate_edge(LiquidScanState *parent_lss,
                        LiquidCompiledRule *rule,
                        int delta_atom_index,
                        DerivedFactStore *input_delta_store,
                        bool *changed)
{
    LiquidCompiledAtom *first_atom = rule->body_atoms[0];
    LiquidCompiledAtom *second_atom = rule->body_atoms[1];
    LiquidCompiledAtom *predicate_atom;
    LiquidCompiledAtom *edge_atom;
    int                 predicate_body_index;
    DerivedFactStore *predicate_store;
    DerivedFactCandidateSet predicate_candidates;
    int64       predicate_scan_vals[MAX_DATALOG_VARS];
    int         i;

    if (first_atom->type == LIQUID_ATOM_PREDICATE)
    {
        predicate_atom = first_atom;
        edge_atom = second_atom;
        predicate_body_index = 0;
    }
    else
    {
        predicate_atom = second_atom;
        edge_atom = first_atom;
        predicate_body_index = 1;
    }

    predicate_store = (delta_atom_index == predicate_body_index &&
                       input_delta_store != NULL)
        ? input_delta_store
        : parent_lss->derived_fact_store;

    for (i = 0; i < predicate_atom->num_terms; i++)
        predicate_scan_vals[i] = predicate_atom->terms[i].is_var
            ? -1 : predicate_atom->terms[i].id;

    predicate_candidates = derived_fact_store_candidates(predicate_store,
                                                         predicate_atom->predicate_id,
                                                         predicate_atom->num_terms,
                                                         predicate_scan_vals);

    for (i = 0; i < predicate_candidates.count; i++)
    {
        LiquidFact *predicate_fact = predicate_candidates.facts[i];
        int64       values[MAX_DATALOG_VARS];
        bool        bound[MAX_DATALOG_VARS];
        int64       edge_scan_vals[3];
        int         term_index;
        PredicateEdgeRuleContext ctx;

        memset(values, 0, rule->num_vars * sizeof(int64));
        memset(bound, 0, rule->num_vars * sizeof(bool));

        if (!unify_atom_row(predicate_atom, predicate_fact->terms, values, bound))
            continue;

        for (term_index = 0; term_index < edge_atom->num_terms; term_index++)
        {
            if (!edge_atom->terms[term_index].is_var)
                edge_scan_vals[term_index] = edge_atom->terms[term_index].id;
            else if (bound[edge_atom->terms[term_index].var_idx])
                edge_scan_vals[term_index] = values[edge_atom->terms[term_index].var_idx];
            else
                edge_scan_vals[term_index] = -1;
        }

        ctx.parent_lss = parent_lss;
        ctx.rule = rule;
        ctx.edge_atom = edge_atom;
        ctx.base_values = values;
        ctx.base_bound = bound;
        ctx.changed = changed;

        if (parent_lss->edge_cache != NULL)
            scan_edge_cache_visit(parent_lss->edge_cache,
                                  edge_scan_vals[0],
                                  edge_scan_vals[1],
                                  edge_scan_vals[2],
                                  run_rule_predicate_edge_cache_visit,
                                  &ctx);
        else
            scan_facts_visit(parent_lss,
                             edge_scan_vals[0],
                             edge_scan_vals[1],
                             edge_scan_vals[2],
                             run_rule_predicate_edge_scan_visit,
                             &ctx);
    }
}

static void
run_rule_two_predicates(LiquidScanState *parent_lss,
                        LiquidCompiledRule *rule,
                        int delta_atom_index,
                        DerivedFactStore *input_delta_store,
                        bool *changed)
{
    LiquidCompiledAtom *first_atom;
    LiquidCompiledAtom *second_atom;
    DerivedFactStore *first_store;
    DerivedFactStore *second_store;
    DerivedFactCandidateSet first_candidates;
    int64       first_scan_vals[MAX_DATALOG_VARS];
    int         first_body_index;
    int         second_body_index;
    int         i;

    if (delta_atom_index == 1)
    {
        first_atom = rule->body_atoms[1];
        second_atom = rule->body_atoms[0];
        first_body_index = 1;
        second_body_index = 0;
    }
    else
    {
        first_atom = rule->body_atoms[0];
        second_atom = rule->body_atoms[1];
        first_body_index = 0;
        second_body_index = 1;
    }

    first_store = (delta_atom_index == first_body_index && input_delta_store != NULL)
        ? input_delta_store
        : parent_lss->derived_fact_store;
    second_store = (delta_atom_index == second_body_index && input_delta_store != NULL)
        ? input_delta_store
        : parent_lss->derived_fact_store;

    for (i = 0; i < first_atom->num_terms; i++)
        first_scan_vals[i] = first_atom->terms[i].is_var ? -1 : first_atom->terms[i].id;

    first_candidates = derived_fact_store_candidates(first_store,
                                                     first_atom->predicate_id,
                                                     first_atom->num_terms,
                                                     first_scan_vals);

    for (i = 0; i < first_candidates.count; i++)
    {
        LiquidFact *first_fact = first_candidates.facts[i];
        int64       first_values[MAX_DATALOG_VARS];
        bool        first_bound[MAX_DATALOG_VARS];
        int64       second_scan_vals[MAX_DATALOG_VARS];
        DerivedFactCandidateSet second_candidates;
        int         j;

        memset(first_values, 0, rule->num_vars * sizeof(int64));
        memset(first_bound, 0, rule->num_vars * sizeof(bool));

        if (!unify_atom_row(first_atom, first_fact->terms, first_values, first_bound))
            continue;

        for (j = 0; j < second_atom->num_terms; j++)
        {
            if (!second_atom->terms[j].is_var)
                second_scan_vals[j] = second_atom->terms[j].id;
            else if (first_bound[second_atom->terms[j].var_idx])
                second_scan_vals[j] = first_values[second_atom->terms[j].var_idx];
            else
                second_scan_vals[j] = -1;
        }

        second_candidates = derived_fact_store_candidates(second_store,
                                                          second_atom->predicate_id,
                                                          second_atom->num_terms,
                                                          second_scan_vals);

        for (j = 0; j < second_candidates.count; j++)
        {
            LiquidFact *second_fact = second_candidates.facts[j];
            int64       values[MAX_DATALOG_VARS];
            bool        bound[MAX_DATALOG_VARS];

            memcpy(values, first_values, rule->num_vars * sizeof(int64));
            memcpy(bound, first_bound, rule->num_vars * sizeof(bool));

            if (!unify_atom_row(second_atom, second_fact->terms, values, bound))
                continue;

            emit_derived_fact(parent_lss, rule, values, bound, changed);
        }
    }
}

/*
 * run_rule — evaluate one rule using parent_lss derived_fact_store as IDB.
 *
 * C3 fix: num_vars computed from actual body atoms, not MAX_DATALOG_VARS.
 */
static void
run_rule(LiquidScanState *parent_lss,
         LiquidCompiledRule *rule,
         int delta_atom_index,
         DerivedFactStore *input_delta_store,
         bool *changed)
{
    LiquidScanState sub_lss;
    ListCell       *fc;
    int             body_index = 0;
    MemoryContext   oldcxt;

    memset(&sub_lss, 0, sizeof(LiquidScanState));

    switch (rule->execution_kind)
    {
        case LIQUID_RULE_EXEC_SINGLE_EDGE:
            run_rule_single_edge(parent_lss, rule, changed);
            return;
        case LIQUID_RULE_EXEC_PREDICATE_EDGE:
            run_rule_predicate_edge(parent_lss,
                                    rule,
                                    delta_atom_index,
                                    input_delta_store,
                                    changed);
            return;
        case LIQUID_RULE_EXEC_TWO_PREDICATES:
            run_rule_two_predicates(parent_lss,
                                    rule,
                                    delta_atom_index,
                                    input_delta_store,
                                    changed);
            return;
        case LIQUID_RULE_EXEC_GENERIC:
        default:
            break;
    }

    sub_lss.plan = parent_lss->plan;
    sub_lss.solver_context = AllocSetContextCreate(parent_lss->solver_context,
                                                   "Sub Solver",
                                                   ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(sub_lss.solver_context);
    sub_lss.atom_state = MemoryContextAllocZero(sub_lss.solver_context,
                                                sizeof(LiquidAtomExecState) *
                                                Max(1, sub_lss.plan->atom_count));
    initialize_atom_exec_states(&sub_lss);
    reset_rule_body_state(&sub_lss, rule);

    /* Mark IDB atoms in body */
    for (body_index = 0; body_index < rule->body_count; body_index++)
    {
        LiquidCompiledAtom *catom = rule->body_atoms[body_index];

        if (catom->type == LIQUID_ATOM_PREDICATE && atom_is_idb(&sub_lss, catom))
            atom_set_use_delta_scan(&sub_lss, catom, delta_atom_index == body_index);
    }

    sub_lss.constraints          = rule->body_atoms;
    sub_lss.constraint_count     = rule->body_count;
    sub_lss.num_vars             = rule->num_vars;
    sub_lss.rules                = parent_lss->rules;
    sub_lss.rule_count           = parent_lss->rule_count;
    sub_lss.rules_evaluated      = true;
    sub_lss.derived_facts_htab   = NULL;
    sub_lss.derived_fact_store   = parent_lss->derived_fact_store;
    sub_lss.delta_fact_store     = input_delta_store;
    sub_lss.edge_cache           = parent_lss->edge_cache;

    {
        MemoryContext sw = MemoryContextSwitchTo(sub_lss.solver_context);
        LiquidBinding *b = MemoryContextAllocZero(sub_lss.solver_context,
                                                  sizeof(LiquidBinding));
        sub_lss.current_frontier = list_make1(b);
        sub_lss.current_frontier_size = 1;
        MemoryContextSwitchTo(sw);
    }

    MemoryContextSwitchTo(oldcxt);

    while (!sub_lss.execution_done && sub_lss.current_frontier != NIL)
        solver_step(&sub_lss);

    /* Collect newly derived facts */
    foreach(fc, sub_lss.current_frontier)
    {
        LiquidBinding *b = (LiquidBinding *) lfirst(fc);

        emit_derived_fact(parent_lss, rule, b->values, b->is_bound, changed);
    }

    MemoryContextDelete(sub_lss.solver_context);
}

static void
mark_relevant_predicates_for_plan(const LiquidExecutionPlan *plan,
                                  bool *relevant_predicates,
                                  MemoryContext mcxt)
{
    int *stack;
    int  stack_count = 0;
    int  query_index;

    if (plan == NULL ||
        plan->predicate_count == 0 ||
        plan->query_predicate_index_count == 0)
    {
        return;
    }

    stack = MemoryContextAlloc(mcxt, sizeof(int) * plan->predicate_count);

    for (query_index = 0; query_index < plan->query_predicate_index_count; query_index++)
    {
        int predicate_index = plan->query_predicate_indexes[query_index];

        if (predicate_index < 0 || predicate_index >= plan->predicate_count)
            continue;
        if (relevant_predicates[predicate_index])
            continue;

        relevant_predicates[predicate_index] = true;
        stack[stack_count++] = predicate_index;
    }

    while (stack_count > 0)
    {
        int predicate_index = stack[--stack_count];
        int dep_index;

        for (dep_index = plan->predicate_dependency_offsets[predicate_index];
             dep_index < plan->predicate_dependency_offsets[predicate_index + 1];
             dep_index++)
        {
            int dependency_index = plan->predicate_dependency_indexes[dep_index];

            if (relevant_predicates[dependency_index])
                continue;

            relevant_predicates[dependency_index] = true;
            stack[stack_count++] = dependency_index;
        }
    }
}

static LiquidCompiledRule **
collect_relevant_rules_for_plan(const LiquidExecutionPlan *plan,
                                bool *relevant_predicates,
                                bool *relevant_sccs,
                                MemoryContext mcxt,
                                int *relevant_count_out)
{
    LiquidCompiledRule **relevant_rules = NULL;
    int                 rule_index;
    int                 relevant_count = 0;

    if (plan->rule_count > 0)
    {
        relevant_rules = MemoryContextAlloc(mcxt,
                                            sizeof(LiquidCompiledRule *) * plan->rule_count);
    }

    for (rule_index = 0; rule_index < plan->rule_count; rule_index++)
    {
        LiquidCompiledRule *rule = plan->rules[rule_index];

        if (rule->head_predicate_index < 0 ||
            !relevant_predicates[rule->head_predicate_index])
        {
            continue;
        }

        relevant_rules[relevant_count++] = rule;
        if (rule->head_scc_id >= 0)
            relevant_sccs[rule->head_scc_id] = true;
    }

    if (relevant_count_out != NULL)
        *relevant_count_out = relevant_count;

    return relevant_rules;
}

static void
evaluate_rules(LiquidScanState *lss)
{
    const LiquidExecutionPlan      *plan = lss->plan;
    int                             iters = 0;
    int                             relevant_rule_count = 0;
    LiquidCompiledRule            **relevant_rules;
    bool                           *relevant_predicates;
    bool                           *relevant_sccs;
    TransitiveClosurePlan           tc_plan;
    int                             tc_plan_scc_id = -1;
    int                             scc_idx;

    memset(&tc_plan, 0, sizeof(tc_plan));

    relevant_predicates = MemoryContextAllocZero(lss->solver_context,
                                                 sizeof(bool) * Max(1, plan->predicate_count));
    relevant_sccs = MemoryContextAllocZero(lss->solver_context,
                                           sizeof(bool) * Max(1, plan->scc_count));
    mark_relevant_predicates_for_plan(plan,
                                      relevant_predicates,
                                      lss->solver_context);
    relevant_rules = collect_relevant_rules_for_plan(plan,
                                                     relevant_predicates,
                                                     relevant_sccs,
                                                     lss->solver_context,
                                                     &relevant_rule_count);

    if (plan->transitive_closure_plan.valid &&
        plan->transitive_closure_plan.rule_index >= 0 &&
        plan->transitive_closure_plan.rule_index < plan->rule_count &&
        plan->transitive_closure_plan.scc_id >= 0 &&
        plan->transitive_closure_plan.scc_id < plan->scc_count &&
        relevant_sccs[plan->transitive_closure_plan.scc_id])
    {
        tc_plan.valid = true;
        tc_plan.predicate_id = plan->transitive_closure_plan.predicate_id;
        tc_plan.recursive_rule = plan->rules[plan->transitive_closure_plan.rule_index];
        tc_plan_scc_id = plan->transitive_closure_plan.scc_id;
    }

    lss->edge_cache = build_edge_cache_for_plan(lss,
                                                lss->solver_context,
                                                lss->constraints,
                                                lss->constraint_count,
                                                relevant_rules,
                                                relevant_rule_count);

    if (relevant_rule_count == 0)
    {
        lss->rules_evaluated = true;
        return;
    }

    lss->derived_facts_htab = create_derived_facts_htab(lss->solver_context);
    lss->derived_fact_store = create_derived_fact_store(lss->solver_context, false);
    lss->delta_fact_store = NULL;

    for (scc_idx = 0; scc_idx < plan->scc_count; scc_idx++)
    {
        int  scc_id = plan->scc_order[scc_idx];
        int  scc_rule_start = plan->scc_rule_offsets[scc_id];
        int  scc_rule_end = plan->scc_rule_offsets[scc_id + 1];
        int  scc_rule_count = scc_rule_end - scc_rule_start;
        bool recursive_scc = plan->scc_sizes[scc_id] > 1 ||
                             plan->scc_self_recursive[scc_id];

        if (!relevant_sccs[scc_id] || scc_rule_count == 0)
            continue;

        if (!recursive_scc)
        {
            bool scc_changed = false;
            int  rule_index;

            lss->delta_fact_store = NULL;

            for (rule_index = scc_rule_start; rule_index < scc_rule_end; rule_index++)
                run_rule(lss,
                         plan->rules[plan->scc_rule_indexes[rule_index]],
                         -1,
                         NULL,
                         &scc_changed);

            continue;
        }

        {
            bool          first_pass = true;
            MemoryContext prev_delta_context = NULL;
            DerivedFactStore *prev_delta_store = NULL;

            for (;;)
            {
                MemoryContext delta_context;
                bool          scc_changed = false;
                bool          tc_scc_resolved = false;
                int           rule_index;

                CHECK_FOR_INTERRUPTS();

                if (iters++ >= MAX_RULE_ITERATIONS)
                    ereport(ERROR,
                            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                             errmsg("recursive LIquid evaluation exceeded %d iterations",
                                    MAX_RULE_ITERATIONS)));

                delta_context = AllocSetContextCreate(lss->solver_context,
                                                      "Liquid Delta Facts",
                                                      ALLOCSET_DEFAULT_SIZES);
                lss->delta_fact_store = create_derived_fact_store(delta_context, false);

                for (rule_index = scc_rule_start; rule_index < scc_rule_end; rule_index++)
                {
                    LiquidCompiledRule *rule = plan->rules[plan->scc_rule_indexes[rule_index]];

                    if (tc_plan.valid && rule == tc_plan.recursive_rule)
                        continue;

                    if (first_pass)
                    {
                        if (!rule->has_idb_body)
                            run_rule(lss, rule, -1, prev_delta_store, &scc_changed);
                    }
                    else if (!rule->has_idb_body)
                    {
                        continue;
                    }
                    else if (!rule->is_recursive)
                        run_rule(lss, rule, -1, prev_delta_store, &scc_changed);
                    else if (prev_delta_store != NULL && prev_delta_store->fact_count > 0)
                    {
                        int idx;

                        if (rule->recursive_atom_count == 0)
                            run_rule(lss, rule, -1, prev_delta_store, &scc_changed);
                        else
                        {
                            for (idx = 0; idx < rule->recursive_atom_count; idx++)
                                run_rule(lss,
                                         rule,
                                         rule->recursive_atom_indexes[idx],
                                         prev_delta_store,
                                         &scc_changed);
                        }
                    }
                }

                if (first_pass && tc_plan.valid && tc_plan_scc_id == scc_id)
                {
                    DerivedFactStore *saved_delta_store = lss->delta_fact_store;

                    /*
                     * This SCC is fully resolved by the transitive-closure plan.
                     * Avoid populating per-iteration delta indexes for closure facts.
                     */
                    lss->delta_fact_store = NULL;
                    run_transitive_closure_plan(lss, &tc_plan, &scc_changed);
                    lss->delta_fact_store = saved_delta_store;
                    tc_scc_resolved = true;
                }

                first_pass = false;

                if (prev_delta_context != NULL)
                {
                    MemoryContextDelete(prev_delta_context);
                    prev_delta_context = NULL;
                    prev_delta_store = NULL;
                }

                if (tc_scc_resolved)
                {
                    MemoryContextDelete(delta_context);
                    lss->delta_fact_store = NULL;
                    break;
                }

                if (!scc_changed)
                {
                    MemoryContextDelete(delta_context);
                    lss->delta_fact_store = NULL;
                    break;
                }

                prev_delta_context = delta_context;
                prev_delta_store = lss->delta_fact_store;
            }

            if (prev_delta_context != NULL)
                MemoryContextDelete(prev_delta_context);
        }
    }

    lss->rules_evaluated = true;
}

static bool
should_skip_edge_cache_for_constraints(LiquidCompiledAtom **constraints,
                                       int constraint_count)
{
    LiquidCompiledAtom *atom;
    int                 bound_terms = 0;
    int                 i;

    if (constraint_count != 1)
        return false;

    atom = constraints[0];
    if (atom->type == LIQUID_ATOM_COMPOUND)
        return false;

    if (atom->type != LIQUID_ATOM_EDGE || atom->num_terms != 3)
        return false;

    for (i = 0; i < atom->num_terms; i++)
    {
        if (!atom->terms[i].is_var && atom->terms[i].id != LIQUID_UNKNOWN_ID)
            bound_terms++;
    }

    return bound_terms >= 1;
}

static bool
run_single_constraint_fast_path(LiquidScanState *lss)
{
    LiquidCompiledAtom *atom;
    LiquidBinding      *binding;
    List               *next_frontier = NIL;
    int                 next_frontier_size = 0;
    int64               scan_vals[MAX_DATALOG_VARS];
    int                 i;
    bool                has_unknown = false;

    if (lss->constraint_count != 1 ||
        list_length(lss->current_frontier) != 1)
        return false;

    atom = lss->constraints[0];
    binding = (LiquidBinding *) linitial(lss->current_frontier);

    if (atom_is_satisfied(lss, atom))
    {
        lss->execution_done = true;
        return true;
    }

    for (i = 0; i < atom->num_terms; i++)
    {
        if (!atom->terms[i].is_var &&
            atom->terms[i].id == LIQUID_UNKNOWN_ID)
        {
            has_unknown = true;
            break;
        }
    }

    if (has_unknown)
    {
        atom_set_satisfied(lss, atom, true);
        lss->current_frontier = NIL;
        lss->current_frontier_size = 0;
        lss->execution_done = true;
        return true;
    }

    for (i = 0; i < atom->num_terms; i++)
    {
        scan_vals[i] = -1;
        if (atom->terms[i].is_var)
        {
            if (binding->is_bound[atom->terms[i].var_idx])
                scan_vals[i] = binding->values[atom->terms[i].var_idx];
        }
        else
            scan_vals[i] = atom->terms[i].id;
    }

    if (atom->type == LIQUID_ATOM_EDGE)
    {
        if (lss->edge_cache)
        {
            EdgeBindingContext ctx;

            ctx.lss = lss;
            ctx.atom = atom;
            ctx.binding = binding;
            ctx.dedup_htab = NULL;
            ctx.next_frontier = &next_frontier;
            ctx.next_frontier_size = &next_frontier_size;

            scan_edge_cache_visit(lss->edge_cache,
                                  scan_vals[0],
                                  scan_vals[1],
                                  scan_vals[2],
                                  append_edge_binding,
                                  &ctx);
        }
        else
        {
            EdgeBindingContext ctx;

            ctx.lss = lss;
            ctx.atom = atom;
            ctx.binding = binding;
            ctx.dedup_htab = NULL;
            ctx.next_frontier = &next_frontier;
            ctx.next_frontier_size = &next_frontier_size;

            scan_facts_visit(lss,
                             scan_vals[0],
                             scan_vals[1],
                             scan_vals[2],
                             append_scan_fact_binding,
                             &ctx);
        }
    }
    else if (atom->type == LIQUID_ATOM_COMPOUND)
    {
        if (can_scan_compound_from_edge_cache(lss, atom))
        {
            CompoundBindingContext ctx;

            ctx.lss = lss;
            ctx.atom = atom;
            ctx.binding = binding;
            ctx.dedup_htab = NULL;
            ctx.next_frontier = &next_frontier;
            ctx.next_frontier_size = &next_frontier_size;

            scan_compound_from_edge_cache_visit(lss->edge_cache,
                                                atom,
                                                scan_vals,
                                                append_compound_binding,
                                                &ctx);
        }
        else
        {
            List     *scan_res = scan_compound(lss, atom, scan_vals);
            ListCell *res_lc;

            foreach(res_lc, scan_res)
            {
                int64 *row = (int64 *) lfirst(res_lc);
                int64  row_vals[MAX_DATALOG_VARS];
                int    n = atom->num_terms;

                if (n > MAX_DATALOG_VARS)
                    n = MAX_DATALOG_VARS;
                memcpy(row_vals, row, n * sizeof(int64));

                try_append_binding(lss,
                                   atom,
                                   binding,
                                   row_vals,
                                   NULL,
                                   &next_frontier,
                                   &next_frontier_size);
            }
            list_free(scan_res);
        }
    }
    else if (atom->type == LIQUID_ATOM_PREDICATE && atom_is_idb(lss, atom))
    {
        DerivedFactStore *store = (atom_uses_delta_scan(lss, atom) &&
                                   lss->delta_fact_store != NULL)
            ? lss->delta_fact_store : lss->derived_fact_store;
        DerivedFactCandidateSet candidates = derived_fact_store_candidates(store,
                                                                           atom->predicate_id,
                                                                           atom->num_terms,
                                                                           scan_vals);
        int idx;

        for (idx = 0; idx < candidates.count; idx++)
        {
            LiquidFact *df = candidates.facts[idx];
            bool        match = true;
            int         k;
            int64       row_vals[MAX_DATALOG_VARS];

            if (df->predicate_id != atom->predicate_id ||
                df->num_terms != atom->num_terms)
                continue;

            for (k = 0; k < df->num_terms; k++)
            {
                if (scan_vals[k] != -1 && scan_vals[k] != df->terms[k])
                {
                    match = false;
                    break;
                }
                row_vals[k] = df->terms[k];
            }

            if (match)
                try_append_binding(lss,
                                   atom,
                                   binding,
                                   row_vals,
                                   NULL,
                                   &next_frontier,
                                   &next_frontier_size);
        }
    }

    atom_set_satisfied(lss, atom, true);
    lss->current_frontier = next_frontier;
    lss->current_frontier_size = next_frontier_size;
    lss->execution_done = true;
    return true;
}

/* ================================================================
 * Top-level solver
 * ================================================================ */

void
run_solver(LiquidScanState *lss)
{
    initialize_atom_exec_states(lss);

    if (!lss->rules_evaluated && lss->rule_count > 0)
        evaluate_rules(lss);
    else if (!lss->edge_cache)
    {
        if (!should_skip_edge_cache_for_constraints(lss->constraints,
                                                    lss->constraint_count))
            lss->edge_cache = build_edge_cache_for_plan(lss,
                                                        lss->solver_context,
                                                        lss->constraints,
                                                        lss->constraint_count,
                                                        NULL,
                                                        0);
    }

    if (run_single_constraint_fast_path(lss))
        return;

    while (!lss->execution_done && lss->current_frontier != NIL)
        solver_step(lss);
}
