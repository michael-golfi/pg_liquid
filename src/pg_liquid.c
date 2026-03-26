#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "pg_liquid.h"

PG_MODULE_MAGIC;

int pg_liquid_edge_cache_budget_kb = 0;
int pg_liquid_edge_cache_row_estimate_bytes = 96;
char *pg_liquid_policy_principal = NULL;

typedef struct LiteralCacheEntry
{
    int64   key;
    const char *literal;
    Datum   text_datum;
    bool    has_text_datum;
} LiteralCacheEntry;

typedef struct EdgeBatch
{
    int64        *subjects;
    int64        *predicates;
    int64        *objects;
    struct EdgeLiteralIdCacheEntry **literal_id_cache_buckets;
    int           literal_id_cache_bucket_count;
    int           count;
    int           capacity;
    MemoryContext mcxt;
} EdgeBatch;

typedef struct EdgeLiteralIdCacheEntry
{
    uint32                         hash;
    const char                    *literal;
    int64                          id;
    struct EdgeLiteralIdCacheEntry *next;
} EdgeLiteralIdCacheEntry;

static HTAB *
create_literal_cache(MemoryContext mcxt)
{
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(LiteralCacheEntry);
    ctl.hcxt = mcxt;
    return hash_create("Literal Cache", 256, &ctl,
                       HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static Datum
lookup_identity_text_datum_cached(HTAB *cache, MemoryContext mcxt, int64 id, bool *isnull)
{
    LiteralCacheEntry *entry;
    bool               found;
    entry = hash_search(cache, &id, HASH_ENTER, &found);
    if (!found)
    {
        const char *literal = lookup_identity_literal_ref(id);

        entry->key = id;
        entry->literal = NULL;
        entry->text_datum = (Datum) 0;
        entry->has_text_datum = false;

        if (literal != NULL)
            entry->literal = literal;
    }

    if (entry->literal == NULL)
    {
        *isnull = true;
        return (Datum) 0;
    }

    if (!entry->has_text_datum)
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(mcxt);

        entry->text_datum = CStringGetTextDatum((char *) entry->literal);
        entry->has_text_datum = true;
        MemoryContextSwitchTo(oldcxt);
    }

    *isnull = false;
    return entry->text_datum;
}

static void
edge_batch_reserve(EdgeBatch *batch, int capacity_hint)
{
    MemoryContext oldcxt;

    if (capacity_hint <= 0)
        return;

    if (batch->capacity >= capacity_hint)
        return;

    oldcxt = MemoryContextSwitchTo(batch->mcxt);

    if (batch->capacity == 0)
    {
        batch->capacity = capacity_hint;
        batch->subjects = palloc(sizeof(int64) * batch->capacity);
        batch->predicates = palloc(sizeof(int64) * batch->capacity);
        batch->objects = palloc(sizeof(int64) * batch->capacity);
    }
    else
    {
        batch->capacity = capacity_hint;
        batch->subjects = repalloc(batch->subjects, sizeof(int64) * batch->capacity);
        batch->predicates = repalloc(batch->predicates, sizeof(int64) * batch->capacity);
        batch->objects = repalloc(batch->objects, sizeof(int64) * batch->capacity);
    }

    MemoryContextSwitchTo(oldcxt);
}

static void
edge_batch_add(EdgeBatch *batch, int64 subject_id, int64 predicate_id, int64 object_id)
{
    if (batch->capacity == 0)
    {
        MemoryContext oldcxt = MemoryContextSwitchTo(batch->mcxt);

        batch->capacity = 256;
        batch->subjects = palloc(sizeof(int64) * batch->capacity);
        batch->predicates = palloc(sizeof(int64) * batch->capacity);
        batch->objects = palloc(sizeof(int64) * batch->capacity);
        MemoryContextSwitchTo(oldcxt);
    }
    else if (batch->count >= batch->capacity)
    {
        batch->capacity *= 2;
        batch->subjects = repalloc(batch->subjects, sizeof(int64) * batch->capacity);
        batch->predicates = repalloc(batch->predicates, sizeof(int64) * batch->capacity);
        batch->objects = repalloc(batch->objects, sizeof(int64) * batch->capacity);
    }

    batch->subjects[batch->count] = subject_id;
    batch->predicates[batch->count] = predicate_id;
    batch->objects[batch->count] = object_id;
    batch->count++;
}

static void
edge_batch_flush(EdgeBatch *batch)
{
    if (batch->count <= 0)
        return;

    insert_edges_batch(batch->subjects,
                       batch->predicates,
                       batch->objects,
                       batch->count);
    batch->count = 0;
}


static int64
resolve_literal_id(const char *literal)
{
    int64 id;

    if (literal == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("assertion literal may not be null")));

    id = lookup_identity(literal);
    if (id >= 0)
        return id;

    return get_or_create_identity(literal);
}

static int64
edge_batch_resolve_literal_id(EdgeBatch *batch, const char *literal)
{
    uint32                  hash;
    uint32                  bucket;
    EdgeLiteralIdCacheEntry *entry;
    MemoryContext           oldcxt;

    if (literal == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("assertion literal may not be null")));

    if (batch == NULL)
        return resolve_literal_id(literal);

    if (batch->literal_id_cache_buckets == NULL)
    {
        int bucket_count = Max(64,
                               Min(8192,
                                   (batch->capacity > 0)
                                       ? batch->capacity
                                       : 256));

        oldcxt = MemoryContextSwitchTo(batch->mcxt);
        batch->literal_id_cache_bucket_count = bucket_count;
        batch->literal_id_cache_buckets =
            MemoryContextAllocZero(batch->mcxt,
                                   sizeof(EdgeLiteralIdCacheEntry *) *
                                   batch->literal_id_cache_bucket_count);
        MemoryContextSwitchTo(oldcxt);
    }

    hash = DatumGetUInt32(hash_any((const unsigned char *) literal,
                                   (int) strlen(literal)));
    bucket = hash % batch->literal_id_cache_bucket_count;
    entry = batch->literal_id_cache_buckets[bucket];
    while (entry != NULL)
    {
        if (entry->hash == hash && strcmp(entry->literal, literal) == 0)
            return entry->id;
        entry = entry->next;
    }

    {
        int64 resolved_id = resolve_literal_id(literal);

        oldcxt = MemoryContextSwitchTo(batch->mcxt);
        entry = MemoryContextAlloc(batch->mcxt, sizeof(*entry));
        entry->hash = hash;
        entry->literal = literal;
        entry->id = resolved_id;
        entry->next = batch->literal_id_cache_buckets[bucket];
        batch->literal_id_cache_buckets[bucket] = entry;
        MemoryContextSwitchTo(oldcxt);
        return entry->id;
    }
}

static int64
resolve_bound_id(LiquidCompiledTerm *term, int64 *values, bool *bound, EdgeBatch *edge_batch)
{
    if (!term->is_var)
    {
        if (term->id == LIQUID_UNKNOWN_ID && term->literal != NULL)
            term->id = edge_batch_resolve_literal_id(edge_batch, term->literal);
        return term->id;
    }

    if (!bound[term->var_idx])
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("unbound variable %s in assertion clause", term->var_name)));

    return values[term->var_idx];
}

static const char *
resolve_bound_literal(LiquidCompiledTerm *term, int64 *values, bool *bound)
{
    const char *literal;

    if (!term->is_var)
        return term->literal;

    if (!bound[term->var_idx])
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("unbound variable %s in assertion clause", term->var_name)));

    literal = lookup_identity_literal_ref(values[term->var_idx]);
    if (literal == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("bound variable %s resolved to unknown identity %ld",
                        term->var_name, (long) values[term->var_idx])));

    return literal;
}

static void
bind_or_check_variable(LiquidCompiledTerm *term, int64 value, int64 *values, bool *bound)
{
    if (!term->is_var)
    {
        if (term->id != value)
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                     errmsg("cid literal does not match computed compound identity")));
        return;
    }

    if (bound[term->var_idx] && values[term->var_idx] != value)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("variable %s bound inconsistently inside assertion clause",
                        term->var_name)));

    values[term->var_idx] = value;
    bound[term->var_idx]  = true;
}

static void
execute_assertion_clause(LiquidCompiledAssertionClause *clause, EdgeBatch *edge_batch)
{
    int64     values[MAX_DATALOG_VARS];
    bool      bound[MAX_DATALOG_VARS];
    int       atom_index;

    if (clause->atom_count == 1)
    {
        LiquidCompiledAtom *single_atom = clause->atoms[0];

        if (single_atom->type == LIQUID_ATOM_EDGE &&
            !single_atom->terms[0].is_var &&
            !single_atom->terms[1].is_var &&
            !single_atom->terms[2].is_var)
        {
            int64 s_id = single_atom->terms[0].id;
            int64 p_id = single_atom->terms[1].id;
            int64 o_id = single_atom->terms[2].id;

            if (s_id == LIQUID_UNKNOWN_ID && single_atom->terms[0].literal != NULL)
                s_id = edge_batch_resolve_literal_id(edge_batch,
                                                     single_atom->terms[0].literal);
            if (p_id == LIQUID_UNKNOWN_ID && single_atom->terms[1].literal != NULL)
                p_id = edge_batch_resolve_literal_id(edge_batch,
                                                     single_atom->terms[1].literal);
            if (o_id == LIQUID_UNKNOWN_ID && single_atom->terms[2].literal != NULL)
                o_id = edge_batch_resolve_literal_id(edge_batch,
                                                     single_atom->terms[2].literal);

            edge_batch_add(edge_batch, s_id, p_id, o_id);
            return;
        }
    }

    memset(values, 0, sizeof(values));
    memset(bound, 0, sizeof(bound));

    for (atom_index = 0; atom_index < clause->atom_count; atom_index++)
    {
        LiquidCompiledAtom *atom = clause->atoms[atom_index];

        if (atom->type == LIQUID_ATOM_EDGE)
        {
            int64 s_id = resolve_bound_id(&atom->terms[0], values, bound, edge_batch);
            int64 p_id = resolve_bound_id(&atom->terms[1], values, bound, edge_batch);
            int64 o_id = resolve_bound_id(&atom->terms[2], values, bound, edge_batch);

            edge_batch_add(edge_batch, s_id, p_id, o_id);
            continue;
        }

        if (atom->type == LIQUID_ATOM_COMPOUND)
        {
            const char **role_names;
            const char **role_values;
            int          i;
            int64        compound_id;

            role_names  = palloc(sizeof(char *) * atom->num_named_args);
            role_values = palloc(sizeof(char *) * atom->num_named_args);

            for (i = 0; i < atom->num_named_args; i++)
            {
                role_names[i]  = atom->named_arg_names[i];
                role_values[i] = resolve_bound_literal(&atom->terms[i + 1], values, bound);
            }

            compound_id = insert_compound(atom->name,
                                          atom->num_named_args,
                                          role_names,
                                          atom->named_arg_ids,
                                          role_values);
            bind_or_check_variable(&atom->terms[0], compound_id, values, bound);
            pfree(role_names);
            pfree(role_values);
            continue;
        }

        if (atom->type != LIQUID_ATOM_PREDICATE)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("unsupported assertion atom")));

        if (strcmp(atom->name, "DefPred") == 0)
        {
            const char *name = resolve_bound_literal(&atom->terms[0], values, bound);
            const char *subject_cardinality = resolve_bound_literal(&atom->terms[1], values, bound);
            const char *subject_type = resolve_bound_literal(&atom->terms[2], values, bound);
            const char *object_cardinality = resolve_bound_literal(&atom->terms[3], values, bound);
            const char *object_type = resolve_bound_literal(&atom->terms[4], values, bound);

            define_predicate(name, subject_cardinality, subject_type,
                             object_cardinality, object_type);
            continue;
        }

        if (strcmp(atom->name, "DefCompound") == 0)
        {
            const char *compound_name = resolve_bound_literal(&atom->terms[0], values, bound);
            const char *predicate_name = resolve_bound_literal(&atom->terms[1], values, bound);
            const char *object_cardinality = resolve_bound_literal(&atom->terms[2], values, bound);
            const char *object_type = resolve_bound_literal(&atom->terms[3], values, bound);

            define_compound(compound_name, predicate_name,
                            object_cardinality, object_type);
            continue;
        }

        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("top-level assertion %s is not supported; use Edge, DefPred, DefCompound, or Type@(...)",
                        atom->name)));
    }
}

static void
execute_compiled_direct_edge_batch(LiquidAssertionOp *op, EdgeBatch *edge_batch)
{
    int i;

    for (i = 0; i < op->data.edge_batch.count; i++)
    {
        LiquidEdgeLiteralTriple *edge = &op->data.edge_batch.edges[i];

        edge_batch_add(edge_batch,
                       edge_batch_resolve_literal_id(edge_batch,
                                                     edge->subject_literal),
                       edge_batch_resolve_literal_id(edge_batch,
                                                     edge->predicate_literal),
                       edge_batch_resolve_literal_id(edge_batch,
                                                     edge->object_literal));
    }
}

static void
execute_define_pred_atom(LiquidCompiledAtom *atom)
{
    int64 values[MAX_DATALOG_VARS];
    bool  bound[MAX_DATALOG_VARS];

    memset(values, 0, sizeof(values));
    memset(bound, 0, sizeof(bound));

    define_predicate(resolve_bound_literal(&atom->terms[0], values, bound),
                     resolve_bound_literal(&atom->terms[1], values, bound),
                     resolve_bound_literal(&atom->terms[2], values, bound),
                     resolve_bound_literal(&atom->terms[3], values, bound),
                     resolve_bound_literal(&atom->terms[4], values, bound));
}

static void
execute_define_compound_atom(LiquidCompiledAtom *atom)
{
    int64 values[MAX_DATALOG_VARS];
    bool  bound[MAX_DATALOG_VARS];

    memset(values, 0, sizeof(values));
    memset(bound, 0, sizeof(bound));

    define_compound(resolve_bound_literal(&atom->terms[0], values, bound),
                    resolve_bound_literal(&atom->terms[1], values, bound),
                    resolve_bound_literal(&atom->terms[2], values, bound),
                    resolve_bound_literal(&atom->terms[3], values, bound));
}

static void
execute_insert_compound_atom(LiquidCompiledAtom *atom)
{
    int64        values[MAX_DATALOG_VARS];
    bool         bound[MAX_DATALOG_VARS];
    const char  *role_values[MAX_COMPOUND_ARGS];
    int          i;
    int64        compound_id;

    memset(values, 0, sizeof(values));
    memset(bound, 0, sizeof(bound));

    for (i = 0; i < atom->num_named_args; i++)
    {
        role_values[i] = resolve_bound_literal(&atom->terms[i + 1], values, bound);
    }

    compound_id = insert_compound(atom->name,
                                  atom->num_named_args,
                                  (const char *const *) atom->named_arg_names,
                                  atom->named_arg_ids,
                                  role_values);
    bind_or_check_variable(&atom->terms[0], compound_id, values, bound);
}

static void
execute_assertion_plan(LiquidExecutionPlan *plan, EdgeBatch *edge_batch)
{
    int i;

    for (i = 0; i < plan->op_count; i++)
    {
        LiquidAssertionOp *op = &plan->ops[i];

        if (op->kind != LIQUID_ASSERTION_OP_EDGE_BATCH)
            edge_batch_flush(edge_batch);

        switch (op->kind)
        {
            case LIQUID_ASSERTION_OP_EDGE_BATCH:
                execute_compiled_direct_edge_batch(op, edge_batch);
                break;
            case LIQUID_ASSERTION_OP_DEFINE_PRED:
                execute_define_pred_atom(op->data.atom);
                break;
            case LIQUID_ASSERTION_OP_DEFINE_COMPOUND:
                execute_define_compound_atom(op->data.atom);
                break;
            case LIQUID_ASSERTION_OP_INSERT_COMPOUND:
                execute_insert_compound_atom(op->data.atom);
                break;
            case LIQUID_ASSERTION_OP_CLAUSE_FALLBACK:
                execute_assertion_clause(&op->data.clause, edge_batch);
                break;
        }
    }
}

static void
materialize_results(LiquidExecutionPlan *plan,
                    LiquidScanState *lss,
                    TupleDesc tdesc,
                    MemoryContext per_query_ctx,
                    Tuplestorestate *tstore)
{
    ListCell *lc;
    HTAB     *literal_cache = create_literal_cache(per_query_ctx);
    MemoryContext oldcxt = MemoryContextSwitchTo(per_query_ctx);

    foreach(lc, lss->current_frontier)
    {
        LiquidBinding *binding = (LiquidBinding *) lfirst(lc);
        Datum          values[MAX_DATALOG_VARS];
        bool           nulls[MAX_DATALOG_VARS];
        int            natts = tdesc->natts;
        int            i;

        memset(values, 0, sizeof(values));
        memset(nulls, true, sizeof(nulls));

        for (i = 0; i < natts && i < plan->query_num_output_vars; i++)
        {
            int output_idx = plan->query_output_var_idxs[i];

            if (binding->is_bound[output_idx])
            {
                bool  isnull_literal = false;
                Datum literal_datum = lookup_identity_text_datum_cached(literal_cache,
                                                                         per_query_ctx,
                                                                         binding->values[output_idx],
                                                                         &isnull_literal);

                if (!isnull_literal)
                {
                    values[i] = literal_datum;
                    nulls[i] = false;
                }
            }
        }

        tuplestore_putvalues(tstore, tdesc, values, nulls);
    }

    MemoryContextSwitchTo(oldcxt);
}

static Datum
liquid_query_internal(FunctionCallInfo fcinfo,
                      int program_arg_index,
                      const char *forced_principal,
                      bool allow_assertions)
{
    char            *program_text = text_to_cstring(PG_GETARG_TEXT_PP(program_arg_index));
    ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc        tupdesc;
    TupleDesc        result_desc;
    Tuplestorestate *tstore;
    MemoryContext    per_query_ctx;
    MemoryContext    oldcxt;
    LiquidProgram   *program;
    LiquidExecutionPlan *plan;
    LiquidScanState *lss = NULL;
    EdgeBatch        edge_batch;

    if (!rsinfo || !IsA(rsinfo, ReturnSetInfo))
        elog(ERROR, "set-valued function called in context that cannot accept a set");

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context that cannot accept type record")));

    per_query_ctx = rsinfo->econtext
        ? rsinfo->econtext->ecxt_per_query_memory
        : CurrentMemoryContext;

    oldcxt = MemoryContextSwitchTo(per_query_ctx);
    tstore = tuplestore_begin_heap(true, false, 1024);
    result_desc = CreateTupleDescCopy(tupdesc);
    BlessTupleDesc(result_desc);
    MemoryContextSwitchTo(oldcxt);

    SPI_connect();

    program = parse_liquid_program(program_text);
    plan = compile_liquid_execution_plan(program, per_query_ctx);

    memset(&edge_batch, 0, sizeof(edge_batch));
    edge_batch.mcxt = per_query_ctx;

    if (!allow_assertions && plan->has_assertions)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("liquid.read_as(...) does not allow top-level assertions"),
                 errhint("Use liquid.query_as(...) from a trusted write-capable path when assertions are required.")));

    preload_identities_batch(plan->preload_literals,
                             plan->preload_literal_count);
    edge_batch_reserve(&edge_batch,
                       Max(256, plan->direct_edge_capacity_hint));
    execute_assertion_plan(plan, &edge_batch);
    edge_batch_flush(&edge_batch);

    if (plan->has_assertions)
        CommandCounterIncrement();

    finalize_execution_plan_ids(plan);

    if (plan->query_atom_count > 0)
    {
        lss = MemoryContextAllocZero(per_query_ctx, sizeof(LiquidScanState));
        lss->plan = plan;
        lss->solver_context = AllocSetContextCreate(per_query_ctx,
                                                    "Liquid Solver Context",
                                                    ALLOCSET_DEFAULT_SIZES);
        lss->constraints     = plan->query_atoms;
        lss->constraint_count = plan->query_atom_count;
        lss->num_vars        = plan->query_num_vars;
        lss->rules           = plan->rules;
        lss->rule_count      = plan->rule_count;
        lss->rules_evaluated = false;
        lss->execution_done  = false;

        MemoryContextSwitchTo(lss->solver_context);
        lss->atom_state = MemoryContextAllocZero(lss->solver_context,
                                                 sizeof(LiquidAtomExecState) *
                                                 Max(1, plan->atom_count));
        lss->current_frontier = list_make1(MemoryContextAllocZero(lss->solver_context,
                                                                  sizeof(LiquidBinding)));
        lss->current_frontier_size = 1;
        if (forced_principal != NULL)
        {
            lss->policy_context.principal_literal = pstrdup(forced_principal);
            lss->policy_context.principal_forced = forced_principal[0] != '\0';
        }
        MemoryContextSwitchTo(per_query_ctx);

        run_solver(lss);
        materialize_results(plan, lss, result_desc, per_query_ctx, tstore);
    }

    SPI_finish();

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult  = tstore;
    rsinfo->setDesc    = result_desc;

    return (Datum) 0;
}

void
_PG_init(void)
{
    register_catalog_xact_callbacks();

    DefineCustomIntVariable("pg_liquid.edge_cache_budget_kb",
                            "Maximum in-memory edge-cache budget in kilobytes. "
                            "Set to 0 to derive the budget from work_mem.",
                            NULL,
                            &pg_liquid_edge_cache_budget_kb,
                            0,
                            0,
                            INT_MAX / 1024,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("pg_liquid.edge_cache_row_estimate_bytes",
                            "Estimated bytes per cached edge row when deciding "
                            "whether to materialize the edge cache.",
                            NULL,
                            &pg_liquid_edge_cache_row_estimate_bytes,
                            96,
                            32,
                            4096,
                            PGC_USERSET,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomStringVariable("pg_liquid.policy_principal",
                               "Session principal used for LIquid CLS policy checks. "
                               "When unset, LIquid CLS filtering is disabled.",
                               NULL,
                               &pg_liquid_policy_principal,
                               NULL,
                               PGC_USERSET,
                               0,
                               NULL,
                               NULL,
                               NULL);
}

PG_FUNCTION_INFO_V1(liquid_query);
Datum
liquid_query(PG_FUNCTION_ARGS)
{
    return liquid_query_internal(fcinfo, 0, NULL, true);
}

PG_FUNCTION_INFO_V1(liquid_query_as);
Datum
liquid_query_as(PG_FUNCTION_ARGS)
{
    char *principal = text_to_cstring(PG_GETARG_TEXT_PP(0));

    return liquid_query_internal(fcinfo, 1, principal, true);
}

PG_FUNCTION_INFO_V1(liquid_read_as);
Datum
liquid_read_as(PG_FUNCTION_ARGS)
{
    char *principal = text_to_cstring(PG_GETARG_TEXT_PP(0));

    return liquid_query_internal(fcinfo, 1, principal, false);
}
