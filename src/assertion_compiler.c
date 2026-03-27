#include "postgres.h"
#include "pg_liquid.h"
#include "common/hashfn.h"

typedef struct Int64IndexEntry
{
    int64 key;
    int   index;
} Int64IndexEntry;

typedef struct PredicateSccInfo
{
    int64  *pred_ids;
    int     num_preds;
    HTAB   *pred_index;
    bool   *adjacency;
    int    *scc_ids;
    int     num_sccs;
    int    *scc_sizes;
    bool   *scc_self_recursive;
    int    *strata;
} PredicateSccInfo;

typedef struct LiteralDedupeEntry
{
    uint32                    hash;
    const char               *literal;
    struct LiteralDedupeEntry *next;
} LiteralDedupeEntry;

static void switch_to_plan_context(LiquidExecutionPlan *plan, MemoryContext *oldcxt);
static void restore_memory_context(MemoryContext oldcxt);
static void plan_append_preload_literal(LiquidExecutionPlan *plan, const char *literal);
static void dedupe_preload_literals(LiquidExecutionPlan *plan);
static bool is_direct_edge_literal_atom(LiquidAtom *atom);
static bool term_has_literal(LiquidTerm *term);
static void append_compound_identity_preload(LiquidExecutionPlan *plan,
                                             const char *compound_type,
                                             int num_roles,
                                             const char *const *role_names,
                                             const char *const *role_values);
static void maybe_append_assertion_identity_preloads(LiquidExecutionPlan *plan,
                                                     LiquidAtom *atom,
                                                     LiquidAssertionOpKind kind);
static void edge_batch_op_add_literals(LiquidExecutionPlan *plan,
                                       LiquidAssertionOp *op,
                                       const char *subject_literal,
                                       const char *predicate_literal,
                                       const char *object_literal);

static bool
query_uses_predicates(LiquidProgram *program)
{
    ListCell *lc;

    if (program == NULL)
        return false;

    foreach(lc, program->query_atoms)
    {
        LiquidAtom *atom = (LiquidAtom *) lfirst(lc);

        if (atom->type == LIQUID_ATOM_PREDICATE)
            return true;
    }

    return false;
}

static void
accumulate_atom_storage_requirements(LiquidAtom *atom,
                                     int *atom_count,
                                     int *term_count,
                                     int *named_arg_count,
                                     int *unresolved_ref_count)
{
    if (atom == NULL)
        return;

    (*atom_count)++;
    *term_count += atom->num_terms;
    *named_arg_count += atom->num_named_args;
    *unresolved_ref_count += atom->num_terms + atom->num_named_args + 1;
}

static void
reserve_plan_storage(LiquidExecutionPlan *plan, LiquidProgram *program)
{
    ListCell *lc;
    int       atom_capacity = 0;
    int       term_capacity = 0;
    int       named_arg_capacity = 0;
    int       query_atom_capacity = 0;
    int       rule_capacity = 0;
    int       rule_body_capacity = 0;
    int       unresolved_ref_capacity = 0;
    int       preload_capacity = 0;
    int       op_capacity = 0;
    int       edge_literal_capacity = 0;
    bool      include_rules = query_uses_predicates(program);
    bool      in_direct_edge_batch = false;
    MemoryContext oldcxt;

    if (program == NULL)
        return;

    foreach(lc, program->assertions)
    {
        LiquidAssertionClause *clause = (LiquidAssertionClause *) lfirst(lc);
        ListCell              *atom_lc;
        bool                   direct_edge_clause = clause->direct_edge_count > 0;

        if (direct_edge_clause)
        {
            edge_literal_capacity += clause->direct_edge_count;
            if (!in_direct_edge_batch)
                op_capacity++;
            in_direct_edge_batch = true;
            preload_capacity += 3 * clause->direct_edge_count;
        }
        else
        {
            op_capacity++;
            in_direct_edge_batch = false;
        }

        foreach(atom_lc, clause->atoms)
        {
            LiquidAtom *atom = (LiquidAtom *) lfirst(atom_lc);

            if (!direct_edge_clause)
            {
                accumulate_atom_storage_requirements(atom,
                                                     &atom_capacity,
                                                     &term_capacity,
                                                     &named_arg_capacity,
                                                     &unresolved_ref_capacity);
            }
            preload_capacity += atom->num_terms;
            if (atom->type == LIQUID_ATOM_COMPOUND)
                preload_capacity += atom->num_named_args + 1;
        }
    }

    query_atom_capacity = list_length(program->query_atoms);
    foreach(lc, program->query_atoms)
        accumulate_atom_storage_requirements((LiquidAtom *) lfirst(lc),
                                             &atom_capacity,
                                             &term_capacity,
                                             &named_arg_capacity,
                                             &unresolved_ref_capacity);

    if (include_rules)
    {
        rule_capacity = list_length(program->rules);
        foreach(lc, program->rules)
        {
            LiquidRule *rule = (LiquidRule *) lfirst(lc);
            ListCell   *body_lc;

            accumulate_atom_storage_requirements(rule->head,
                                                 &atom_capacity,
                                                 &term_capacity,
                                                 &named_arg_capacity,
                                                 &unresolved_ref_capacity);
            rule_body_capacity += list_length(rule->body);

            foreach(body_lc, rule->body)
                accumulate_atom_storage_requirements((LiquidAtom *) lfirst(body_lc),
                                                     &atom_capacity,
                                                     &term_capacity,
                                                     &named_arg_capacity,
                                                     &unresolved_ref_capacity);
        }
    }

    switch_to_plan_context(plan, &oldcxt);

    if (op_capacity > 0)
    {
        plan->ops = palloc0(sizeof(LiquidAssertionOp) * op_capacity);
        plan->op_capacity = op_capacity;
    }
    if (edge_literal_capacity > 0)
    {
        plan->edge_literals = palloc(sizeof(LiquidEdgeLiteralTriple) * edge_literal_capacity);
        plan->edge_literal_capacity = edge_literal_capacity;
    }
    if (term_capacity > 0)
    {
        plan->terms = palloc(sizeof(LiquidCompiledTerm) * term_capacity);
        plan->term_capacity = term_capacity;
    }
    if (atom_capacity > 0)
    {
        plan->atoms = palloc0(sizeof(LiquidCompiledAtom) * atom_capacity);
        plan->atom_capacity = atom_capacity;
    }
    if (named_arg_capacity > 0)
    {
        plan->named_arg_names = palloc(sizeof(char *) * named_arg_capacity);
        plan->named_arg_ids = palloc(sizeof(int64) * named_arg_capacity);
        plan->named_arg_capacity = named_arg_capacity;
    }
    if (query_atom_capacity > 0)
    {
        plan->query_atoms = palloc(sizeof(LiquidCompiledAtom *) * query_atom_capacity);
        plan->query_atom_capacity = query_atom_capacity;
    }
    if (rule_capacity > 0)
    {
        plan->rules_storage = palloc0(sizeof(LiquidCompiledRule) * rule_capacity);
        plan->rules = palloc(sizeof(LiquidCompiledRule *) * rule_capacity);
        plan->rule_capacity = rule_capacity;
    }
    if (rule_body_capacity > 0)
    {
        plan->rule_body_atoms = palloc(sizeof(LiquidCompiledAtom *) * rule_body_capacity);
        plan->rule_body_atom_capacity = rule_body_capacity;
        plan->recursive_atom_indexes = palloc(sizeof(int) * rule_body_capacity);
        plan->recursive_atom_index_capacity = rule_body_capacity;
    }
    if (unresolved_ref_capacity > 0)
    {
        plan->unresolved_identity_refs =
            palloc(sizeof(LiquidPlanUnresolvedIdentityRef) * unresolved_ref_capacity);
        plan->unresolved_identity_ref_capacity = unresolved_ref_capacity;
    }
    if (preload_capacity > 0)
    {
        plan->preload_literals = palloc(sizeof(char *) * preload_capacity);
        plan->preload_literal_capacity = preload_capacity;
    }

    restore_memory_context(oldcxt);
}

static void
switch_to_plan_context(LiquidExecutionPlan *plan, MemoryContext *oldcxt)
{
    *oldcxt = MemoryContextSwitchTo(plan->mcxt);
}

static void
restore_memory_context(MemoryContext oldcxt)
{
    MemoryContextSwitchTo(oldcxt);
}

static void
ensure_terms_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->term_count + additional;
    if (required <= plan->term_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->term_capacity = Max(required,
                              (plan->term_capacity == 0)
                                  ? 64
                                  : plan->term_capacity * 2);
    if (plan->terms == NULL)
        plan->terms = palloc(sizeof(LiquidCompiledTerm) * plan->term_capacity);
    else
        plan->terms = repalloc(plan->terms,
                               sizeof(LiquidCompiledTerm) * plan->term_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_atoms_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;
    int           old_capacity;

    if (additional <= 0)
        return;

    required = plan->atom_count + additional;
    if (required <= plan->atom_capacity)
        return;

    old_capacity = plan->atom_capacity;
    switch_to_plan_context(plan, &oldcxt);
    plan->atom_capacity = Max(required,
                              (plan->atom_capacity == 0)
                                  ? 32
                                  : plan->atom_capacity * 2);
    if (plan->atoms == NULL)
        plan->atoms = palloc0(sizeof(LiquidCompiledAtom) * plan->atom_capacity);
    else
    {
        plan->atoms = repalloc(plan->atoms,
                               sizeof(LiquidCompiledAtom) * plan->atom_capacity);
        memset(plan->atoms + old_capacity,
               0,
               sizeof(LiquidCompiledAtom) * (plan->atom_capacity - old_capacity));
    }
    restore_memory_context(oldcxt);
}

static void
ensure_named_arg_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->named_arg_count + additional;
    if (required <= plan->named_arg_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->named_arg_capacity = Max(required,
                                   (plan->named_arg_capacity == 0)
                                       ? 32
                                       : plan->named_arg_capacity * 2);
    if (plan->named_arg_names == NULL)
    {
        plan->named_arg_names = palloc(sizeof(char *) * plan->named_arg_capacity);
        plan->named_arg_ids = palloc(sizeof(int64) * plan->named_arg_capacity);
    }
    else
    {
        plan->named_arg_names = repalloc(plan->named_arg_names,
                                         sizeof(char *) * plan->named_arg_capacity);
        plan->named_arg_ids = repalloc(plan->named_arg_ids,
                                       sizeof(int64) * plan->named_arg_capacity);
    }
    restore_memory_context(oldcxt);
}

static void
ensure_query_atom_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->query_atom_count + additional;
    if (required <= plan->query_atom_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->query_atom_capacity = Max(required,
                                    (plan->query_atom_capacity == 0)
                                        ? 8
                                        : plan->query_atom_capacity * 2);
    if (plan->query_atoms == NULL)
        plan->query_atoms = palloc(sizeof(LiquidCompiledAtom *) *
                                   plan->query_atom_capacity);
    else
        plan->query_atoms = repalloc(plan->query_atoms,
                                     sizeof(LiquidCompiledAtom *) *
                                     plan->query_atom_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_rule_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;
    int           old_capacity;

    if (additional <= 0)
        return;

    required = plan->rule_count + additional;
    if (required <= plan->rule_capacity)
        return;

    old_capacity = plan->rule_capacity;
    switch_to_plan_context(plan, &oldcxt);
    plan->rule_capacity = Max(required,
                              (plan->rule_capacity == 0)
                                  ? 8
                                  : plan->rule_capacity * 2);
    if (plan->rules_storage == NULL)
    {
        plan->rules_storage = palloc0(sizeof(LiquidCompiledRule) * plan->rule_capacity);
        plan->rules = palloc(sizeof(LiquidCompiledRule *) * plan->rule_capacity);
    }
    else
    {
        plan->rules_storage = repalloc(plan->rules_storage,
                                       sizeof(LiquidCompiledRule) * plan->rule_capacity);
        memset(plan->rules_storage + old_capacity,
               0,
               sizeof(LiquidCompiledRule) * (plan->rule_capacity - old_capacity));
        plan->rules = repalloc(plan->rules,
                               sizeof(LiquidCompiledRule *) * plan->rule_capacity);
    }
    restore_memory_context(oldcxt);
}

static void
ensure_rule_body_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->rule_body_atom_count + additional;
    if (required <= plan->rule_body_atom_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->rule_body_atom_capacity = Max(required,
                                        (plan->rule_body_atom_capacity == 0)
                                            ? 32
                                            : plan->rule_body_atom_capacity * 2);
    if (plan->rule_body_atoms == NULL)
        plan->rule_body_atoms = palloc(sizeof(LiquidCompiledAtom *) *
                                       plan->rule_body_atom_capacity);
    else
        plan->rule_body_atoms = repalloc(plan->rule_body_atoms,
                                         sizeof(LiquidCompiledAtom *) *
                                         plan->rule_body_atom_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_recursive_index_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->recursive_atom_index_count + additional;
    if (required <= plan->recursive_atom_index_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->recursive_atom_index_capacity = Max(required,
                                              (plan->recursive_atom_index_capacity == 0)
                                                  ? 16
                                                  : plan->recursive_atom_index_capacity * 2);
    if (plan->recursive_atom_indexes == NULL)
        plan->recursive_atom_indexes = palloc(sizeof(int) *
                                              plan->recursive_atom_index_capacity);
    else
        plan->recursive_atom_indexes = repalloc(plan->recursive_atom_indexes,
                                                sizeof(int) *
                                                plan->recursive_atom_index_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_unresolved_ref_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->unresolved_identity_ref_count + additional;
    if (required <= plan->unresolved_identity_ref_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->unresolved_identity_ref_capacity =
        Max(required,
            (plan->unresolved_identity_ref_capacity == 0)
                ? 32
                : plan->unresolved_identity_ref_capacity * 2);
    if (plan->unresolved_identity_refs == NULL)
        plan->unresolved_identity_refs =
            palloc(sizeof(LiquidPlanUnresolvedIdentityRef) *
                   plan->unresolved_identity_ref_capacity);
    else
        plan->unresolved_identity_refs =
            repalloc(plan->unresolved_identity_refs,
                     sizeof(LiquidPlanUnresolvedIdentityRef) *
                     plan->unresolved_identity_ref_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_preload_literal_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;

    if (additional <= 0)
        return;

    required = plan->preload_literal_count + additional;
    if (required <= plan->preload_literal_capacity)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->preload_literal_capacity = Max(required,
                                         (plan->preload_literal_capacity == 0)
                                             ? 64
                                             : plan->preload_literal_capacity * 2);
    if (plan->preload_literals == NULL)
        plan->preload_literals = palloc(sizeof(char *) * plan->preload_literal_capacity);
    else
        plan->preload_literals = repalloc(plan->preload_literals,
                                          sizeof(char *) * plan->preload_literal_capacity);
    restore_memory_context(oldcxt);
}

static void
ensure_op_capacity(LiquidExecutionPlan *plan, int additional)
{
    MemoryContext oldcxt;
    int           required;
    int           old_capacity;

    if (additional <= 0)
        return;

    required = plan->op_count + additional;
    if (required <= plan->op_capacity)
        return;

    old_capacity = plan->op_capacity;
    switch_to_plan_context(plan, &oldcxt);
    plan->op_capacity = Max(required,
                            (plan->op_capacity == 0)
                                ? 8
                                : plan->op_capacity * 2);
    if (plan->ops == NULL)
        plan->ops = palloc0(sizeof(LiquidAssertionOp) * plan->op_capacity);
    else
    {
        plan->ops = repalloc(plan->ops,
                             sizeof(LiquidAssertionOp) * plan->op_capacity);
        memset(plan->ops + old_capacity,
               0,
               sizeof(LiquidAssertionOp) * (plan->op_capacity - old_capacity));
    }
    restore_memory_context(oldcxt);
}

static LiquidCompiledTerm *
append_term_slots(LiquidExecutionPlan *plan, int count)
{
    LiquidCompiledTerm *terms;

    ensure_terms_capacity(plan, count);
    terms = &plan->terms[plan->term_count];
    plan->term_count += count;
    return terms;
}

static LiquidCompiledAtom *
append_atom_slot(LiquidExecutionPlan *plan)
{
    LiquidCompiledAtom *atom;

    ensure_atoms_capacity(plan, 1);
    atom = &plan->atoms[plan->atom_count];
    memset(atom, 0, sizeof(*atom));
    atom->plan_index = plan->atom_count;
    plan->atom_count++;
    return atom;
}

static LiquidCompiledRule *
append_rule_slot(LiquidExecutionPlan *plan)
{
    LiquidCompiledRule *rule;

    ensure_rule_capacity(plan, 1);
    rule = &plan->rules_storage[plan->rule_count];
    memset(rule, 0, sizeof(*rule));
    plan->rules[plan->rule_count] = rule;
    plan->rule_count++;
    return rule;
}

static LiquidCompiledAtom **
append_rule_body_slots(LiquidExecutionPlan *plan, int count)
{
    LiquidCompiledAtom **atoms;

    ensure_rule_body_capacity(plan, count);
    atoms = &plan->rule_body_atoms[plan->rule_body_atom_count];
    plan->rule_body_atom_count += count;
    return atoms;
}

static int *
append_recursive_index_slots(LiquidExecutionPlan *plan, int count)
{
    int *indexes;

    ensure_recursive_index_capacity(plan, count);
    indexes = &plan->recursive_atom_indexes[plan->recursive_atom_index_count];
    plan->recursive_atom_index_count += count;
    return indexes;
}

static LiquidAssertionOp *
append_assertion_op(LiquidExecutionPlan *plan, LiquidAssertionOpKind kind)
{
    LiquidAssertionOp *op;

    ensure_op_capacity(plan, 1);
    op = &plan->ops[plan->op_count++];
    memset(op, 0, sizeof(*op));
    op->kind = kind;
    return op;
}

static void
append_query_atom(LiquidExecutionPlan *plan, LiquidCompiledAtom *atom)
{
    ensure_query_atom_capacity(plan, 1);
    plan->query_atoms[plan->query_atom_count++] = atom;
}

static void
plan_append_preload_literal(LiquidExecutionPlan *plan, const char *literal)
{
    if (literal == NULL)
        return;

    ensure_preload_literal_capacity(plan, 1);
    plan->preload_literals[plan->preload_literal_count++] = literal;
}

static void
dedupe_preload_literals(LiquidExecutionPlan *plan)
{
    int i;

    if (plan == NULL || plan->preload_literal_count <= 1)
        return;

    if (plan->preload_literal_count <= 32)
    {
        int unique_count = 0;

        for (i = 0; i < plan->preload_literal_count; i++)
        {
            const char *literal = plan->preload_literals[i];
            bool        seen = false;
            int         prior_index;

            if (literal == NULL)
                continue;

            for (prior_index = 0; prior_index < unique_count; prior_index++)
            {
                if (strcmp(plan->preload_literals[prior_index], literal) == 0)
                {
                    seen = true;
                    break;
                }
            }

            if (!seen)
                plan->preload_literals[unique_count++] = literal;
        }

        plan->preload_literal_count = unique_count;
        return;
    }

    {
        LiteralDedupeEntry **buckets;
        LiteralDedupeEntry  *entries;
        int                  bucket_count;
        int                  unique_count = 0;
        int                  entry_count = 0;

        bucket_count = Max(64,
                           Min(16384, plan->preload_literal_count * 2));
        buckets = palloc0(sizeof(LiteralDedupeEntry *) * bucket_count);
        entries = palloc(sizeof(LiteralDedupeEntry) * plan->preload_literal_count);

        for (i = 0; i < plan->preload_literal_count; i++)
        {
            const char        *literal = plan->preload_literals[i];
            uint32             hash;
            int                bucket;
            LiteralDedupeEntry *entry;

            if (literal == NULL)
                continue;

            hash = DatumGetUInt32(hash_any((const unsigned char *) literal,
                                           (int) strlen(literal)));
            bucket = hash % bucket_count;
            entry = buckets[bucket];
            while (entry != NULL)
            {
                if (entry->hash == hash && strcmp(entry->literal, literal) == 0)
                    break;
                entry = entry->next;
            }

            if (entry != NULL)
                continue;

            entry = &entries[entry_count++];
            entry->hash = hash;
            entry->literal = literal;
            entry->next = buckets[bucket];
            buckets[bucket] = entry;
            plan->preload_literals[unique_count++] = literal;
        }

        plan->preload_literal_count = unique_count;
        pfree(entries);
        pfree(buckets);
    }
}

static void
plan_append_unresolved_identity_ref(LiquidExecutionPlan *plan,
                                    int64 *target_id,
                                    const char *literal)
{
    LiquidPlanUnresolvedIdentityRef *ref;

    if (target_id == NULL || literal == NULL)
        return;

    ensure_unresolved_ref_capacity(plan, 1);
    ref = &plan->unresolved_identity_refs[plan->unresolved_identity_ref_count++];
    ref->target_id = target_id;
    ref->literal = literal;
}

static void
collect_compiled_term(LiquidExecutionPlan *plan,
                      LiquidCompiledTerm *term,
                      bool collect_preload,
                      bool collect_unresolved)
{
    if (term->is_var)
        return;

    if (collect_preload && term->literal != NULL)
        plan_append_preload_literal(plan, term->literal);

    if (collect_unresolved &&
        term->id == LIQUID_UNKNOWN_ID &&
        term->literal != NULL)
    {
        plan_append_unresolved_identity_ref(plan, &term->id, term->literal);
    }
}

static void
collect_compiled_atom(LiquidExecutionPlan *plan,
                      LiquidCompiledAtom *atom,
                      bool collect_preload,
                      bool collect_unresolved)
{
    int i;

    if (atom->type == LIQUID_ATOM_COMPOUND)
    {
        if (collect_preload && atom->name != NULL)
            plan_append_preload_literal(plan, atom->name);

        if (collect_unresolved &&
            atom->predicate_id == LIQUID_UNKNOWN_ID &&
            atom->name != NULL)
        {
            plan_append_unresolved_identity_ref(plan, &atom->predicate_id, atom->name);
        }

        for (i = 0; i < atom->num_named_args; i++)
        {
            if (collect_preload && atom->named_arg_names[i] != NULL)
                plan_append_preload_literal(plan, atom->named_arg_names[i]);

            if (collect_unresolved &&
                atom->named_arg_ids[i] == LIQUID_UNKNOWN_ID &&
                atom->named_arg_names[i] != NULL)
            {
                plan_append_unresolved_identity_ref(plan,
                                                    &atom->named_arg_ids[i],
                                                    atom->named_arg_names[i]);
            }
        }
    }
    else if (collect_unresolved &&
             atom->type == LIQUID_ATOM_PREDICATE &&
             atom->predicate_id == LIQUID_UNKNOWN_ID &&
             atom->name != NULL)
    {
        plan_append_unresolved_identity_ref(plan, &atom->predicate_id, atom->name);
    }

    for (i = 0; i < atom->num_terms; i++)
        collect_compiled_term(plan, &atom->terms[i], collect_preload, collect_unresolved);
}

static LiquidCompiledAtom *
compile_atom(LiquidExecutionPlan *plan,
             LiquidAtom *source,
             bool collect_preload,
             bool collect_unresolved)
{
    LiquidCompiledAtom *atom;
    int                 i;

    atom = append_atom_slot(plan);
    atom->type = source->type;
    atom->name = source->name;
    atom->predicate_id = source->predicate_id;
    atom->predicate_index = -1;
    atom->statically_is_idb = false;
    atom->num_terms = source->num_terms;
    if (atom->num_terms > 0)
    {
        atom->terms = append_term_slots(plan, atom->num_terms);
        for (i = 0; i < atom->num_terms; i++)
        {
            atom->terms[i].id = source->terms[i].id;
            atom->terms[i].literal = source->terms[i].literal;
            atom->terms[i].is_var = source->terms[i].is_var;
            atom->terms[i].var_idx = source->terms[i].var_idx;
            atom->terms[i].var_name = source->terms[i].var_name;
        }
    }

    atom->num_named_args = source->num_named_args;
    if (atom->num_named_args > 0)
    {
        int offset;

        ensure_named_arg_capacity(plan, atom->num_named_args);
        offset = plan->named_arg_count;
        atom->named_arg_names = &plan->named_arg_names[offset];
        atom->named_arg_ids = &plan->named_arg_ids[offset];
        for (i = 0; i < atom->num_named_args; i++)
        {
            atom->named_arg_names[i] = source->named_arg_names[i];
            atom->named_arg_ids[i] = source->named_arg_ids[i];
        }
        plan->named_arg_count += atom->num_named_args;
    }

    collect_compiled_atom(plan, atom, collect_preload, collect_unresolved);
    return atom;
}

static int
compute_compiled_atom_span_num_vars(LiquidCompiledAtom **atoms, int atom_count)
{
    int max_var_idx = -1;
    int atom_index;

    for (atom_index = 0; atom_index < atom_count; atom_index++)
    {
        LiquidCompiledAtom *atom = atoms[atom_index];
        int                 term_index;

        for (term_index = 0; term_index < atom->num_terms; term_index++)
        {
            if (atom->terms[term_index].is_var &&
                atom->terms[term_index].var_idx > max_var_idx)
            {
                max_var_idx = atom->terms[term_index].var_idx;
            }
        }
    }

    if (max_var_idx + 1 > MAX_DATALOG_VARS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many variables in compiled atom span; max is %d",
                        MAX_DATALOG_VARS)));

    return max_var_idx + 1;
}

static bool
is_direct_edge_literal_atom(LiquidAtom *atom)
{
    return atom->type == LIQUID_ATOM_EDGE &&
           atom->num_terms == 3 &&
           !atom->terms[0].is_var &&
           !atom->terms[1].is_var &&
           !atom->terms[2].is_var &&
           atom->terms[0].literal != NULL &&
           atom->terms[1].literal != NULL &&
           atom->terms[2].literal != NULL;
}

static LiquidAssertionOpKind
classify_single_atom_clause(LiquidAtom *atom)
{
    if (is_direct_edge_literal_atom(atom))
        return LIQUID_ASSERTION_OP_EDGE_BATCH;

    if (atom->type == LIQUID_ATOM_COMPOUND)
        return LIQUID_ASSERTION_OP_INSERT_COMPOUND;

    if (atom->type == LIQUID_ATOM_PREDICATE && atom->name != NULL)
    {
        if (strcmp(atom->name, "DefPred") == 0)
            return LIQUID_ASSERTION_OP_DEFINE_PRED;
        if (strcmp(atom->name, "DefCompound") == 0)
            return LIQUID_ASSERTION_OP_DEFINE_COMPOUND;
    }

    return LIQUID_ASSERTION_OP_CLAUSE_FALLBACK;
}

static bool
term_has_literal(LiquidTerm *term)
{
    return term != NULL && !term->is_var && term->literal != NULL;
}

static void
append_compound_identity_preload(LiquidExecutionPlan *plan,
                                 const char *compound_type,
                                 int num_roles,
                                 const char *const *role_names,
                                 const char *const *role_values)
{
    char          *identity_literal;
    MemoryContext  oldcxt;

    if (plan == NULL || compound_type == NULL || num_roles <= 0)
        return;

    switch_to_plan_context(plan, &oldcxt);
    identity_literal = build_compound_identity_literal(compound_type,
                                                       num_roles,
                                                       role_names,
                                                       role_values);
    restore_memory_context(oldcxt);
    plan_append_preload_literal(plan, identity_literal);
}

static void
maybe_append_assertion_identity_preloads(LiquidExecutionPlan *plan,
                                         LiquidAtom *atom,
                                         LiquidAssertionOpKind kind)
{
    if (plan == NULL || atom == NULL)
        return;

    if (kind == LIQUID_ASSERTION_OP_DEFINE_PRED)
    {
        static const char *const subject_role_names[1] = {"liquid/subject_meta"};
        static const char *const object_role_names[1] = {"liquid/object_meta"};
        const char               *role_values[1];

        if (atom->num_terms < 1 || !term_has_literal(&atom->terms[0]))
            return;

        role_values[0] = atom->terms[0].literal;
        append_compound_identity_preload(plan,
                                         "Smeta",
                                         1,
                                         subject_role_names,
                                         role_values);
        append_compound_identity_preload(plan,
                                         "Ometa",
                                         1,
                                         object_role_names,
                                         role_values);
        return;
    }

    if (kind == LIQUID_ASSERTION_OP_DEFINE_COMPOUND)
    {
        static const char *const subject_role_names[1] = {"liquid/subject_meta"};
        static const char *const object_role_names[1] = {"liquid/object_meta"};
        const char               *role_values[1];

        if (atom->num_terms < 2 || !term_has_literal(&atom->terms[1]))
            return;

        role_values[0] = atom->terms[1].literal;
        append_compound_identity_preload(plan,
                                         "Smeta",
                                         1,
                                         subject_role_names,
                                         role_values);
        append_compound_identity_preload(plan,
                                         "Ometa",
                                         1,
                                         object_role_names,
                                         role_values);
        return;
    }

    if (kind == LIQUID_ASSERTION_OP_INSERT_COMPOUND)
    {
        const char *role_values[MAX_COMPOUND_ARGS];
        int         i;

        if (atom->name == NULL || atom->num_named_args <= 0)
            return;

        for (i = 0; i < atom->num_named_args; i++)
        {
            if (i + 1 >= atom->num_terms || !term_has_literal(&atom->terms[i + 1]))
                return;
            role_values[i] = atom->terms[i + 1].literal;
        }

        append_compound_identity_preload(plan,
                                         atom->name,
                                         atom->num_named_args,
                                         (const char *const *) atom->named_arg_names,
                                         role_values);
    }
}

static void
edge_batch_op_add_literals(LiquidExecutionPlan *plan,
                           LiquidAssertionOp *op,
                           const char *subject_literal,
                           const char *predicate_literal,
                           const char *object_literal)
{
    MemoryContext           oldcxt;
    LiquidEdgeLiteralTriple *edge;

    if (plan->edge_literals != NULL &&
        plan->edge_literal_count < plan->edge_literal_capacity)
    {
        if (op->data.edge_batch.count == 0)
            op->data.edge_batch.edges = &plan->edge_literals[plan->edge_literal_count];
        edge = &plan->edge_literals[plan->edge_literal_count++];
        op->data.edge_batch.count++;
    }
    else if (op->data.edge_batch.count >= op->data.edge_batch.capacity)
    {
        switch_to_plan_context(plan, &oldcxt);
        op->data.edge_batch.capacity = (op->data.edge_batch.capacity == 0)
            ? 32
            : op->data.edge_batch.capacity * 2;
        if (op->data.edge_batch.edges == NULL)
            op->data.edge_batch.edges = palloc(sizeof(LiquidEdgeLiteralTriple) *
                                               op->data.edge_batch.capacity);
        else
            op->data.edge_batch.edges = repalloc(op->data.edge_batch.edges,
                                                 sizeof(LiquidEdgeLiteralTriple) *
                                                 op->data.edge_batch.capacity);
        restore_memory_context(oldcxt);
        edge = &op->data.edge_batch.edges[op->data.edge_batch.count++];
    }
    else
    {
        edge = &op->data.edge_batch.edges[op->data.edge_batch.count++];
    }

    edge->subject_literal = subject_literal;
    edge->predicate_literal = predicate_literal;
    edge->object_literal = object_literal;
    plan->direct_edge_capacity_hint++;

    plan_append_preload_literal(plan, subject_literal);
    plan_append_preload_literal(plan, predicate_literal);
    plan_append_preload_literal(plan, object_literal);
}

static void
edge_batch_op_add(LiquidExecutionPlan *plan,
                  LiquidAssertionOp *op,
                  LiquidAtom *atom)
{
    edge_batch_op_add_literals(plan,
                               op,
                               atom->terms[0].literal,
                               atom->terms[1].literal,
                               atom->terms[2].literal);
}

static int
predicate_index_for_id(PredicateSccInfo *info, int64 predicate_id)
{
    Int64IndexEntry *entry;

    if (info == NULL || info->pred_index == NULL)
        return -1;

    entry = hash_search(info->pred_index, &predicate_id, HASH_FIND, NULL);
    return entry ? entry->index : -1;
}

static void
predicate_graph_dfs_order(PredicateSccInfo *info,
                          int node_idx,
                          bool *visited,
                          int *order,
                          int *order_count)
{
    int next_idx;

    visited[node_idx] = true;

    for (next_idx = 0; next_idx < info->num_preds; next_idx++)
    {
        if (!info->adjacency[node_idx * info->num_preds + next_idx] ||
            visited[next_idx])
            continue;
        predicate_graph_dfs_order(info, next_idx, visited, order, order_count);
    }

    order[(*order_count)++] = node_idx;
}

static void
predicate_graph_dfs_assign(PredicateSccInfo *info, int node_idx, int scc_id, bool *visited)
{
    int prev_idx;

    visited[node_idx] = true;
    info->scc_ids[node_idx] = scc_id;

    for (prev_idx = 0; prev_idx < info->num_preds; prev_idx++)
    {
        if (!info->adjacency[prev_idx * info->num_preds + node_idx] ||
            visited[prev_idx])
            continue;
        predicate_graph_dfs_assign(info, prev_idx, scc_id, visited);
    }
}

static PredicateSccInfo
build_predicate_scc_info(LiquidExecutionPlan *plan)
{
    PredicateSccInfo info;
    HASHCTL          ctl;
    HASHCTL          seen_ctl;
    HTAB            *seen_predicates;
    bool            *visited;
    int             *order;
    int              order_count = 0;
    int              i;

    memset(&info, 0, sizeof(info));

    memset(&seen_ctl, 0, sizeof(seen_ctl));
    seen_ctl.keysize = sizeof(int64);
    seen_ctl.entrysize = sizeof(Int64IndexEntry);
    seen_ctl.hcxt = plan->mcxt;
    seen_predicates = hash_create("SccHeadPredicates", 64, &seen_ctl,
                                  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    for (i = 0; i < plan->rule_count; i++)
    {
        LiquidCompiledRule *rule = plan->rules[i];
        Int64IndexEntry    *seen_entry;
        bool                seen_found;

        seen_entry = hash_search(seen_predicates,
                                 &rule->head->predicate_id,
                                 HASH_ENTER,
                                 &seen_found);
        if (seen_found)
            continue;
        seen_entry->key = rule->head->predicate_id;

        info.pred_ids = info.num_preds == 0
            ? MemoryContextAlloc(plan->mcxt, sizeof(int64))
            : repalloc(info.pred_ids, (info.num_preds + 1) * sizeof(int64));
        info.pred_ids[info.num_preds++] = rule->head->predicate_id;
    }

    hash_destroy(seen_predicates);

    if (info.num_preds == 0)
        return info;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(Int64IndexEntry);
    ctl.hcxt = plan->mcxt;
    info.pred_index = hash_create("PredicateIndex", info.num_preds * 2 + 1, &ctl,
                                  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    for (i = 0; i < info.num_preds; i++)
    {
        Int64IndexEntry *entry;
        bool             found;

        entry = hash_search(info.pred_index, &info.pred_ids[i], HASH_ENTER, &found);
        entry->key = info.pred_ids[i];
        entry->index = i;
    }

    info.adjacency = MemoryContextAllocZero(plan->mcxt,
                                            info.num_preds * info.num_preds * sizeof(bool));
    info.scc_ids = MemoryContextAllocZero(plan->mcxt, info.num_preds * sizeof(int));

    for (i = 0; i < plan->rule_count; i++)
    {
        LiquidCompiledRule *rule = plan->rules[i];
        int                 head_idx = predicate_index_for_id(&info,
                                                              rule->head->predicate_id);
        int                 body_index;

        for (body_index = 0; body_index < rule->body_count; body_index++)
        {
            LiquidCompiledAtom *atom = rule->body_atoms[body_index];
            int                 body_idx;

            if (atom->type != LIQUID_ATOM_PREDICATE)
                continue;

            body_idx = predicate_index_for_id(&info, atom->predicate_id);
            if (body_idx >= 0)
                info.adjacency[body_idx * info.num_preds + head_idx] = true;
        }
    }

    visited = MemoryContextAllocZero(plan->mcxt, info.num_preds * sizeof(bool));
    order = MemoryContextAlloc(plan->mcxt, info.num_preds * sizeof(int));

    for (i = 0; i < info.num_preds; i++)
    {
        if (!visited[i])
            predicate_graph_dfs_order(&info, i, visited, order, &order_count);
    }

    memset(visited, 0, info.num_preds * sizeof(bool));

    for (i = info.num_preds - 1; i >= 0; i--)
    {
        int node_idx = order[i];

        if (visited[node_idx])
            continue;

        predicate_graph_dfs_assign(&info, node_idx, info.num_sccs, visited);
        info.num_sccs++;
    }

    info.scc_sizes = MemoryContextAllocZero(plan->mcxt, info.num_sccs * sizeof(int));
    info.scc_self_recursive = MemoryContextAllocZero(plan->mcxt,
                                                     info.num_sccs * sizeof(bool));
    info.strata = MemoryContextAllocZero(plan->mcxt, info.num_sccs * sizeof(int));

    for (i = 0; i < info.num_preds; i++)
    {
        int scc_id = info.scc_ids[i];
        int j;

        info.scc_sizes[scc_id]++;
        if (info.adjacency[i * info.num_preds + i])
            info.scc_self_recursive[scc_id] = true;

        for (j = 0; j < info.num_preds; j++)
        {
            int target_scc = info.scc_ids[j];

            if (!info.adjacency[i * info.num_preds + j] || scc_id == target_scc)
                continue;
            if (info.strata[target_scc] < info.strata[scc_id] + 1)
                info.strata[target_scc] = info.strata[scc_id] + 1;
        }
    }

    {
        bool changed;

        do {
            changed = false;
            for (i = 0; i < info.num_preds; i++)
            {
                int scc_id = info.scc_ids[i];
                int j;

                for (j = 0; j < info.num_preds; j++)
                {
                    int target_scc = info.scc_ids[j];

                    if (!info.adjacency[i * info.num_preds + j] ||
                        scc_id == target_scc)
                        continue;
                    if (info.strata[target_scc] < info.strata[scc_id] + 1)
                    {
                        info.strata[target_scc] = info.strata[scc_id] + 1;
                        changed = true;
                    }
                }
            }
        } while (changed);
    }

    return info;
}

static void
free_predicate_scc_info(PredicateSccInfo *info)
{
    if (info->pred_index != NULL)
        hash_destroy(info->pred_index);
}

static bool
rule_uses_single_edge_body(LiquidCompiledRule *rule)
{
    return rule->body_count == 1 &&
           rule->body_atoms[0]->type == LIQUID_ATOM_EDGE;
}

static bool
rule_uses_two_predicate_body(LiquidCompiledRule *rule)
{
    return rule->body_count == 2 &&
           rule->body_atoms[0]->type == LIQUID_ATOM_PREDICATE &&
           rule->body_atoms[1]->type == LIQUID_ATOM_PREDICATE;
}

static bool
rule_uses_predicate_edge_body(LiquidCompiledRule *rule)
{
    if (rule->body_count != 2)
        return false;

    return (rule->body_atoms[0]->type == LIQUID_ATOM_PREDICATE &&
            rule->body_atoms[1]->type == LIQUID_ATOM_EDGE) ||
           (rule->body_atoms[0]->type == LIQUID_ATOM_EDGE &&
            rule->body_atoms[1]->type == LIQUID_ATOM_PREDICATE);
}

static void
copy_predicate_metadata_to_plan(LiquidExecutionPlan *plan, PredicateSccInfo *info)
{
    MemoryContext oldcxt;
    int           predicate_index;
    int           scc_index;
    int           dependency_capacity = 0;
    int           dependency_write_index = 0;

    plan->predicate_ids = info->pred_ids;
    plan->predicate_count = info->num_preds;
    plan->predicate_scc_ids = info->scc_ids;
    plan->scc_count = info->num_sccs;
    plan->scc_sizes = info->scc_sizes;
    plan->scc_self_recursive = info->scc_self_recursive;
    plan->scc_strata = info->strata;

    info->pred_ids = NULL;
    info->scc_ids = NULL;
    info->scc_sizes = NULL;
    info->scc_self_recursive = NULL;
    info->strata = NULL;

    if (plan->predicate_count == 0)
        return;

    for (predicate_index = 0; predicate_index < plan->predicate_count; predicate_index++)
    {
        int body_index;

        for (body_index = 0; body_index < plan->predicate_count; body_index++)
        {
            if (info->adjacency[body_index * plan->predicate_count + predicate_index])
                dependency_capacity++;
        }
    }

    switch_to_plan_context(plan, &oldcxt);
    plan->predicate_dependency_offsets =
        palloc0(sizeof(int) * (plan->predicate_count + 1));
    if (dependency_capacity > 0)
        plan->predicate_dependency_indexes = palloc(sizeof(int) * dependency_capacity);
    plan->predicate_dependency_count = dependency_capacity;
    if (plan->scc_count > 0)
    {
        plan->scc_order = palloc(sizeof(int) * plan->scc_count);
        plan->scc_rule_offsets = palloc0(sizeof(int) * (plan->scc_count + 1));
        if (plan->rule_count > 0)
            plan->scc_rule_indexes = palloc(sizeof(int) * plan->rule_count);
    }
    restore_memory_context(oldcxt);

    for (predicate_index = 0; predicate_index < plan->predicate_count; predicate_index++)
    {
        int body_index;

        plan->predicate_dependency_offsets[predicate_index] = dependency_write_index;
        for (body_index = 0; body_index < plan->predicate_count; body_index++)
        {
            if (!info->adjacency[body_index * plan->predicate_count + predicate_index])
                continue;
            plan->predicate_dependency_indexes[dependency_write_index++] = body_index;
        }
    }
    plan->predicate_dependency_offsets[plan->predicate_count] = dependency_write_index;

    if (plan->scc_count > 0)
    {
        int *scc_rule_next;
        int  rule_index;

        for (scc_index = 0; scc_index < plan->scc_count; scc_index++)
            plan->scc_order[scc_index] = scc_index;

        for (scc_index = 1; scc_index < plan->scc_count; scc_index++)
        {
            int key = plan->scc_order[scc_index];
            int key_stratum = plan->scc_strata[key];
            int insert_index = scc_index - 1;

            while (insert_index >= 0 &&
                   plan->scc_strata[plan->scc_order[insert_index]] > key_stratum)
            {
                plan->scc_order[insert_index + 1] = plan->scc_order[insert_index];
                insert_index--;
            }
            plan->scc_order[insert_index + 1] = key;
        }

        for (rule_index = 0; rule_index < plan->rule_count; rule_index++)
        {
            int head_scc = plan->rules[rule_index]->head_scc_id;

            if (head_scc >= 0)
                plan->scc_rule_offsets[head_scc + 1]++;
        }

        for (scc_index = 1; scc_index <= plan->scc_count; scc_index++)
            plan->scc_rule_offsets[scc_index] += plan->scc_rule_offsets[scc_index - 1];

        if (plan->rule_count > 0)
        {
            switch_to_plan_context(plan, &oldcxt);
            scc_rule_next = palloc(sizeof(int) * plan->scc_count);
            restore_memory_context(oldcxt);
            memcpy(scc_rule_next,
                   plan->scc_rule_offsets,
                   sizeof(int) * plan->scc_count);

            for (rule_index = 0; rule_index < plan->rule_count; rule_index++)
            {
                int head_scc = plan->rules[rule_index]->head_scc_id;

                if (head_scc < 0)
                    continue;

                plan->scc_rule_indexes[scc_rule_next[head_scc]++] = rule_index;
            }

            pfree(scc_rule_next);
        }
    }
}

static void
mark_plan_idb_atoms(LiquidExecutionPlan *plan, PredicateSccInfo *info)
{
    int atom_index;

    for (atom_index = 0; atom_index < plan->atom_count; atom_index++)
    {
        LiquidCompiledAtom *atom = &plan->atoms[atom_index];

        atom->predicate_index = (atom->type == LIQUID_ATOM_PREDICATE)
            ? predicate_index_for_id(info, atom->predicate_id)
            : -1;
        atom->statically_is_idb =
            atom->type == LIQUID_ATOM_PREDICATE &&
            atom->predicate_index >= 0;
    }
}

static void
build_query_predicate_indexes(LiquidExecutionPlan *plan)
{
    MemoryContext oldcxt;
    int           query_index;
    int           predicate_count = 0;

    for (query_index = 0; query_index < plan->query_atom_count; query_index++)
    {
        LiquidCompiledAtom *atom = plan->query_atoms[query_index];

        if (atom->type == LIQUID_ATOM_PREDICATE && atom->predicate_index >= 0)
            predicate_count++;
    }

    plan->query_predicate_index_count = predicate_count;
    if (predicate_count == 0)
        return;

    switch_to_plan_context(plan, &oldcxt);
    plan->query_predicate_indexes = palloc(sizeof(int) * predicate_count);
    restore_memory_context(oldcxt);

    predicate_count = 0;
    for (query_index = 0; query_index < plan->query_atom_count; query_index++)
    {
        LiquidCompiledAtom *atom = plan->query_atoms[query_index];

        if (atom->type == LIQUID_ATOM_PREDICATE && atom->predicate_index >= 0)
            plan->query_predicate_indexes[predicate_count++] = atom->predicate_index;
    }
}

static bool
binary_tc_order_matches(LiquidCompiledAtom *head,
                        LiquidCompiledAtom *first,
                        LiquidCompiledAtom *second)
{
    if (head->num_terms != 2 ||
        first->num_terms != 2 ||
        second->num_terms != 2)
        return false;

    if (!head->terms[0].is_var ||
        !head->terms[1].is_var ||
        !first->terms[0].is_var ||
        !first->terms[1].is_var ||
        !second->terms[0].is_var ||
        !second->terms[1].is_var)
        return false;

    return first->terms[0].var_idx == head->terms[0].var_idx &&
           second->terms[1].var_idx == head->terms[1].var_idx &&
           first->terms[1].var_idx == second->terms[0].var_idx &&
           first->terms[0].var_idx != first->terms[1].var_idx &&
           first->terms[1].var_idx != second->terms[1].var_idx;
}

static bool
rule_is_binary_transitive_closure(LiquidCompiledRule *rule)
{
    LiquidCompiledAtom *first;
    LiquidCompiledAtom *second;

    if (rule->head->num_terms != 2 || rule->body_count != 2)
        return false;

    first = rule->body_atoms[0];
    second = rule->body_atoms[1];

    if (first->type != LIQUID_ATOM_PREDICATE ||
        second->type != LIQUID_ATOM_PREDICATE ||
        first->predicate_id != rule->head->predicate_id ||
        second->predicate_id != rule->head->predicate_id)
        return false;

    return binary_tc_order_matches(rule->head, first, second) ||
           binary_tc_order_matches(rule->head, second, first);
}

static void
find_transitive_closure_rule(LiquidExecutionPlan *plan)
{
    int rule_index;

    memset(&plan->transitive_closure_plan, 0, sizeof(plan->transitive_closure_plan));

    for (rule_index = 0; rule_index < plan->rule_count; rule_index++)
    {
        LiquidCompiledRule *rule = plan->rules[rule_index];
        int                 candidate_index;
        int                 recursive_rule_count = 0;

        if (!rule->is_recursive || !rule_is_binary_transitive_closure(rule))
            continue;

        if (rule->head_scc_id < 0 ||
            plan->scc_sizes[rule->head_scc_id] != 1 ||
            !plan->scc_self_recursive[rule->head_scc_id])
        {
            continue;
        }

        for (candidate_index = 0; candidate_index < plan->rule_count; candidate_index++)
        {
            LiquidCompiledRule *candidate = plan->rules[candidate_index];

            if (candidate->head_predicate_index != rule->head_predicate_index)
                continue;

            if (candidate->is_recursive)
            {
                recursive_rule_count++;
                if (candidate != rule)
                {
                    recursive_rule_count = -1;
                    break;
                }
            }
            else if (candidate->has_idb_body)
            {
                recursive_rule_count = -1;
                break;
            }
        }

        if (recursive_rule_count != 1)
            continue;

        plan->transitive_closure_plan.valid = true;
        plan->transitive_closure_plan.predicate_id = rule->head->predicate_id;
        plan->transitive_closure_plan.rule_index = rule_index;
        plan->transitive_closure_plan.scc_id = rule->head_scc_id;
        return;
    }
}

static void
finalize_rule_metadata(LiquidExecutionPlan *plan)
{
    PredicateSccInfo info;
    int              rule_index;

    info = build_predicate_scc_info(plan);
    mark_plan_idb_atoms(plan, &info);

    for (rule_index = 0; rule_index < plan->rule_count; rule_index++)
    {
        LiquidCompiledRule *rule = plan->rules[rule_index];
        int                 head_idx = predicate_index_for_id(&info,
                                                              rule->head->predicate_id);
        int                 head_scc = (head_idx >= 0) ? info.scc_ids[head_idx] : -1;
        bool                recursive_scc = (head_scc >= 0) &&
            (info.scc_sizes[head_scc] > 1 || info.scc_self_recursive[head_scc]);
        int                 body_index;
        int                 recursive_count = 0;

        rule->head_predicate_index = head_idx;
        rule->head_scc_id = head_scc;
        rule->has_idb_body = false;
        rule->is_recursive = false;
        rule->recursive_atom_count = 0;
        rule->recursive_atom_indexes = NULL;
        rule->execution_kind = LIQUID_RULE_EXEC_GENERIC;

        for (body_index = 0; body_index < rule->body_count; body_index++)
        {
            LiquidCompiledAtom *atom = rule->body_atoms[body_index];

            if (atom->type != LIQUID_ATOM_PREDICATE)
                continue;

            if (atom->statically_is_idb)
                rule->has_idb_body = true;

            if (!recursive_scc || atom->predicate_index < 0)
                continue;

            if (info.scc_ids[atom->predicate_index] == head_scc)
                recursive_count++;
        }

        if (recursive_count > 0)
        {
            int *recursive_indexes = append_recursive_index_slots(plan, recursive_count);
            int  write_index = 0;

            for (body_index = 0; body_index < rule->body_count; body_index++)
            {
                LiquidCompiledAtom *atom = rule->body_atoms[body_index];

                if (atom->type != LIQUID_ATOM_PREDICATE || atom->predicate_index < 0)
                    continue;

                if (info.scc_ids[atom->predicate_index] == head_scc)
                    recursive_indexes[write_index++] = body_index;
            }

            rule->recursive_atom_count = recursive_count;
            rule->recursive_atom_indexes = recursive_indexes;
            rule->is_recursive = true;
        }

        if (rule_uses_single_edge_body(rule))
            rule->execution_kind = LIQUID_RULE_EXEC_SINGLE_EDGE;
        else if (rule_uses_predicate_edge_body(rule))
            rule->execution_kind = LIQUID_RULE_EXEC_PREDICATE_EDGE;
        else if (rule_uses_two_predicate_body(rule))
            rule->execution_kind = LIQUID_RULE_EXEC_TWO_PREDICATES;
    }

    copy_predicate_metadata_to_plan(plan, &info);
    build_query_predicate_indexes(plan);
    find_transitive_closure_rule(plan);
    free_predicate_scc_info(&info);
}

static void
compile_assertion_clause(LiquidExecutionPlan *plan,
                         LiquidAssertionClause *clause,
                         LiquidAssertionOp **current_edge_batch)
{
    if (clause->direct_edge_count > 0)
    {
        int edge_index;

        if (*current_edge_batch == NULL)
            *current_edge_batch = append_assertion_op(plan,
                                                      LIQUID_ASSERTION_OP_EDGE_BATCH);

        for (edge_index = 0; edge_index < clause->direct_edge_count; edge_index++)
        {
            LiquidEdgeLiteralTriple *edge = &clause->direct_edges[edge_index];

            edge_batch_op_add_literals(plan,
                                       *current_edge_batch,
                                       edge->subject_literal,
                                       edge->predicate_literal,
                                       edge->object_literal);
        }
        return;
    }

    if (list_length(clause->atoms) == 1)
    {
        LiquidAtom            *atom = (LiquidAtom *) linitial(clause->atoms);
        LiquidAssertionOpKind  kind = classify_single_atom_clause(atom);

        if (kind == LIQUID_ASSERTION_OP_EDGE_BATCH)
        {
            if (*current_edge_batch == NULL)
                *current_edge_batch = append_assertion_op(plan,
                                                          LIQUID_ASSERTION_OP_EDGE_BATCH);

            edge_batch_op_add(plan, *current_edge_batch, atom);
            return;
        }

        *current_edge_batch = NULL;

        if (kind == LIQUID_ASSERTION_OP_DEFINE_PRED ||
            kind == LIQUID_ASSERTION_OP_DEFINE_COMPOUND ||
            kind == LIQUID_ASSERTION_OP_INSERT_COMPOUND)
        {
            LiquidAssertionOp *op = append_assertion_op(plan, kind);

            op->data.atom = compile_atom(plan, atom, true, false);
            maybe_append_assertion_identity_preloads(plan, atom, kind);
            return;
        }
    }

    *current_edge_batch = NULL;
    {
        LiquidAssertionOp *op = append_assertion_op(plan, LIQUID_ASSERTION_OP_CLAUSE_FALLBACK);
        ListCell          *lc;
        int                atom_count = list_length(clause->atoms);
        int                atom_index = 0;
        MemoryContext      oldcxt;

        switch_to_plan_context(plan, &oldcxt);
        op->data.clause.atom_count = atom_count;
        op->data.clause.atoms = palloc(sizeof(LiquidCompiledAtom *) * atom_count);
        restore_memory_context(oldcxt);

        foreach(lc, clause->atoms)
        {
            op->data.clause.atoms[atom_index++] =
                compile_atom(plan, (LiquidAtom *) lfirst(lc), true, false);
        }
    }
}

static void
compile_query_atoms(LiquidExecutionPlan *plan, LiquidProgram *program)
{
    ListCell *lc;

    plan->query_num_vars = program->query_num_vars;
    plan->query_num_output_vars = program->query_num_output_vars;
    memcpy(plan->query_output_var_idxs,
           program->query_output_var_idxs,
           sizeof(program->query_output_var_idxs));

    foreach(lc, program->query_atoms)
        append_query_atom(plan,
                          compile_atom(plan, (LiquidAtom *) lfirst(lc), true, true));
}

static void
compile_rules(LiquidExecutionPlan *plan, LiquidProgram *program)
{
    ListCell *lc;

    foreach(lc, program->rules)
    {
        LiquidRule         *source_rule = (LiquidRule *) lfirst(lc);
        LiquidCompiledRule *compiled_rule = append_rule_slot(plan);
        ListCell           *body_lc;
        int                 body_count = list_length(source_rule->body);
        int                 body_index = 0;
        LiquidCompiledAtom **num_var_atoms;
        int                 num_var_atom_count;
        MemoryContext       oldcxt;

        compiled_rule->head = compile_atom(plan, source_rule->head, true, true);
        compiled_rule->body_count = body_count;
        compiled_rule->body_atoms = append_rule_body_slots(plan, body_count);

        foreach(body_lc, source_rule->body)
        {
            compiled_rule->body_atoms[body_index++] =
                compile_atom(plan, (LiquidAtom *) lfirst(body_lc), true, true);
        }

        num_var_atom_count = body_count + 1;
        switch_to_plan_context(plan, &oldcxt);
        num_var_atoms = palloc(sizeof(LiquidCompiledAtom *) * num_var_atom_count);
        restore_memory_context(oldcxt);

        num_var_atoms[0] = compiled_rule->head;
        for (body_index = 0; body_index < body_count; body_index++)
            num_var_atoms[body_index + 1] = compiled_rule->body_atoms[body_index];
        compiled_rule->num_vars = compute_compiled_atom_span_num_vars(num_var_atoms,
                                                                      num_var_atom_count);
        pfree(num_var_atoms);
    }
}

LiquidExecutionPlan *
compile_liquid_execution_plan(LiquidProgram *program, MemoryContext mcxt)
{
    LiquidExecutionPlan *plan;
    LiquidAssertionOp   *current_edge_batch = NULL;
    ListCell            *lc;
    MemoryContext        oldcxt;

    oldcxt = MemoryContextSwitchTo(mcxt);
    plan = palloc0(sizeof(LiquidExecutionPlan));
    plan->mcxt = mcxt;
    MemoryContextSwitchTo(oldcxt);

    if (program == NULL)
        return plan;

    reserve_plan_storage(plan, program);
    plan->has_assertions = (program->assertions != NIL);

    foreach(lc, program->assertions)
        compile_assertion_clause(plan,
                                 (LiquidAssertionClause *) lfirst(lc),
                                 &current_edge_batch);

    compile_query_atoms(plan, program);
    if (query_uses_predicates(program))
    {
        compile_rules(plan, program);
        finalize_rule_metadata(plan);
    }
    dedupe_preload_literals(plan);

    return plan;
}

void
finalize_execution_plan_ids(LiquidExecutionPlan *plan)
{
    int i;

    if (plan == NULL || plan->unresolved_identity_ref_count == 0)
        return;

    for (i = 0; i < plan->unresolved_identity_ref_count; i++)
    {
        LiquidPlanUnresolvedIdentityRef *ref = &plan->unresolved_identity_refs[i];

        if (*ref->target_id != LIQUID_UNKNOWN_ID || ref->literal == NULL)
            continue;

        {
            int64 resolved = get_or_create_identity(ref->literal);

            if (resolved >= 0)
                *ref->target_id = resolved;
        }
    }
}
