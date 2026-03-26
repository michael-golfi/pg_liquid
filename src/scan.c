#include "postgres.h"
#include "pg_liquid.h"
#include "utils/fmgroids.h"
#include "executor/spi.h"
#include "utils/builtins.h"

typedef struct Int64GrantEntry
{
    int64 key;
    char  status;
} Int64GrantEntry;

typedef struct TripleGrantEntry
{
    TripleGrantKey key;
    char           status;
} TripleGrantEntry;

typedef struct Int64PairKey
{
    int64 left_id;
    int64 right_id;
} Int64PairKey;

typedef struct Int64PairEntry
{
    Int64PairKey key;
    char         status;
} Int64PairEntry;

static SPIPlanPtr scan_plan_cache[8] = {NULL};
static bool       scan_plans_initialized = false;

typedef struct ScanFactsListContext
{
    List *results;
} ScanFactsListContext;

static void append_scan_facts_row(int64 s_id, int64 p_id, int64 o_id, void *ctx_ptr);

static HTAB *
create_int64_grant_htab(MemoryContext mcxt, const char *name)
{
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(int64);
    ctl.entrysize = sizeof(Int64GrantEntry);
    ctl.hcxt = mcxt;

    return hash_create(name, 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
create_triple_grant_htab(MemoryContext mcxt, const char *name)
{
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(TripleGrantKey);
    ctl.entrysize = sizeof(TripleGrantEntry);
    ctl.hcxt = mcxt;

    return hash_create(name, 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
create_int64_pair_htab(MemoryContext mcxt, const char *name)
{
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(Int64PairKey);
    ctl.entrysize = sizeof(Int64PairEntry);
    ctl.hcxt = mcxt;

    return hash_create(name, 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
htab_contains_int64(HTAB *htab, int64 key)
{
    return htab != NULL && hash_search(htab, &key, HASH_FIND, NULL) != NULL;
}

static bool
htab_contains_triple(HTAB *htab, int64 s_id, int64 p_id, int64 o_id)
{
    TripleGrantKey key;

    if (htab == NULL)
        return false;

    key.subject_id = s_id;
    key.predicate_id = p_id;
    key.object_id = o_id;
    return hash_search(htab, &key, HASH_FIND, NULL) != NULL;
}

static bool
htab_contains_pair(HTAB *htab, int64 left_id, int64 right_id)
{
    Int64PairKey key;

    if (htab == NULL)
        return false;

    key.left_id = left_id;
    key.right_id = right_id;
    return hash_search(htab, &key, HASH_FIND, NULL) != NULL;
}

static void
htab_insert_int64(HTAB *htab, int64 key)
{
    bool found;

    if (htab == NULL)
        return;

    hash_search(htab, &key, HASH_ENTER, &found);
}

static void
htab_insert_triple(HTAB *htab, int64 s_id, int64 p_id, int64 o_id)
{
    TripleGrantKey key;
    bool           found;

    if (htab == NULL)
        return;

    key.subject_id = s_id;
    key.predicate_id = p_id;
    key.object_id = o_id;
    hash_search(htab, &key, HASH_ENTER, &found);
}

static void
htab_insert_pair(HTAB *htab, int64 left_id, int64 right_id)
{
    Int64PairKey key;
    bool         found;

    if (htab == NULL)
        return;

    key.left_id = left_id;
    key.right_id = right_id;
    hash_search(htab, &key, HASH_ENTER, &found);
}

static void
load_principal_scope(LiquidPolicyContext *policy)
{
    static const char *query =
        "WITH RECURSIVE principal_scope(id) AS ( "
        "  SELECT id "
        "  FROM liquid.vertices "
        "  WHERE literal = $1 "
        "UNION "
        "  SELECT edge.object_id "
        "  FROM liquid.edges edge "
        "  JOIN principal_scope scope ON scope.id = edge.subject_id "
        "  WHERE edge.is_deleted = false "
        "    AND edge.predicate_id = $2 "
        ") "
        "SELECT id "
        "FROM principal_scope";
    Oid   argtypes[2] = {TEXTOID, INT8OID};
    Datum argvalues[2];
    char  nulls[2] = {' ', ' '};
    int   row_index;

    if (policy->principal_literal == NULL || policy->principal_literal[0] == '\0')
        return;

    argvalues[0] = CStringGetTextDatum(policy->principal_literal);
    argvalues[1] = Int64GetDatum(policy->acts_for_id);

    if (SPI_execute_with_args(query, 2, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
        return;

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        bool  isnull;
        int64 principal_id;

        principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                   SPI_tuptable->tupdesc, 1, &isnull));
        if (!isnull)
            htab_insert_int64(policy->principal_scope, principal_id);
    }
}

static void
load_int64_grants(HTAB *target,
                  HTAB *principal_scope,
                  const char *query,
                  int64 grant_predicate_id)
{
    Oid   argtypes[1] = {INT8OID};
    Datum argvalues[1];
    char  nulls[1] = {' '};
    int   row_index;

    if (target == NULL || principal_scope == NULL)
        return;

    argvalues[0] = Int64GetDatum(grant_predicate_id);

    if (SPI_execute_with_args(query, 1, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
        return;

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        bool  isnull_principal;
        bool  isnull_value;
        int64 principal_id;
        int64 value_id;

        principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                   SPI_tuptable->tupdesc, 1, &isnull_principal));
        value_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 2, &isnull_value));
        if (!isnull_principal &&
            !isnull_value &&
            htab_contains_int64(principal_scope, principal_id))
            htab_insert_int64(target, value_id);
    }
}

static void
load_pair_grants(HTAB *target,
                 HTAB *principal_scope,
                 const char *query,
                 int64 arg_id)
{
    Oid   argtypes[1] = {INT8OID};
    Datum argvalues[1];
    char  nulls[1] = {' '};
    int   row_index;

    if (target == NULL || principal_scope == NULL)
        return;

    argvalues[0] = Int64GetDatum(arg_id);

    if (SPI_execute_with_args(query, 1, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
        return;

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        bool  isnull_principal;
        bool  isnull_left;
        bool  isnull_right;
        int64 principal_id;
        int64 left_id;
        int64 right_id;

        principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                   SPI_tuptable->tupdesc, 1, &isnull_principal));
        left_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                              SPI_tuptable->tupdesc, 2, &isnull_left));
        right_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 3, &isnull_right));
        if (!isnull_principal &&
            !isnull_left &&
            !isnull_right &&
            htab_contains_int64(principal_scope, principal_id))
            htab_insert_pair(target, left_id, right_id);
    }
}

static void
load_triple_grants(LiquidPolicyContext *policy)
{
    Oid   argtypes[5] = {INT8OID, INT8OID, INT8OID, INT8OID, TEXTOID};
    Datum argvalues[5];
    char  nulls[5] = {' ', ' ', ' ', ' ', ' '};
    int   row_index;
    Datum prefix;
    int64 principal_role_ids[2];
    int   i;

    static const char *query =
        "SELECT principal_edge.object_id, subj.object_id, pred.object_id, obj.object_id "
        "FROM liquid.edges principal_edge "
        "JOIN liquid.edges subj ON subj.subject_id = principal_edge.subject_id "
        "JOIN liquid.edges pred ON pred.subject_id = principal_edge.subject_id "
        "JOIN liquid.edges obj ON obj.subject_id = principal_edge.subject_id "
        "JOIN liquid.vertices cid_v ON cid_v.id = principal_edge.subject_id "
        "WHERE principal_edge.is_deleted = false "
        "  AND subj.is_deleted = false "
        "  AND pred.is_deleted = false "
        "  AND obj.is_deleted = false "
        "  AND substr(cid_v.literal, 1, char_length($5)) = $5 "
        "  AND principal_edge.predicate_id = $1 "
        "  AND subj.predicate_id = $2 "
        "  AND pred.predicate_id = $3 "
        "  AND obj.predicate_id = $4";

    if (policy->subject_role_id < 0 ||
        policy->predicate_role_id < 0 ||
        policy->object_role_id < 0)
        return;

    if (policy->principal_scope == NULL)
        return;

    principal_role_ids[0] = policy->principal_role_id;
    principal_role_ids[1] = policy->user_role_id;

    prefix = CStringGetTextDatum("ReadTriple@(");
    argvalues[1] = Int64GetDatum(policy->subject_role_id);
    argvalues[2] = Int64GetDatum(policy->predicate_role_id);
    argvalues[3] = Int64GetDatum(policy->object_role_id);
    argvalues[4] = prefix;

    for (i = 0; i < 2; i++)
    {
        if (principal_role_ids[i] < 0)
            continue;

        argvalues[0] = Int64GetDatum(principal_role_ids[i]);

        if (SPI_execute_with_args(query, 5, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
            continue;

        for (row_index = 0; row_index < (int) SPI_processed; row_index++)
        {
            bool  isnull_principal;
            bool  isnull_subject;
            bool  isnull_predicate;
            bool  isnull_object;
            int64 principal_id;
            int64 s_id;
            int64 p_id;
            int64 o_id;

            principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                       SPI_tuptable->tupdesc, 1, &isnull_principal));
            s_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 2, &isnull_subject));
            p_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 3, &isnull_predicate));
            o_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 4, &isnull_object));
            if (!isnull_principal &&
                !isnull_subject &&
                !isnull_predicate &&
                !isnull_object &&
                htab_contains_int64(policy->principal_scope, principal_id))
                htab_insert_triple(policy->triple_grants, s_id, p_id, o_id);
        }
    }
}

static bool
has_subject_policy_access(LiquidPolicyContext *policy, int64 subject_id, int64 predicate_id)
{
    return htab_contains_pair(policy->predicate_subject_policies, subject_id, predicate_id);
}

static bool
has_object_policy_access(LiquidPolicyContext *policy, int64 object_id, int64 predicate_id)
{
    return htab_contains_pair(policy->predicate_object_policies, object_id, predicate_id);
}

static bool
has_compound_role_policy_access(LiquidPolicyContext *policy, int64 compound_cid, int64 compound_type_id)
{
    return htab_contains_pair(policy->compound_role_policies, compound_cid, compound_type_id);
}

static void
load_compound_int64_grants(HTAB *target,
                           const char *compound_type,
                           HTAB *principal_scope,
                           int64 principal_role_id,
                           int64 value_role_id)
{
    static const char *query =
        "SELECT principal_edge.object_id, value_edge.object_id "
        "FROM liquid.edges principal_edge "
        "JOIN liquid.edges value_edge ON value_edge.subject_id = principal_edge.subject_id "
        "JOIN liquid.vertices cid_v ON cid_v.id = principal_edge.subject_id "
        "WHERE principal_edge.is_deleted = false "
        "  AND value_edge.is_deleted = false "
        "  AND substr(cid_v.literal, 1, char_length($3)) = $3 "
        "  AND principal_edge.predicate_id = $1 "
        "  AND value_edge.predicate_id = $2";
    Oid   argtypes[3] = {INT8OID, INT8OID, TEXTOID};
    Datum argvalues[3];
    char  nulls[3] = {' ', ' ', ' '};
    int   row_index;
    char *prefix;

    if (principal_scope == NULL || principal_role_id < 0 || value_role_id < 0)
        return;

    prefix = psprintf("%s@(", compound_type);
    argvalues[0] = Int64GetDatum(principal_role_id);
    argvalues[1] = Int64GetDatum(value_role_id);
    argvalues[2] = CStringGetTextDatum(prefix);

    if (SPI_execute_with_args(query, 3, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
    {
        pfree(prefix);
        return;
    }

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        bool  isnull_principal;
        bool  isnull_value;
        int64 principal_id;
        int64 value_id;

        principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                   SPI_tuptable->tupdesc, 1, &isnull_principal));
        value_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 2, &isnull_value));
        if (!isnull_principal &&
            !isnull_value &&
            htab_contains_int64(principal_scope, principal_id))
            htab_insert_int64(target, value_id);
    }

    pfree(prefix);
}

static void
load_compound_pair_grants(HTAB *target,
                          const char *compound_type,
                          HTAB *principal_scope,
                          int64 selector_role_id,
                          int64 value_role_id)
{
    static const char *query =
        "SELECT relation.object_id, relation.subject_id, value_edge.object_id "
        "FROM liquid.edges relation "
        "JOIN liquid.edges selector_edge ON selector_edge.object_id = relation.predicate_id "
        "JOIN liquid.edges value_edge ON value_edge.subject_id = selector_edge.subject_id "
        "JOIN liquid.vertices cid_v ON cid_v.id = selector_edge.subject_id "
        "WHERE relation.is_deleted = false "
        "  AND selector_edge.is_deleted = false "
        "  AND value_edge.is_deleted = false "
        "  AND substr(cid_v.literal, 1, char_length($3)) = $3 "
        "  AND selector_edge.predicate_id = $1 "
        "  AND value_edge.predicate_id = $2";
    Oid   argtypes[3] = {INT8OID, INT8OID, TEXTOID};
    Datum argvalues[3];
    char  nulls[3] = {' ', ' ', ' '};
    int   row_index;
    char *prefix;

    if (principal_scope == NULL || selector_role_id < 0 || value_role_id < 0)
        return;

    prefix = psprintf("%s@(", compound_type);
    argvalues[0] = Int64GetDatum(selector_role_id);
    argvalues[1] = Int64GetDatum(value_role_id);
    argvalues[2] = CStringGetTextDatum(prefix);

    if (SPI_execute_with_args(query, 3, argtypes, argvalues, nulls, true, 0) != SPI_OK_SELECT)
    {
        pfree(prefix);
        return;
    }

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        bool  isnull_principal;
        bool  isnull_left;
        bool  isnull_right;
        int64 principal_id;
        int64 left_id;
        int64 right_id;

        principal_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                                   SPI_tuptable->tupdesc, 1, &isnull_principal));
        left_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                              SPI_tuptable->tupdesc, 2, &isnull_left));
        right_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                               SPI_tuptable->tupdesc, 3, &isnull_right));
        if (!isnull_principal &&
            !isnull_left &&
            !isnull_right &&
            htab_contains_int64(principal_scope, principal_id))
            htab_insert_pair(target, left_id, right_id);
    }

    pfree(prefix);
}

void
liquid_ensure_policy_context(LiquidScanState *lss)
{
    LiquidPolicyContext *policy = &lss->policy_context;
    MemoryContext        oldcxt;
    char                *forced_principal = policy->principal_literal;
    bool                 forced = policy->principal_forced;
    const char          *session_principal = pg_liquid_policy_principal;
    bool                 has_forced_principal = forced_principal != NULL && forced_principal[0] != '\0';
    bool                 has_session_principal = session_principal != NULL && session_principal[0] != '\0';

    if (policy->initialized)
        return;

    /*
     * Fast path for the common read-only case: no explicit principal and no
     * session principal configured means policy filtering is disabled.
     */
    if (!has_forced_principal && !forced && !has_session_principal)
    {
        memset(policy, 0, sizeof(*policy));
        policy->initialized = true;
        return;
    }

    memset(policy, 0, sizeof(*policy));
    policy->principal_forced = forced;

    oldcxt = MemoryContextSwitchTo(lss->solver_context);
    if (has_forced_principal)
        policy->principal_literal = pstrdup(forced_principal);
    else if (has_session_principal)
        policy->principal_literal = pstrdup(session_principal);
    MemoryContextSwitchTo(oldcxt);

    policy->acts_for_id = lookup_identity("liquid/acts_for");
    policy->can_read_predicate_id = lookup_identity("liquid/can_read_predicate");
    policy->can_read_compound_id = lookup_identity("liquid/can_read_compound");
    policy->readable_if_subject_has_id = lookup_identity("liquid/readable_if_subject_has");
    policy->readable_if_object_has_id = lookup_identity("liquid/readable_if_object_has");
    policy->readable_compound_if_role_has_id = lookup_identity("liquid/readable_compound_if_role_has");
    policy->read_triple_type_id = lookup_identity("ReadTriple");
    policy->principal_role_id = lookup_identity("principal");
    policy->user_role_id = lookup_identity("user");
    policy->subject_role_id = lookup_identity("subject");
    policy->predicate_role_id = lookup_identity("predicate");
    policy->object_role_id = lookup_identity("object");
    policy->compound_type_role_id = lookup_identity("compound_type");
    policy->relation_role_id = lookup_identity("relation");
    policy->role_role_id = lookup_identity("role");
    oldcxt = MemoryContextSwitchTo(lss->solver_context);
    policy->principal_scope = create_int64_grant_htab(lss->solver_context, "PrincipalScope");
    policy->predicate_grants = create_int64_grant_htab(lss->solver_context, "PredicateGrants");
    policy->compound_grants = create_int64_grant_htab(lss->solver_context, "CompoundGrants");
    policy->triple_grants = create_triple_grant_htab(lss->solver_context, "TripleGrants");
    policy->predicate_subject_policies = create_int64_pair_htab(lss->solver_context, "PredicateSubjectPolicies");
    policy->predicate_object_policies = create_int64_pair_htab(lss->solver_context, "PredicateObjectPolicies");
    policy->compound_role_policies = create_int64_pair_htab(lss->solver_context, "CompoundRolePolicies");
    MemoryContextSwitchTo(oldcxt);

    policy->enabled = policy->principal_literal != NULL && policy->principal_literal[0] != '\0';

    if (!policy->enabled)
    {
        policy->initialized = true;
        return;
    }

    load_principal_scope(policy);

    if (policy->can_read_predicate_id >= 0)
    {
        load_compound_int64_grants(policy->predicate_grants,
                                   "ReadPredicate",
                                   policy->principal_scope,
                                   policy->principal_role_id,
                                   policy->predicate_role_id);

        load_int64_grants(policy->predicate_grants,
                          policy->principal_scope,
                          "SELECT e.subject_id, e.object_id "
                          "FROM liquid.edges e "
                          "WHERE e.is_deleted = false "
                          "  AND e.predicate_id = $1",
                          policy->can_read_predicate_id);
    }

    if (policy->can_read_compound_id >= 0)
    {
        load_compound_int64_grants(policy->compound_grants,
                                   "ReadCompound",
                                   policy->principal_scope,
                                   policy->principal_role_id,
                                   policy->compound_type_role_id);

        load_int64_grants(policy->compound_grants,
                          policy->principal_scope,
                          "SELECT e.subject_id, e.object_id "
                          "FROM liquid.edges e "
                          "WHERE e.is_deleted = false "
                          "  AND e.predicate_id = $1",
                          policy->can_read_compound_id);
    }

    if (policy->readable_if_subject_has_id >= 0)
    {
        load_compound_pair_grants(policy->predicate_subject_policies,
                                  "PredicateReadBySubject",
                                  policy->principal_scope,
                                  policy->relation_role_id,
                                  policy->predicate_role_id);

        load_pair_grants(policy->predicate_subject_policies,
                         policy->principal_scope,
                         "SELECT relation.object_id, relation.subject_id, policy.subject_id "
                         "FROM liquid.edges relation "
                         "JOIN liquid.edges policy ON policy.object_id = relation.predicate_id "
                         "WHERE relation.is_deleted = false "
                         "  AND policy.is_deleted = false "
                         "  AND policy.predicate_id = $1",
                         policy->readable_if_subject_has_id);
    }

    if (policy->readable_if_object_has_id >= 0)
    {
        load_compound_pair_grants(policy->predicate_object_policies,
                                  "PredicateReadByObject",
                                  policy->principal_scope,
                                  policy->relation_role_id,
                                  policy->predicate_role_id);

        load_pair_grants(policy->predicate_object_policies,
                         policy->principal_scope,
                         "SELECT relation.object_id, relation.subject_id, policy.subject_id "
                         "FROM liquid.edges relation "
                         "JOIN liquid.edges policy ON policy.object_id = relation.predicate_id "
                         "WHERE relation.is_deleted = false "
                         "  AND policy.is_deleted = false "
                         "  AND policy.predicate_id = $1",
                         policy->readable_if_object_has_id);
    }

    if (policy->readable_compound_if_role_has_id >= 0)
    {
        load_compound_pair_grants(policy->compound_role_policies,
                                  "CompoundReadByRole",
                                  policy->principal_scope,
                                  policy->role_role_id,
                                  policy->compound_type_role_id);

        load_pair_grants(policy->compound_role_policies,
                         policy->principal_scope,
                         "SELECT relation.object_id, relation.subject_id, policy.subject_id "
                         "FROM liquid.edges relation "
                         "JOIN liquid.edges policy ON policy.object_id = relation.predicate_id "
                         "WHERE relation.is_deleted = false "
                         "  AND policy.is_deleted = false "
                         "  AND policy.predicate_id = $1",
                         policy->readable_compound_if_role_has_id);
    }

    load_triple_grants(policy);
    policy->initialized = true;
}

bool
liquid_compound_granted(LiquidScanState *lss, int64 compound_type_id)
{
    liquid_ensure_policy_context(lss);

    if (!lss->policy_context.enabled && !lss->policy_context.principal_forced)
        return false;

    return htab_contains_int64(lss->policy_context.compound_grants, compound_type_id);
}

bool
liquid_compound_visible(LiquidScanState *lss, int64 compound_cid, int64 compound_type_id)
{
    LiquidPolicyContext *policy;

    liquid_ensure_policy_context(lss);
    policy = &lss->policy_context;

    if (!policy->enabled && !policy->principal_forced)
        return true;

    if (htab_contains_int64(policy->compound_grants, compound_type_id))
        return true;

    return has_compound_role_policy_access(policy, compound_cid, compound_type_id);
}

bool
liquid_edge_visible(LiquidScanState *lss, int64 s_id, int64 p_id, int64 o_id)
{
    LiquidPolicyContext *policy;

    liquid_ensure_policy_context(lss);
    policy = &lss->policy_context;

    if (!policy->enabled && !policy->principal_forced)
        return true;

    if (htab_contains_int64(policy->predicate_grants, p_id))
        return true;

    if (htab_contains_triple(policy->triple_grants, s_id, p_id, o_id))
        return true;

    if (has_subject_policy_access(policy, s_id, p_id))
        return true;

    if (has_object_policy_access(policy, o_id, p_id))
        return true;

    return false;
}

static void
ensure_scan_plans(void)
{
    static const char *templates[8] = {
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND subject_id = $1",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND predicate_id = $1",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND subject_id = $1 AND predicate_id = $2",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND object_id = $1",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND subject_id = $1 AND object_id = $2",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false AND predicate_id = $1 AND object_id = $2",
        "SELECT subject_id, predicate_id, object_id "
        "FROM liquid.edges WHERE is_deleted = false "
        "AND subject_id = $1 AND predicate_id = $2 AND object_id = $3"
    };
    static const int nargs_map[8] = {0, 1, 1, 2, 1, 2, 2, 3};
    static const Oid arg_types[3] = {INT8OID, INT8OID, INT8OID};
    int              i;

    if (scan_plans_initialized)
        return;

    for (i = 0; i < 8; i++)
    {
        scan_plan_cache[i] = SPI_prepare(templates[i], nargs_map[i], (Oid *) arg_types);
        if (scan_plan_cache[i] != NULL)
            SPI_keepplan(scan_plan_cache[i]);
    }

    scan_plans_initialized = true;
}

List *
scan_facts(LiquidScanState *lss, int64 s_id, int64 p_id, int64 o_id)
{
    ScanFactsListContext ctx;

    ctx.results = NIL;
    scan_facts_visit(lss, s_id, p_id, o_id, append_scan_facts_row, &ctx);
    return ctx.results;
}

static void
append_scan_facts_row(int64 s_id, int64 p_id, int64 o_id, void *ctx_ptr)
{
    ScanFactsListContext *ctx = (ScanFactsListContext *) ctx_ptr;
    int64                *row = palloc(sizeof(int64) * 3);

    row[0] = s_id;
    row[1] = p_id;
    row[2] = o_id;
    ctx->results = lappend(ctx->results, row);
}

void
scan_facts_visit(LiquidScanState *lss,
                 int64 s_id,
                 int64 p_id,
                 int64 o_id,
                 LiquidFactRowVisitor visitor,
                 void *ctx)
{
    Datum  argvalues[3];
    char   nulls[3] = {' ', ' ', ' '};
    int    nargs = 0;
    int    shape = 0;
    int    ret;
    int    row_index;
    bool   isnull;

    liquid_ensure_policy_context(lss);
    ensure_scan_plans();

    if (s_id != -1)
    {
        shape |= 1;
        argvalues[nargs++] = Int64GetDatum(s_id);
    }
    if (p_id != -1)
    {
        shape |= 2;
        argvalues[nargs++] = Int64GetDatum(p_id);
    }
    if (o_id != -1)
    {
        shape |= 4;
        argvalues[nargs++] = Int64GetDatum(o_id);
    }

    if (visitor == NULL)
        return;

    if (scan_plan_cache[shape] == NULL)
        return;

    ret = SPI_execute_plan(scan_plan_cache[shape], argvalues, nulls, false, 0);
    if (ret != SPI_OK_SELECT)
        return;

    for (row_index = 0; row_index < (int) SPI_processed; row_index++)
    {
        int64 row[3];

        row[0] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                             SPI_tuptable->tupdesc, 1, &isnull));
        row[1] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                             SPI_tuptable->tupdesc, 2, &isnull));
        row[2] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[row_index],
                                             SPI_tuptable->tupdesc, 3, &isnull));

        if (liquid_edge_visible(lss, row[0], row[1], row[2]))
            visitor(row[0], row[1], row[2], ctx);
    }
}
