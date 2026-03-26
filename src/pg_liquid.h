/*
 * pg_liquid.h — shared header for the pg_liquid extension.
 *
 * LIquid blog parity reset:
 *   - LIquid program parsing with local rule predicates
 *   - Named compound atoms with cid bindings
 *   - Single tabular query path
 *   - Existing solver retained for adaptive rule evaluation
 */

#ifndef PG_LIQUID_H
#define PG_LIQUID_H

#include "postgres.h"
#include "fmgr.h"
#include "utils/rel.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/planner.h"
#include "utils/tuplestore.h"
#include "access/relscan.h"
#include "access/table.h"
#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "nodes/pathnodes.h"
#include "nodes/extensible.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"

/* Maximum variables in a single Datalog query */
#define MAX_DATALOG_VARS    32

/* Maximum atoms in a rule body */
#define MAX_RULE_BODY_ATOMS 64

/* Maximum arguments in a compound */
#define MAX_COMPOUND_ARGS   16

/* Safety bound: maximum fixpoint iterations for rule evaluation */
#define MAX_RULE_ITERATIONS 10000

/*
 * Sentinel value returned by lookup_identity when a literal is unknown.
 * Engine treats any atom containing this as guaranteed to produce no results.
 */
#define LIQUID_UNKNOWN_ID   (-2LL)

/* ----------------------------------------------------------------
 * AST types
 * ---------------------------------------------------------------- */

typedef enum LiquidAtomType
{
    LIQUID_ATOM_EDGE,       /* Edge(s, p, o) — binary edge in the graph */
    LIQUID_ATOM_PREDICATE,  /* SomePred(a, b, ...) — query-local Datalog predicate */
    LIQUID_ATOM_COMPOUND    /* Type@(cid=x, role="value", ...) */
} LiquidAtomType;

typedef struct LiquidTerm
{
    int64   id;         /* constant vertex ID, or LIQUID_UNKNOWN_ID */
    char   *literal;    /* literal value for quoted constants */
    bool    is_var;
    int     var_idx;
    char   *var_name;
} LiquidTerm;

typedef struct LiquidAtom
{
    LiquidAtomType  type;
    char           *name;           /* predicate or compound type name */
    int64           predicate_id;   /* vertex ID of predicate name */
    int             num_terms;
    LiquidTerm     *terms;
    bool            satisfied;
    bool            is_idb;
    bool            use_delta_scan;
    double          cost;           /* estimated scan cost, set by planner */
    int             num_named_args; /* LIQUID_ATOM_COMPOUND only */
    char          **named_arg_names;
    int64          *named_arg_ids;
} LiquidAtom;

typedef enum LiquidRuleExecutionKind
{
    LIQUID_RULE_EXEC_GENERIC = 0,
    LIQUID_RULE_EXEC_SINGLE_EDGE,
    LIQUID_RULE_EXEC_PREDICATE_EDGE,
    LIQUID_RULE_EXEC_TWO_PREDICATES
} LiquidRuleExecutionKind;

typedef struct LiquidRule
{
    LiquidAtom *head;
    List       *body;                   /* List of LiquidAtom* */
    int         num_vars;
    bool        has_idb_body;
    bool        is_recursive;
    int         recursive_atom_count;
    int         recursive_atom_indexes[MAX_RULE_BODY_ATOMS];
    LiquidRuleExecutionKind execution_kind;
} LiquidRule;

typedef struct LiquidFact
{
    int64   predicate_id;
    int     num_terms;
    int64  *terms;
} LiquidFact;

typedef struct LiquidBinding
{
    int64   values[MAX_DATALOG_VARS];
    bool    is_bound[MAX_DATALOG_VARS];
} LiquidBinding;

typedef struct LiquidCompiledTerm
{
    int64   id;
    char   *literal;
    bool    is_var;
    int     var_idx;
    char   *var_name;
} LiquidCompiledTerm;

typedef struct LiquidCompiledAtom
{
    int              plan_index;
    LiquidAtomType   type;
    char            *name;
    int64            predicate_id;
    int              predicate_index;
    bool             statically_is_idb;
    int              num_terms;
    LiquidCompiledTerm *terms;
    int              num_named_args;
    char           **named_arg_names;
    int64           *named_arg_ids;
} LiquidCompiledAtom;

typedef struct LiquidCompiledRule
{
    LiquidCompiledAtom *head;
    LiquidCompiledAtom **body_atoms;
    int                 body_count;
    int                 head_predicate_index;
    int                 head_scc_id;
    int                 num_vars;
    bool                has_idb_body;
    bool                is_recursive;
    int                 recursive_atom_count;
    int                *recursive_atom_indexes;
    LiquidRuleExecutionKind execution_kind;
} LiquidCompiledRule;

typedef struct LiquidPlanUnresolvedIdentityRef
{
    int64      *target_id;
    const char *literal;
} LiquidPlanUnresolvedIdentityRef;

/* ----------------------------------------------------------------
 * Hash key structs
 * ---------------------------------------------------------------- */

typedef struct DerivedFactKey
{
    int64   predicate_id;
    int     num_terms;
    int64   terms[MAX_DATALOG_VARS];
} DerivedFactKey;

typedef struct DerivedFactEntry
{
    DerivedFactKey  key;
    LiquidFact     *fact;
} DerivedFactEntry;

typedef struct DerivedFactStore DerivedFactStore;

/* ----------------------------------------------------------------
 * Indexed Edge Cache
 * ---------------------------------------------------------------- */

typedef struct EdgeCacheRow
{
    int64 s;
    int64 p;
    int64 o;
} EdgeCacheRow;

typedef struct EdgeIndexKey
{
    int64 k1;
    int64 k2;   /* -1 if single-key */
} EdgeIndexKey;

typedef struct EdgeIndexEntry
{
    EdgeIndexKey    key;
    int            *row_indices;
    int             count;
    int             capacity;
} EdgeIndexEntry;

typedef struct EdgeCache
{
    EdgeCacheRow   *rows;
    int             count;
    int             capacity;
    HTAB           *idx_sp;   /* (subject, predicate) */
    HTAB           *idx_po;   /* (predicate, object)  */
    HTAB           *idx_so;   /* (subject, object)    */
    HTAB           *idx_p;    /* (predicate)           */
    HTAB           *idx_s;    /* (subject)             */
    HTAB           *idx_o;    /* (object)              */
} EdgeCache;

extern int pg_liquid_edge_cache_budget_kb;
extern int pg_liquid_edge_cache_row_estimate_bytes;
extern char *pg_liquid_policy_principal;

typedef struct TripleGrantKey
{
    int64 subject_id;
    int64 predicate_id;
    int64 object_id;
} TripleGrantKey;

typedef struct LiquidPolicyContext
{
    bool    initialized;
    bool    enabled;
    bool    principal_forced;
    char   *principal_literal;
    int64   acts_for_id;
    int64   can_read_predicate_id;
    int64   can_read_compound_id;
    int64   readable_if_subject_has_id;
    int64   readable_if_object_has_id;
    int64   readable_compound_if_role_has_id;
    int64   read_triple_type_id;
    int64   principal_role_id;
    int64   user_role_id;
    int64   subject_role_id;
    int64   predicate_role_id;
    int64   object_role_id;
    int64   compound_type_role_id;
    int64   relation_role_id;
    int64   role_role_id;
    HTAB   *principal_scope;
    HTAB   *predicate_grants;
    HTAB   *compound_grants;
    HTAB   *triple_grants;
    HTAB   *predicate_subject_policies;
    HTAB   *predicate_object_policies;
    HTAB   *compound_role_policies;
} LiquidPolicyContext;

/* ----------------------------------------------------------------
 * Scan State
 * ---------------------------------------------------------------- */

typedef struct LiquidScanState
{
    const struct LiquidExecutionPlan *plan;
    LiquidCompiledAtom **constraints;
    int             constraint_count;
    int             num_vars;
    struct LiquidAtomExecState *atom_state;

    Tuplestorestate *result_store;
    TupleDesc       result_desc;

    MemoryContext   solver_context;
    List           *current_frontier;   /* List of LiquidBinding* */
    int             current_frontier_size;
    bool            execution_done;

    LiquidCompiledRule **rules;
    int             rule_count;
    HTAB           *derived_facts_htab;
    DerivedFactStore *derived_fact_store;
    bool            rules_evaluated;

    EdgeCache      *edge_cache;

    DerivedFactStore *delta_fact_store;

    LiquidPolicyContext policy_context;
} LiquidScanState;

/* ----------------------------------------------------------------
 * Parsed LIquid program
 * ---------------------------------------------------------------- */

typedef struct LiquidEdgeLiteralTriple
{
    const char *subject_literal;
    const char *predicate_literal;
    const char *object_literal;
} LiquidEdgeLiteralTriple;

typedef struct LiquidAssertionClause
{
    List   *atoms;              /* List of LiquidAtom* */
    int     num_vars;
    LiquidEdgeLiteralTriple *direct_edges;
    int     direct_edge_count;
    int     direct_edge_capacity;
} LiquidAssertionClause;

typedef struct LiquidProgram
{
    List   *assertions;         /* List of LiquidAssertionClause* */
    List   *rules;              /* List of LiquidRule* */
    List   *query_atoms;        /* List of LiquidAtom* */
    int     query_num_vars;
    int     query_num_output_vars;
    int     query_output_var_idxs[MAX_DATALOG_VARS];
} LiquidProgram;

typedef enum LiquidAssertionOpKind
{
    LIQUID_ASSERTION_OP_EDGE_BATCH = 0,
    LIQUID_ASSERTION_OP_DEFINE_PRED,
    LIQUID_ASSERTION_OP_DEFINE_COMPOUND,
    LIQUID_ASSERTION_OP_INSERT_COMPOUND,
    LIQUID_ASSERTION_OP_CLAUSE_FALLBACK
} LiquidAssertionOpKind;

typedef struct LiquidCompiledAssertionClause
{
    LiquidCompiledAtom **atoms;
    int                 atom_count;
} LiquidCompiledAssertionClause;

typedef struct LiquidCompiledTransitiveClosurePlan
{
    bool    valid;
    int64   predicate_id;
    int     rule_index;
    int     scc_id;
} LiquidCompiledTransitiveClosurePlan;

typedef struct LiquidAssertionOp
{
    LiquidAssertionOpKind kind;
    union
    {
        struct
        {
            LiquidEdgeLiteralTriple *edges;
            int                      count;
            int                      capacity;
        } edge_batch;
        LiquidCompiledAtom *atom;
        LiquidCompiledAssertionClause clause;
    } data;
} LiquidAssertionOp;

typedef struct LiquidExecutionPlan
{
    LiquidAssertionOp *ops;
    int                op_count;
    int                op_capacity;
    LiquidEdgeLiteralTriple *edge_literals;
    int                edge_literal_count;
    int                edge_literal_capacity;
    LiquidCompiledTerm *terms;
    int                term_count;
    int                term_capacity;
    LiquidCompiledAtom *atoms;
    int                atom_count;
    int                atom_capacity;
    char             **named_arg_names;
    int64             *named_arg_ids;
    int                named_arg_count;
    int                named_arg_capacity;
    LiquidCompiledAtom **query_atoms;
    int                query_atom_count;
    int                query_atom_capacity;
    LiquidCompiledRule *rules_storage;
    LiquidCompiledRule **rules;
    int                rule_count;
    int                rule_capacity;
    int64             *predicate_ids;
    int                predicate_count;
    int               *predicate_dependency_offsets;
    int               *predicate_dependency_indexes;
    int                predicate_dependency_count;
    int               *predicate_scc_ids;
    int                scc_count;
    int               *scc_sizes;
    bool              *scc_self_recursive;
    int               *scc_strata;
    int               *scc_order;
    int               *scc_rule_offsets;
    int               *scc_rule_indexes;
    int               *query_predicate_indexes;
    int                query_predicate_index_count;
    LiquidCompiledAtom **rule_body_atoms;
    int                rule_body_atom_count;
    int                rule_body_atom_capacity;
    int               *recursive_atom_indexes;
    int                recursive_atom_index_count;
    int                recursive_atom_index_capacity;
    LiquidPlanUnresolvedIdentityRef *unresolved_identity_refs;
    int                unresolved_identity_ref_count;
    int                unresolved_identity_ref_capacity;
    const char       **preload_literals;
    int                preload_literal_count;
    int                preload_literal_capacity;
    int                query_num_vars;
    int                query_num_output_vars;
    int                query_output_var_idxs[MAX_DATALOG_VARS];
    int                direct_edge_capacity_hint;
    bool               has_assertions;
    LiquidCompiledTransitiveClosurePlan transitive_closure_plan;
    MemoryContext      mcxt;
} LiquidExecutionPlan;

typedef struct LiquidAtomExecState
{
    bool    satisfied;
    bool    is_idb;
    bool    use_delta_scan;
    double  cost;
} LiquidAtomExecState;

/* ----------------------------------------------------------------
 * Function declarations
 * ---------------------------------------------------------------- */

int64   lookup_identity(const char *literal);
char   *lookup_identity_literal(int64 id);
const char *lookup_identity_literal_ref(int64 id);
int64   get_or_create_identity(const char *literal);
void    reset_catalog_caches(void);
void    register_catalog_xact_callbacks(void);
void    preload_identities_batch(const char **literals, int count);
void    define_predicate(const char *name,
                         const char *subject_cardinality,
                         const char *subject_type,
                         const char *object_cardinality,
                         const char *object_type);
void    define_compound(const char *name,
                        const char *predicate_name,
                        const char *object_cardinality,
                        const char *object_type);
char   *build_compound_identity_literal(const char *compound_type,
                                        int num_roles,
                                        const char *const *role_names,
                                        const char *const *role_values);
int64   insert_compound(const char *compound_type,
                        int num_roles,
                        const char *const *role_names,
                        const int64 *role_ids,
                        const char *const *role_values);
void    insert_edge_internal(int64 subject_id, int64 predicate_id, int64 object_id);
void    insert_edges_batch(const int64 *subject_ids,
                           const int64 *predicate_ids,
                           const int64 *object_ids,
                           int count);


/* parser.c */
LiquidProgram *parse_liquid_program(char *input);
int         compute_atom_list_num_vars(List *atoms);

/* assertion_compiler.c */
LiquidExecutionPlan *compile_liquid_execution_plan(LiquidProgram *program,
                                                   MemoryContext mcxt);
void finalize_execution_plan_ids(LiquidExecutionPlan *plan);

/* scan.c */
typedef void (*LiquidFactRowVisitor)(int64 s_id, int64 p_id, int64 o_id, void *ctx);

void scan_facts_visit(LiquidScanState *lss,
                      int64 s_id,
                      int64 p_id,
                      int64 o_id,
                      LiquidFactRowVisitor visitor,
                      void *ctx);
List *scan_facts(LiquidScanState *lss, int64 s_id, int64 p_id, int64 o_id);
void liquid_ensure_policy_context(LiquidScanState *lss);
bool liquid_edge_visible(LiquidScanState *lss, int64 s_id, int64 p_id, int64 o_id);
bool liquid_compound_granted(LiquidScanState *lss, int64 compound_type_id);
bool liquid_compound_visible(LiquidScanState *lss, int64 compound_cid, int64 compound_type_id);

/* engine.c */
void run_solver(LiquidScanState *lss);

#endif /* PG_LIQUID_H */
