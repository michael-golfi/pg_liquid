#include "postgres.h"
#include "pg_liquid.h"
#include "utils/builtins.h"

typedef enum ParseMode
{
    PARSE_MODE_QUERY,
    PARSE_MODE_ASSERTION
} ParseMode;

typedef struct VarRegistry
{
    char **names;
    bool  *output;
    int   count;
    int   capacity;
    int   anonymous_counter;
} VarRegistry;

typedef struct LocalPredicateRegistry
{
    char **names;
    int64 *ids;
    int   count;
    int   capacity;
    int64 next_id;
} LocalPredicateRegistry;

static void
ensure_var_capacity(VarRegistry *registry)
{
    int old_capacity;

    if (registry->count < registry->capacity)
        return;

    old_capacity = registry->capacity;
    registry->capacity = (registry->capacity == 0) ? 16 : registry->capacity * 2;

    if (registry->names == NULL)
        registry->names = palloc0(sizeof(char *) * registry->capacity);
    else
    {
        registry->names = repalloc(registry->names, sizeof(char *) * registry->capacity);
        memset(registry->names + old_capacity, 0,
               sizeof(char *) * (registry->capacity - old_capacity));
    }

    if (registry->output == NULL)
        registry->output = palloc0(sizeof(bool) * registry->capacity);
    else
    {
        registry->output = repalloc(registry->output, sizeof(bool) * registry->capacity);
        memset(registry->output + old_capacity, 0,
               sizeof(bool) * (registry->capacity - old_capacity));
    }
}

static void
ensure_local_predicate_capacity(LocalPredicateRegistry *registry)
{
    int old_capacity;

    if (registry->count < registry->capacity)
        return;

    old_capacity = registry->capacity;
    registry->capacity = (registry->capacity == 0) ? 16 : registry->capacity * 2;

    if (registry->names == NULL)
        registry->names = palloc0(sizeof(char *) * registry->capacity);
    else
    {
        registry->names = repalloc(registry->names, sizeof(char *) * registry->capacity);
        memset(registry->names + old_capacity, 0,
               sizeof(char *) * (registry->capacity - old_capacity));
    }

    if (registry->ids == NULL)
        registry->ids = palloc0(sizeof(int64) * registry->capacity);
    else
    {
        registry->ids = repalloc(registry->ids, sizeof(int64) * registry->capacity);
        memset(registry->ids + old_capacity, 0,
               sizeof(int64) * (registry->capacity - old_capacity));
    }
}

static const char *
scan_quoted_literal(const char *cursor)
{
    cursor++;

    while (*cursor != '\0')
    {
        if (*cursor == '\\')
        {
            if (*(cursor + 1) == '\0')
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("unterminated escape sequence in string literal")));
            cursor += 2;
            continue;
        }
        if (*cursor == '"')
            return cursor + 1;
        cursor++;
    }

    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("unterminated string literal")));
    return cursor;
}

static char *
decode_string_literal_span(const char *literal, int len)
{
    char *decoded;
    int   src = 1;
    int   dst = 0;

    if (len < 2 || literal[0] != '"' || literal[len - 1] != '"')
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("invalid string literal: %s", literal)));

    decoded = palloc(len - 1);

    while (src < len - 1)
    {
        if (literal[src] == '\\')
        {
            src++;
            switch (literal[src])
            {
                case '"':
                case '\\':
                    decoded[dst++] = literal[src++];
                    break;
                case 'n':
                    decoded[dst++] = '\n';
                    src++;
                    break;
                case 'r':
                    decoded[dst++] = '\r';
                    src++;
                    break;
                case 't':
                    decoded[dst++] = '\t';
                    src++;
                    break;
                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("unsupported escape sequence in string literal")));
            }
            continue;
        }

        decoded[dst++] = literal[src++];
    }

    decoded[dst] = '\0';
    return decoded;
}

static char *
decode_string_literal(const char *literal)
{
    return decode_string_literal_span(literal, strlen(literal));
}

static char *
trim_whitespace(char *s)
{
    char *end;

    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')
        s++;
    if (*s == '\0')
        return s;

    end = s + strlen(s) - 1;
    while (end > s &&
           (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r'))
        *end-- = '\0';
    return s;
}

static char *
strip_comments(char *input)
{
    char *cursor = input;

    while (*cursor != '\0')
    {
        if (*cursor == '"')
        {
            cursor = (char *) scan_quoted_literal(cursor);
            continue;
        }
        if (*cursor == '%')
        {
            while (*cursor != '\0' && *cursor != '\n')
                *cursor++ = ' ';
            continue;
        }
        cursor++;
    }

    return input;
}

static List *
split_items(char *input)
{
    List *items = NIL;
    char *cursor = input;
    char *start = input;
    int   paren_depth = 0;

    while (*cursor != '\0')
    {
        if (*cursor == '"')
        {
            cursor = (char *) scan_quoted_literal(cursor);
            continue;
        }
        if (*cursor == '(')
            paren_depth++;
        else if (*cursor == ')')
        {
            if (paren_depth == 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("unmatched ')'")));
            paren_depth--;
        }
        else if (*cursor == ',' && paren_depth == 0)
        {
            char *item;

            *cursor = '\0';
            item = trim_whitespace(start);
            if (*item != '\0')
                items = lappend(items, item);
            start = cursor + 1;
        }
        cursor++;
    }

    if (paren_depth != 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("unbalanced parentheses")));

    start = trim_whitespace(start);
    if (*start != '\0')
        items = lappend(items, start);
    return items;
}

static bool
has_top_level_comma(const char *input)
{
    const char *cursor = input;
    int         paren_depth = 0;

    while (*cursor != '\0')
    {
        if (*cursor == '"')
        {
            cursor = scan_quoted_literal(cursor);
            continue;
        }
        if (*cursor == '(')
            paren_depth++;
        else if (*cursor == ')')
        {
            if (paren_depth == 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("unmatched ')'")));
            paren_depth--;
        }
        else if (*cursor == ',' && paren_depth == 0)
            return true;
        cursor++;
    }

    if (paren_depth != 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("unbalanced parentheses")));

    return false;
}

static int
register_variable(VarRegistry *registry, const char *name, bool expose_output)
{
    int i;

    if (strcmp(name, "_") == 0)
    {
        char anonymous_name[32];

        if (registry->count >= MAX_DATALOG_VARS)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many variables in statement; max is %d",
                            MAX_DATALOG_VARS)));

        ensure_var_capacity(registry);
        snprintf(anonymous_name, sizeof(anonymous_name),
                 "__anon_%d", registry->anonymous_counter++);
        registry->names[registry->count]  = pstrdup(anonymous_name);
        registry->output[registry->count] = false;
        return registry->count++;
    }

    for (i = 0; i < registry->count; i++)
    {
        if (strcmp(registry->names[i], name) == 0)
        {
            if (expose_output)
                registry->output[i] = true;
            return i;
        }
    }

    if (registry->count >= MAX_DATALOG_VARS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many variables in statement; max is %d",
                        MAX_DATALOG_VARS)));

    ensure_var_capacity(registry);
    registry->names[registry->count]  = pstrdup(name);
    registry->output[registry->count] = expose_output;
    return registry->count++;
}

static int64
lookup_or_register_local_predicate(LocalPredicateRegistry *registry,
                                   const char *name)
{
    int i;

    for (i = 0; i < registry->count; i++)
    {
        if (strcmp(registry->names[i], name) == 0)
            return registry->ids[i];
    }

    ensure_local_predicate_capacity(registry);
    registry->names[registry->count] = pstrdup(name);
    registry->ids[registry->count]   = registry->next_id--;
    return registry->ids[registry->count++];
}

static LiquidTerm
parse_term(char *token, VarRegistry *registry, ParseMode mode, bool expose_output)
{
    LiquidTerm term;
    char      *trimmed = trim_whitespace(token);

    memset(&term, 0, sizeof(LiquidTerm));

    if (*trimmed == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("empty term")));

    if (*trimmed == '"')
    {
        (void) scan_quoted_literal(trimmed);
        term.literal = decode_string_literal(trimmed);
        term.is_var  = false;
        if (mode == PARSE_MODE_QUERY)
            term.id = lookup_identity(term.literal);
        else
            term.id = LIQUID_UNKNOWN_ID;
        if (term.id < 0)
            term.id = LIQUID_UNKNOWN_ID;
        return term;
    }

    if (*trimmed == '?')
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("legacy ?var syntax is not supported; use bare variables")));

    term.is_var   = true;
    term.var_name = pstrdup(trimmed);
    term.var_idx  = register_variable(registry, trimmed, expose_output);
    return term;
}

static LiquidAtom *
parse_normal_atom(char *token,
                  VarRegistry *registry,
                  LocalPredicateRegistry *local_predicates,
                  ParseMode mode,
                  bool expose_output)
{
    LiquidAtom *atom;
    char       *trimmed = trim_whitespace(token);
    char       *open_paren;
    char       *close_paren;
    char       *name;
    char       *args_text;
    List       *args;
    ListCell   *lc;
    int         arg_count;
    int         arg_index = 0;

    open_paren = strchr(trimmed, '(');
    if (open_paren == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("invalid Liquid atom: %s", trimmed)));

    *open_paren = '\0';
    name = pstrdup(trim_whitespace(trimmed));
    args_text = open_paren + 1;
    close_paren = strrchr(args_text, ')');
    if (close_paren == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("missing closing ')' in atom: %s", name)));
    *close_paren = '\0';

    args = split_items(args_text);
    arg_count = list_length(args);
    if (arg_count == 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("atom %s() must contain at least one term", name)));
    if (arg_count > MAX_DATALOG_VARS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("atom %s has too many terms; max is %d",
                        name, MAX_DATALOG_VARS)));

    atom = palloc0(sizeof(LiquidAtom));
    atom->name = name;
    atom->num_terms = arg_count;
    atom->terms = palloc0(sizeof(LiquidTerm) * atom->num_terms);

    if (strcmp(name, "Edge") == 0)
    {
        atom->type = LIQUID_ATOM_EDGE;
        if (atom->num_terms != 3)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("Edge requires exactly 3 terms")));
    }
    else
    {
        atom->type = LIQUID_ATOM_PREDICATE;
        atom->predicate_id = lookup_or_register_local_predicate(local_predicates, name);
        atom->is_idb = true;
    }

    foreach(lc, args)
    {
        atom->terms[arg_index++] = parse_term((char *) lfirst(lc),
                                              registry, mode, expose_output);
    }

    return atom;
}

static LiquidAtom *
parse_compound_atom(char *token,
                    VarRegistry *registry,
                    ParseMode mode,
                    bool expose_output)
{
    LiquidAtom *atom;
    char       *trimmed = trim_whitespace(token);
    char       *marker;
    char       *close_paren;
    char       *type_name;
    char       *args_text;
    List       *args;
    ListCell   *lc;
    int         role_count = 0;
    int         role_index = 0;
    bool        saw_cid = false;

    marker = strstr(trimmed, "@(");
    if (marker == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("invalid compound atom: %s", trimmed)));

    *marker = '\0';
    type_name = pstrdup(trim_whitespace(trimmed));
    args_text = marker + 2;
    close_paren = strrchr(args_text, ')');
    if (close_paren == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("missing closing ')' in compound atom: %s", type_name)));
    *close_paren = '\0';

    args = split_items(args_text);
    foreach(lc, args)
    {
        char *entry = pstrdup((char *) lfirst(lc));
        char *equals = strchr(entry, '=');

        if (equals == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("compound arguments must be named: %s", entry)));
        *equals = '\0';
        if (strcmp(trim_whitespace(entry), "cid") != 0)
            role_count++;
    }

    if (role_count <= 0 || role_count > MAX_COMPOUND_ARGS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("compound %s must have between 1 and %d named roles",
                        type_name, MAX_COMPOUND_ARGS)));

    atom = palloc0(sizeof(LiquidAtom));
    atom->type = LIQUID_ATOM_COMPOUND;
    atom->name = type_name;
    if (mode == PARSE_MODE_QUERY)
        atom->predicate_id = lookup_identity(type_name);
    else
        atom->predicate_id = LIQUID_UNKNOWN_ID;
    if (atom->predicate_id < 0)
        atom->predicate_id = LIQUID_UNKNOWN_ID;
    atom->num_terms = role_count + 1;
    atom->terms = palloc0(sizeof(LiquidTerm) * atom->num_terms);
    atom->num_named_args = role_count;
    atom->named_arg_names = palloc0(sizeof(char *) * role_count);
    atom->named_arg_ids = palloc0(sizeof(int64) * role_count);

    foreach(lc, args)
    {
        char *entry = pstrdup((char *) lfirst(lc));
        char *equals = strchr(entry, '=');
        char *key;
        char *value;

        if (equals == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("compound arguments must be named: %s", entry)));

        *equals = '\0';
        key   = trim_whitespace(entry);
        value = trim_whitespace(equals + 1);

        if (strcmp(key, "cid") == 0)
        {
            atom->terms[0] = parse_term(value, registry, mode, expose_output);
            saw_cid = true;
            continue;
        }

        atom->named_arg_names[role_index] = pstrdup(key);
        if (mode == PARSE_MODE_QUERY)
            atom->named_arg_ids[role_index] = lookup_identity(key);
        else
            atom->named_arg_ids[role_index] = LIQUID_UNKNOWN_ID;
        if (atom->named_arg_ids[role_index] < 0)
            atom->named_arg_ids[role_index] = LIQUID_UNKNOWN_ID;

        atom->terms[role_index + 1] = parse_term(value, registry, mode, expose_output);
        role_index++;
    }

    if (!saw_cid)
        atom->terms[0] = parse_term("_", registry, mode, false);

    return atom;
}

static LiquidAtom *
parse_atom(char *token,
           VarRegistry *registry,
           LocalPredicateRegistry *local_predicates,
           ParseMode mode,
           bool expose_output)
{
    if (strstr(token, "@(") != NULL)
        return parse_compound_atom(token, registry, mode, expose_output);
    return parse_normal_atom(token, registry, local_predicates, mode, expose_output);
}

static LiquidRule *
parse_rule(char *statement, LocalPredicateRegistry *local_predicates)
{
    LiquidRule *rule;
    VarRegistry registry;
    char       *separator;
    char       *head_text;
    char       *body_text;
    List       *body_atoms;
    ListCell   *lc;
    int         body_len;

    memset(&registry, 0, sizeof(registry));

    separator = strstr(statement, ":-");
    if (separator == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("invalid rule: %s", statement)));

    *separator = '\0';
    head_text = trim_whitespace(statement);
    body_text = trim_whitespace(separator + 2);

    rule = palloc0(sizeof(LiquidRule));
    rule->head = parse_normal_atom(head_text, &registry, local_predicates,
                                   PARSE_MODE_QUERY, false);
    if (rule->head->type != LIQUID_ATOM_PREDICATE)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("rule heads must be ordinary predicates")));

    body_atoms = split_items(body_text);
    body_len = list_length(body_atoms);
    if (body_len <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("rule body may not be empty")));
    if (body_len > MAX_RULE_BODY_ATOMS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("rule body has too many atoms; max is %d",
                        MAX_RULE_BODY_ATOMS)));

    foreach(lc, body_atoms)
    {
        LiquidAtom *atom = parse_atom((char *) lfirst(lc), &registry,
                                      local_predicates, PARSE_MODE_QUERY, false);
        rule->body = lappend(rule->body, atom);
    }

    foreach(lc, rule->body)
    {
        LiquidAtom *atom = (LiquidAtom *) lfirst(lc);
        int         i;

        if (atom->type == LIQUID_ATOM_PREDICATE &&
            atom->predicate_id == rule->head->predicate_id)
            atom->is_idb = true;

        for (i = 0; i < rule->head->num_terms; i++)
        {
            int      term_index;
            bool     found = false;
            ListCell *blc;

            if (!rule->head->terms[i].is_var)
                continue;

            foreach(blc, rule->body)
            {
                LiquidAtom *body_atom = (LiquidAtom *) lfirst(blc);
                for (term_index = 0; term_index < body_atom->num_terms; term_index++)
                {
                    if (body_atom->terms[term_index].is_var &&
                        body_atom->terms[term_index].var_idx ==
                        rule->head->terms[i].var_idx)
                    {
                        found = true;
                        break;
                    }
                }
                if (found)
                    break;
            }

            if (!found)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("unsafe rule: head variable %s does not appear in body",
                                rule->head->terms[i].var_name)));
        }
    }

    return rule;
}

static bool
parse_simple_edge_assertion(char *statement,
                            LiquidEdgeLiteralTriple *edge_out)
{
    const char            *cursor = trim_whitespace(statement);
    const char            *literal_starts[3];
    int                    literal_lengths[3];
    int                    i;

    if (strncmp(cursor, "Edge(", 5) != 0)
        return false;

    cursor += 5;

    for (i = 0; i < 3; i++)
    {
        const char *literal_end;

        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n' || *cursor == '\r')
            cursor++;

        if (*cursor != '"')
            return false;

        literal_end = scan_quoted_literal(cursor);
        literal_starts[i] = cursor;
        literal_lengths[i] = (int) (literal_end - cursor);
        cursor = literal_end;

        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n' || *cursor == '\r')
            cursor++;

        if (i < 2)
        {
            if (*cursor != ',')
                return false;
            cursor++;
        }
    }

    if (*cursor != ')')
        return false;
    cursor++;

    while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n' || *cursor == '\r')
        cursor++;
    if (*cursor != '\0')
        return false;

    for (i = 0; i < 3; i++)
    {
        char *decoded = decode_string_literal_span(literal_starts[i],
                                                   literal_lengths[i]);

        if (i == 0)
            edge_out->subject_literal = decoded;
        else if (i == 1)
            edge_out->predicate_literal = decoded;
        else
            edge_out->object_literal = decoded;
    }
    return true;
}

static LiquidAssertionClause *
make_direct_edge_clause(void)
{
    LiquidAssertionClause *clause = palloc0(sizeof(LiquidAssertionClause));

    clause->direct_edge_capacity = 8;
    clause->direct_edges = palloc(sizeof(LiquidEdgeLiteralTriple) *
                                  clause->direct_edge_capacity);
    clause->direct_edge_count = 0;
    clause->num_vars = 0;
    return clause;
}

static void
append_direct_edge_to_clause(LiquidAssertionClause *clause,
                             LiquidEdgeLiteralTriple *edge)
{
    if (clause->direct_edge_count >= clause->direct_edge_capacity)
    {
        clause->direct_edge_capacity = (clause->direct_edge_capacity == 0)
            ? 8
            : clause->direct_edge_capacity * 2;
        clause->direct_edges = repalloc(clause->direct_edges,
                                        sizeof(LiquidEdgeLiteralTriple) *
                                        clause->direct_edge_capacity);
    }

    clause->direct_edges[clause->direct_edge_count++] = *edge;
}

static LiquidAssertionClause *
parse_assertion_clause(char *statement)
{
    LiquidAssertionClause *clause;
    VarRegistry            registry;
    LocalPredicateRegistry local_predicates;
    List                  *atoms;
    ListCell              *lc;

    clause = palloc0(sizeof(LiquidAssertionClause));

    memset(&registry, 0, sizeof(registry));
    memset(&local_predicates, 0, sizeof(local_predicates));
    local_predicates.next_id = -1000;

    if (!has_top_level_comma(statement))
        clause->atoms = list_make1(parse_atom(statement,
                                              &registry,
                                              &local_predicates,
                                              PARSE_MODE_ASSERTION,
                                              false));
    else
    {
        atoms = split_items(statement);
        foreach(lc, atoms)
        {
            clause->atoms = lappend(clause->atoms,
                                    parse_atom((char *) lfirst(lc),
                                               &registry,
                                               &local_predicates,
                                               PARSE_MODE_ASSERTION,
                                               false));
        }
    }
    clause->num_vars = registry.count;

    return clause;
}

static void
record_output_vars(LiquidProgram *program, VarRegistry *registry)
{
    int i;

    program->query_num_vars = registry->count;
    for (i = 0; i < registry->count; i++)
    {
        if (registry->output[i])
            program->query_output_var_idxs[program->query_num_output_vars++] = i;
    }
}

static void
append_builtin_rules(LiquidProgram *program, LocalPredicateRegistry *local_predicates)
{
    static const char *builtin_rules[] = {
        "TypeAndCardinality(e, type, cardinality) :- Edge(e, \"liquid/type\", type), Edge(e, \"liquid/cardinality\", cardinality)",
        "DefPred(p, sc, st, oc, ot) :- Smeta@(cid=sm, liquid/subject_meta=p), TypeAndCardinality(sm, st, sc), Ometa@(cid=om, liquid/object_meta=p), TypeAndCardinality(om, ot, oc)",
        "DefCompound(compound, predicate, object_cardinality, object_type) :- DefPred(predicate, \"1\", \"liquid/node\", object_cardinality, object_type), Edge(compound, \"liquid/compound_predicate\", predicate)"
    };
    int i;

    for (i = 0; i < lengthof(builtin_rules); i++)
    {
        char *rule_text = pstrdup(builtin_rules[i]);
        program->rules = lappend(program->rules, parse_rule(rule_text, local_predicates));
    }
}

static bool
statement_has_predicate_atoms(char *statement)
{
    char     *statement_copy = pstrdup(statement);
    List     *atoms;
    ListCell *lc;

    atoms = split_items(statement_copy);
    foreach(lc, atoms)
    {
        char *atom_text = trim_whitespace((char *) lfirst(lc));

        if (strstr(atom_text, "@(") == NULL &&
            strncmp(atom_text, "Edge(", 5) != 0)
        {
            return true;
        }
    }

    return false;
}

LiquidProgram *
parse_liquid_program(char *input)
{
    LiquidProgram          *program = palloc0(sizeof(LiquidProgram));
    LocalPredicateRegistry  local_predicates;
    LiquidAssertionClause  *current_direct_edge_batch = NULL;
    char                   *clean_input;
    char                   *cursor;
    char                   *start;
    int                     paren_depth = 0;
    bool                    saw_query = false;
    bool                    builtins_appended = false;

    memset(&local_predicates, 0, sizeof(local_predicates));
    local_predicates.next_id = -1000;

    clean_input = strip_comments(input);
    cursor = clean_input;
    start = clean_input;

    while (*cursor != '\0')
    {
        if (*cursor == '"')
        {
            cursor = (char *) scan_quoted_literal(cursor);
            continue;
        }
        if (*cursor == '(')
            paren_depth++;
        else if (*cursor == ')')
        {
            if (paren_depth == 0)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("unmatched ')'")));
            paren_depth--;
        }
        else if ((*cursor == '.' || *cursor == '?') && paren_depth == 0)
        {
            char terminator = *cursor;
            char *statement;

            *cursor = '\0';
            statement = trim_whitespace(start);
            if (*statement != '\0')
            {
                if (terminator == '?')
                {
                    VarRegistry query_registry;
                    List       *atoms;
                    ListCell   *lc;

                    if (saw_query)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("Liquid programs may contain only one terminal query")));
                    if (!builtins_appended && statement_has_predicate_atoms(statement))
                    {
                        append_builtin_rules(program, &local_predicates);
                        builtins_appended = true;
                    }
                    memset(&query_registry, 0, sizeof(query_registry));
                    atoms = split_items(statement);
                    foreach(lc, atoms)
                    {
                        program->query_atoms = lappend(program->query_atoms,
                            parse_atom((char *) lfirst(lc), &query_registry,
                                       &local_predicates, PARSE_MODE_QUERY, true));
                    }
                    record_output_vars(program, &query_registry);
                    saw_query = true;
                }
                else if (strstr(statement, ":-") != NULL)
                {
                    if (saw_query)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("terminal query must be the last statement")));
                    if (!builtins_appended)
                    {
                        append_builtin_rules(program, &local_predicates);
                        builtins_appended = true;
                    }
                    program->rules = lappend(program->rules,
                                             parse_rule(statement, &local_predicates));
                }
                else
                {
                    LiquidAssertionClause *clause;
                    LiquidEdgeLiteralTriple edge;

                    if (saw_query)
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("assertions must appear before the terminal query")));

                    if (parse_simple_edge_assertion(statement, &edge))
                    {
                        if (current_direct_edge_batch == NULL)
                        {
                            current_direct_edge_batch = make_direct_edge_clause();
                            program->assertions = lappend(program->assertions,
                                                          current_direct_edge_batch);
                        }
                        append_direct_edge_to_clause(current_direct_edge_batch, &edge);
                    }
                    else
                    {
                        clause = parse_assertion_clause(statement);
                        program->assertions = lappend(program->assertions, clause);
                        current_direct_edge_batch = NULL;
                    }
                }
            }
            start = cursor + 1;
        }
        cursor++;
    }

    if (paren_depth != 0)
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("unbalanced parentheses")));

    return program;
}

int
compute_atom_list_num_vars(List *atoms)
{
    int       max_var_idx = -1;
    ListCell *lc;

    foreach(lc, atoms)
    {
        LiquidAtom *atom = (LiquidAtom *) lfirst(lc);
        int         i;

        for (i = 0; i < atom->num_terms; i++)
        {
            if (atom->terms[i].is_var && atom->terms[i].var_idx > max_var_idx)
                max_var_idx = atom->terms[i].var_idx;
        }
    }

    if (max_var_idx + 1 > MAX_DATALOG_VARS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many variables in atom list; max is %d",
                        MAX_DATALOG_VARS)));

    return max_var_idx + 1;
}
