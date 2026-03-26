**Editor's note:** In this two-part blog series, we introduce **LIquid**, a new graph database built by LinkedIn to support human real-time querying of the _economic graph_. It is a complete implementation of the relational model that supports fast, constant-time traversal of graph edges with a relational graph data model that is simple and self-describing, yet still manages to support the definition and indexing of complex n-ary relationships and property graphs. LIquid's interface is a declarative query language based on Datalog. LIquid's query processing engine achieves high performance by using dynamic query planning on wait-free shared-memory index structures.

### Part 1: Graph Data and Applications

Part 1 below will describe how graph data relates to applications, specifically the in-memory graph of structures commonly used as "models" in the sense of "Model-View-Controller" as understood by graphical applications. Since the reason for having a database is ultimately applications, this seems like a good place to start. In part 2, we will describe how graph data can be stored in a conventional relational database, and why conventional relational databases don't work very well for this purpose. Rather than scrapping the relational model completely, we will go on to show how relational graph data can be processed efficiently.

### Introducing LIquid, LinkedIn's in-house graph database

Our team at LinkedIn has spent the last four years building a new graph database named **LIquid**. LIquid is a single, automated service that replaces the hand-coded queries that sometimes added several hundred milliseconds to page load times for members. It can be queried with a powerful general-purpose query language and return the necessary results in an optimal fashion.

### Why LIquid?

Why does LinkedIn need a graph database? We'll get to a more formal answer later in this post, but here's the intuition: the value of an _economic graph_ for the average member lies mostly in their second degree network. These are the connections of your connections, such as the colleague of an old school friend, or the new boss of a co-worker from a prior job.

On LinkedIn, your first degree network is likely small, typically averaging a few hundred current or former coworkers, and other people that you already know. Your third degree network is likely very large, but it is hard to act on, say by leveraging these relationships to get a job, because doing so requires making two sequential new connections. However, the set of second degree connections is typically at least 50,000 entities (e.g., people, schools, employers). It is large enough to be diverse, but at the same time, actionable: you are one new connection away from something that can happen. In fact, your next job is probably in your second degree network.

A first degree network of, for example, 250 connections, would be easily handled by a simple table or key-value store. Second degree networks are a much more challenging software engineering problem because the set of second degree connections is too large to pre-materialize and store, and the write amplification involved in keeping a _pre-materialized_ second degree connection set up to date—roughly 250 times the base write rate, or once for each first degree connection—makes this approach impractical.

On the other hand, computing second degree connections on demand is no picnic either. The first degree is easy to materialize, but thereafter, the join to produce the second is daunting, particularly if you have to search a table of billions of edges stored in conventional sorted storage (typically some sort of terabyte-scale _B-tree_). For an average first degree, this join will be extremely sparse, effectively 250 random look-ups.

For these reasons, LinkedIn has built a series of increasingly general-purpose _graph serving systems_ to provide real-time access to the core of the economic graph. Initially, the graph only stored connections in a very simple format, a tuple of (source, dest, score). Subsequent versions were extended to include other types of edges such as employment and education. However, edges were limited to 2 integer endpoints and 64 bits of attribute data, (type, source, dest, attributes). LIquid is the most recent of these systems, and the first one which is general purpose: a database that implements the relational model, fully described by a schema and accessed by a declarative query language with functionality comparable to SQL.

In LIquid, edges are triples of strings, (subject, predicate, object), and compounds can be built out of edges to represent n-ary relationships with arbitrary attributes.

### From Objects to Graphs

Faced with the problem of working with second degree connections, most programmers would probably start out by putting everything in memory as objects and chasing pointers. For application programmers, this has been the predominant approach to modeling the world since _Smalltalk_ popularized object-orientation and the model-view-controller approach to application development in the 1970s.

While sequential access is always faster, random access in memory is fast enough that programmers are pretty blithe about adding an extra layer of indirection. The stunning complexity of modern UI suggests that second degree networks, which might take thousands of random accesses to produce, should be tractable to work with in human real time. Browsers, word processors, and spreadsheets all traverse tens or hundreds of thousands of pointers rapidly enough for people to consider them instantaneous.

Since our goal is to build applications, let's start with a conventional application model, a graph of objects. If we were to build a graph of authors and books, it might look something like this:

The first question to ask is, "Why aren't we done? Can't we just keep a large graph of objects ready to use in main memory? In fact, variants of this approach were tried by a variety of _object-oriented databases_, OODBs, in the 1990s, and while they succeeded in certain use cases, none of them worked very well in general. OODBs died off while relational systems prospered.

Significant problems with OODBs were:

So, rather than dusting off some OODB, let's explore how we might turn a graph of objects into graph data. The key insight is that the relations between books and authors already exist in the form of arguments to NewAuthorBook. NewAuthorBook updates the reciprocal fields, books_written and authors, which function as _inverted indexes_. The application program only uses the inverted indexes so the original author-book relation is simply discarded when NewAuthorBook is done.

If we're just concerning ourselves with authors and books, a satisfactory solution is to make the relationship between people and books more general. Something like this will allow any sort of relationship between people and books:

While this strategy can be made to work in any specific case, it does not work when applied more generally. We'll have the exact same problem again when we create movie_person. Furthermore, even the simple assumption that authors are people is problematic. There are pseudonyms, "Mark Twain," and groups, "The Beatles," that need a way to be represented as authors. The crux of the matter is that objects encode the type of relation in the class definition, specifically in the field offsets of the inverted indexes, offsetof(author, books_written) and offsetof(book, author). To express a durable model of the world, this rigid encoding simply doesn't work.

In practice, object-oriented programs work around this problem by creating a series of executables, each encoding some specific schemaand storing the data elsewhere (relational tables, documents) using a more flexible schema. However, adding the type of the relationship to the inverted index is a step in the right direction.

Since the use of fixed offsets and structure extension is actually the cause of schema problems, let's generalize the notion of "type of relationship" to "predicate" in a generic edge:

Notice that all objects are now structurally identical instances of identity, vectors of edges that refer to the object with one of subject, predicate, or object. There's no schema evolution problem because there's only one schema. The user can make up instances of identity that are predicates and start using them at any time. The cost of this flexibility is type-specific getter methods that compute their results by filtering edges for those with a matching predicate. This becomes a bit cumbersome if all we want to do is access a field. However, when you consider chasing pointers through a large graph of objects, every pointer dereference is an _L3 cache_ miss, about 100ns. Scanning a few words in between dereferences doesn't hurt very much, say an extra 20% (that would be about 40 L1 loads), tops. The getters are very simple queries. We can use the same data structures to write more complex queries.

Suppose that we have two identities and we want to know if they are related—if an edge between them exists. Here's that query:

Imagine that _a_ has a huge fan in, millions of edges, while _b_ has just a few. It would be much better to scan _b_'s edges looking for _a_, than vice versa. Our uniform inverted index makes this query easy to implement, and the path to cost-driven evaluation of more complex queries, perhaps represented as little graphs of edge constraints, is easy to follow.

So, we've gained a flexible schema, and the ability to write queries. While writing application code in this idiom is much more cumbersome, from the standpoint of a database, this isn't a problem. Application code consumes resources unpredictably and sometimes crashes, so we're not going to be running application code in the database. Instead, we will be running queries (expressed in a declarative query language like SQL) in the database and shipping the results back to the application code that can use whatever classes it wants to represent the result graph.

As data, our graph has one more problem: there's no way to refer to an instance of identity, a vertex, other than with a pointer to its location in memory. If the application program is going to run in a different address space, it needs a durable way to refer to identities. Durable identities need to come from the user, commonly as strings, things like "urn:author:1234". The database can map these strings to pointers to instances of identity, wherever they may happen to be. Handily enough, strings can encode pretty much anything—URNs, names, numbers, dates—so an edge consisting of three strings is completely general. We can say ("urn:author:1234", "wrote", "urn:book:4567"), or ("urn:author:1234", "name", "Jack Kerouac"); the graph machinery doesn't really care. What it cares about is the identity (a number) that corresponds to the string.

Thus far, we're still relying on snapshot consistency. For a query such as _FindPredicates_ to get consistent results, nobody else can change any of the objects that it reads. Simple strategies like locking objects one at a time as the query encounters them are hopelessly slow, deadlock-prone, and wrong. What we need are transactions that behave as if they obtained read or write locks on an entire set of objects in an instant.

While transaction processing is extremely well-studied, the sad truth is that relative to a joyful romp through a snapshot in RAM, transactions perform poorly, distributed transactions, extremely poorly. If we're going to compete with pointer-chasing, we need access to the indexes at pointer-speed, with basically no coordination whatsoever, not even a _compare-and-swap_.

If we restrict ourselves to a single writer, there is a small vocabulary of data structures that have _wait-free_ implementations: logs, linked lists, _hash tables_. This is just barely enough for our purposes. To start, the writer will append edges to a single "graph log." Each edge can then be referred to by its offset in the log and the log offset functions as a _virtual time_ which allows us to define consistency as "answering a query 'as of' a certain log offset." The index will contain copies of edges from the graph log. Each identity/object in the index is essentially a special-purpose mini-log containing just edges that refer to that object.

In order to use the indexes to compute a consistent result, we decorate each edge with its offset from the graph log. Something like this (if the vectors were wait-free):

To generate a consistent result, we choose a log offset, probably the current end of the _graph_log_, and then scan the index log backwards, discarding edges until the offset is less than our chosen log offset. Thereafter, any edges we find may be part of the answer to our query. Thus, relative to a snapshot, we have to do a little more work. But the work is uniformly distributed—there's no possibility of lock contention or deadlock—and it can take place at the maximum speed that the processor is capable of.

In theory, the single writer is a bottleneck, but in practice, we have no trouble appending edges to the log at a rate which exceeds the write rate of a large social graph by many orders of magnitude. An ever-growing log would be a liability for edges which frequently change, but most human-curated data does not have this property. Additions occur at human speed, and deletions, which we handle by appending a tombstone edge, are relatively infrequent.

While heavily optimized for both space and speed, these index structures are the basic index data structures that LIquid uses. They deliver fast, constant-time performance that allows graph edges to be used blithely, much as one would use pointers.

While we still have much to describe, the outline of LIquid is clear: we build a log-structured, in-memory inverted index of edges, triples of strings. Internally, the strings are converted to integers, pointers to structures in the object-oriented formulation. We process queries using this index and return the results to the application program as a set of edges, a subgraph. The application program converts the edges into whatever structural idiom is convenient, a graph of structures or a DOM tree, and displays the results to the user.

The ingredients for this conversion are simple: mappings from identity string to address or structure offset and possibly a topological sort of result edges so that the target hierarchy can be built bottom-up.

In the next part, we'll describe how to express a graph in the relational model, why conventional relational databases don't work very well for this purpose, and how LIquid can process relational graph data efficiently.

## Part 2: Relational Graph Data and Query Processing

**Adventures in Software**
**September 16, 2020**
**Co-authors:** Scott Meyer, Andrew Carter, and Andrew Rodriguez

**Editor's note:** This is the second part of a two-part blog series. Part 1 described how graph data relates to the in-memory graph of structures commonly used as models (as in "model-view-controller") by applications, how graph data can be stored in a conventional relational database, and why conventional relational databases don't work very well on this sort of data. Rather than scrapping the relational model, in this installment, we'll show how relational graph data can be processed efficiently.

### Graphs as relational data

In the last post, we started by discussing the design of LIquid in a straightforward way, beginning from the application programmer's view, a graph of objects in memory, and solving problems as we encountered them. However, having arrived at a point where we have relational edge data (the graph log) and an inverted index (the objects), we might want to look at a conventional relational approach to this problem. In fact, representing a graph as an adjacency list of edges in a tabular relational system is easy:

```sql
CREATE TABLE vertices (
  vertex_id integer PRIMARY KEY,
  literal text UNIQUE NOT NULL
);

CREATE TABLE edges (
  subject integer REFERENCES vertices,
  predicate integer REFERENCES vertices,
  object integer REFERENCES vertices,
  PRIMARY KEY (subject, predicate, object)
);

INSERT INTO vertices VALUES
  (1, 'knows'),
  (2, 'a1'),
  (3, 'b1'),
  (4, 'b2'),
  (5, 'c1'),
  (6, 'skills'),
  (7, 'worked_for'),
  (8, 'java'),
  (9, 'C++'),
  (10, 'IBM'),
  (11, 'Oracle');

INSERT INTO edges VALUES
  (2, 1, 3),
  (2, 1, 4),
  (3, 1, 5),
  (4, 1, 5),
  (5, 6, 8),
  (5, 6, 9),
  (5, 7, 10),
  (5, 7, 11);
```

There are obvious benefits to adopting this restricted form of the standard tabular relational model, including:

- Use of the well-studied relational model: It is difficult to argue that set theory and first-order logic should be considered harmful or something to avoid. The relational model has had enormous practical success, and for very good reasons.
- No NULLs: Edges either exist or do not. The system is not forced to assign meaning to something like Edge("x", "age", NULL), so there's no need for built-in trinary logic. The application must explicitly say what it intends (0, MAXINT, …) when nulls arise due to disjunction in queries.
- No normalization problems: This data is in fifth normal form. There's no way for the user to express denormalization.
- Easy data portability: Today, it is difficult to move data from one database to another. There's no standard encoding for a set of tables. For single tables, in our specific case (edges expressed as triples of strings), data exchange is easy: CSV and spreadsheets are common tabular interchange formats.

This form of adjacency-list graph encoding is simple, well-known and has been available for decades. So again, "Why aren't we done?" Basically, two reasons:

- Poor performance: Traditional relational database systems, based on a static tree of relational algebra operations applied to sorted storage, perform horribly on this schema. For a static query planner based on table-level statistics, self joins are intensely problematic.
- SQL: SQL (an implementation relational algebra) is extremely awkward to use for building applications. It is verbose, composes awkwardly, and naturally returns only results which can be expressed as a single table. Colloquially, this awkwardness is known as the "object-relational impedance mismatch" or worse, if the speaker has suffered through a recent adverse encounter with it.

Our workload, and a very common application workload generally, is browsing: inspecting a small portion of a very large graph. While the subgraph displayed on a browser page is tiny relative to the size of the overall graph, it has an intricate structure because human beings like to know what is interesting about the object they are looking at. The declarative query that retrieves the subgraph necessary to display a page will commonly have tens or hundreds of edge-constraints, self-joins with the edges table in this data model. Unavoidably, the bulk of query evaluation for such queries turns into random probing of the edges table. If the table is implemented by a sorted structure, some form of B-tree is nearly universal in tabular relational systems, random probing is O(log(N)), and N and the constant factors involved are large. Concretely, at terabyte scale, an immutable B-tree will probably have at least three levels, with only the first being relatively hot in the L1 cache. A single probe will require two probably-uncached binary searches, easily dozens of L3 cache misses to materialize a single edge. Mutability will make this performance worse. In contrast, the index structures that we describe in "Nanosecond Indexing of Graph Data with Hash Maps and VLists" (presentation) will do a probe for a single edge in either 2 or 3 L3 misses. The fact that social graph data is frequently skewed, with the largest fan-outs being many orders of magnitude larger than the average, exacerbates the performance difference between sorted and hash storage. In a sorted index, finding the size of a result set without actually materializing it will require a second binary search. In practice, it is difficult to decide when the second binary search is worth doing. In a hash-based index, the size of a set can be stored in the top-level hash table, available in 1 L3 miss. Detecting skew in advance of experiencing its adverse effects is cheap and easy.

Shifting our attention from storage to query evaluation, we find that relational algebra is inefficient, both as a declarative specification and as execution machinery. Consider the following simple graph query about skills and employers in someone's second degree network:

```sql
WITH g(a, b, c, d, e) AS (
  WITH
    e1(a, b) AS (
      SELECT subject, object FROM edges WHERE subject=2
        AND predicate=1),
    e2(b, c) AS (
      SELECT subject, object FROM edges WHERE predicate=1),
    e3(c, d) AS (
      SELECT subject, object FROM edges WHERE predicate=6),
    e4(c, e) AS (
      SELECT subject, object FROM edges WHERE predicate=7)
  SELECT e1.a, e1.b, e2.c, e3.d, e4.e
    FROM e1 JOIN e2 USING(b) JOIN e3 USING(c)
    JOIN e4 USING(c))
SELECT va.literal AS a, vb.literal AS b, vc.literal AS c,
    vd.literal AS d, ve.literal AS e
  FROM
    g, vertices AS va, vertices AS vb, vertices AS vc,
      vertices AS vd, vertices AS ve
  WHERE
    g.a=va.vertex_id AND
    g.b=vb.vertex_id AND
    g.c=vc.vertex_id AND
    g.d=vd.vertex_id AND
    g.e=ve.vertex_id;
```

This produces the following result:

```
 a  | b  | c  |  d   |   e
----+----+----+------+--------
 a1 | b1 | c1 | java | Oracle
 a1 | b2 | c1 | java | Oracle
 a1 | b1 | c1 | java | IBM
 a1 | b2 | c1 | java | IBM
 a1 | b1 | c1 | C++  | Oracle
 a1 | b2 | c1 | C++  | Oracle
 a1 | b1 | c1 | C++  | IBM
 a1 | b2 | c1 | C++  | IBM
```

As notation, this is cumbersome. Syntax alone is a solvable problem. However, notice the redundancy in the output: pairs like (Java, Oracle) or (a1, b1) occur multiple times, even though there is just one underlying edge. This spurious cross product in the result table is a fundamental problem. Both the data and the result desired by the user are graph-shaped. Representing a graph as a table of tuples of variable bindings forces cross products, two of them in this very simple example. In theory, this is just a protocol problem brought on by SQL's use of a single table for query results—one we could fix by returning matching rows from multiple tables instead of just one. However, in practice, tabular relational systems really use relational algebra to evaluate queries, and thus spurious cross products are actually an efficiency concern. As one of our co-authors, Andrew Carter, once quipped, "It is difficult to have MxN stuff without doing MxN work." Sadly, this isn't the end of the bad news for relational algebra. Graph-shaped queries—those with circular constraints among the variables—turn out to be important, with triangular or diamond-shaped constraints being a common task. Recent work on "Worst-case Optimal Join Algorithms" has shown that "any algorithm based on the traditional select-project-join style plans typically employed in an RDBMS are asymptotically slower than the optimal for some queries," with a simple triangular join being the leading example.

LIquid uses a cost-based dynamic query evaluator, based on constraint satisfaction applied to a graph of edge-constraints. In this context, "dynamic" means that the query plan is not known when evaluation starts. In a traditional SQL query evaluator, the query plan (a tree of relational algebra operators) is completely determined before evaluation starts. In contrast, LIquid's evaluator establishes path consistency of the result subgraph by repeatedly satisfying the cheapest remaining constraint. For cost computations, the evaluator relies heavily on constant-time access to set sizes in the index. We'll describe the query evaluator in more detail in an upcoming paper; for now, let's just say that we think that the performance problems traditionally associated with a relational graph data model are tractable. This leaves us needing only a relational query language with a succinct, composable syntax. Like this, perhaps:

```datalog
FOAF(a, b, c) :-  % Friend of a Friend
  Edge(a, "knows", b),
  Edge(b, "knows", c).

EmployerSkills(e, c, d) :-
  Edge(c, "worked_for", e),
  Edge(c, "skilled_at", d).

SkillsEmployersFOAF(a, b, c, d, e) :-
  FOAF(a, b, c),
  EmployerSkills(e, c, d).
```

This is Datalog, a subset of the logic programming language Prolog. The above query is the same as the previous SQL query. Unlike SQL, Datalog composes easily: The above is the same query, composed from two sub-queries. While Datalog has a dearth of industrial-strength implementations, it is extremely well studied, commonly used in the study of relational databases. To adapt Datalog for our purposes, we need only require that Edge-relations be the only form which can be asserted. All rules must ultimately reduce to Edges. The shape of query results can be selected by the user: the traditional table of rule-head variable bindings as in SQL, or the actual edges used to satisfy the query. Aside from possibly forcing cross products after the constraints have been satisfied, choosing tabular results has no impact on query evaluation. However, if what the user needs is a complex, graph-shaped result, such as might naturally be produced by a complex, composed query, then an efficient way to consume it is important. Tabular results and spurious cross products are the crux of the object-relational impedance mismatch. Returning query results as a subgraph (a set of edges) allows arbitrarily complex queries to be answered in a single, consistent operation.

Now we have an in-memory relational database with hash-based indexes and Datalog as a query language, with a dynamic query planner based on constraint satisfaction. Are we done yet? Not quite. Relative to object-graphs or SQL, we lack schema. All values are just strings. Worse, we have no way to insist that certain relationships be unique—for example, that an entity should have just one name. Without enforced uniqueness, there's no way to map a graph into a normalized table. Consider:

```datalog
People(id, n, dob) :-
  Edge(id, "name", n),
  Edge(id, "date_of_birth", dob).
```

Generally, people have one name and one birthday. For this query, a single table is an excellent return value. However, if the graph allows multiple name or date of birth edges on an entity, we will have to expand the cross product and the user will have to re-normalize manually. Tables are common and useful, and users reasonably require this mapping. What we need to repair this default is a way to describe the ends of an edge: the scalar type (how to interpret the literal string that identifies the vertex) and cardinality constraint (how many incident edges) for subject and object. As with the SQL catalog, which describes tables, we encode this description of edges as graph data so that the graph is self-describing: liquid/type and liquid/cardinality are "bootstrap predicates," known to the database implementation and used to define the schema.

Now, we could describe a predicate as follows:

```datalog
TypeAndCardinality(e, type, cardinality) :-
  Edge(e, "liquid/type", type),
  Edge(e, "liquid/cardinality", cardinality).

DefPred(p, sm, sc, st, om, oc, ot) :-
  Edge(p, "liquid/subject_meta", sm),
  TypeAndCardinality(sm, st, sc),
  Edge(p, "liquid/object_meta", om),
  TypeAndCardinality(om, ot, oc).

DefPred("name",
  "name_sub", "1", "liquid/node",
  "name_obj", "0", "liquid/string").
```

We say "could" because it is a bit awkward to ask the user to define extra identities for the subject and object metadata, "name_sub" and "name_obj". This will be fixed presently. Scalar types such as liquid/node, liquid/string, or liquid/int determine parsing rules with which the corresponding identity must comply for the write to succeed. Identities can and do serve multiple purposes:

```datalog
Edge("book:1", "isbn", "9783161484100").  % an identifier
Edge("attribute:1", "count", "9783161484100").  % an integer
Edge("alien:1", "name", "9783161484100").  % a string

Edge(_, _, "9783161484100")?  % all of these things

Edge(_, p, "9783161484100"),  % just string-valued things
  Edge(p, "liquid/object_meta", om),
  Edge(om, "liquid/type", "liquid/string")?
```

As in the above example, the user is expected to disambiguate in the query if need be. The database implementation is free to exploit type information, for example by sorted or quantized indexing or immediate storage of small integers. While many relationships are naturally represented as a subject-predicate-object triple: mother, name, author, there are also many which are not, like an employment tenure (person, company, start date), or an actor's film performance (actor, role, film). For ternary relationships, we could force them into the existing Edge by making one of the vertices do double duty as a predicate: "Harrison Ford Han-Solo'd in Star Wars," but this is a one-off hack that doesn't generalize to n-ary relationships. A graph data model should allow arbitrary n-ary "compound edges" to behave just like simple edges, as unique relationships which either exist or do not. Given that edges can be used blithely, it is tempting to start on a solution by building n-ary relationships out of edges:

```datalog
FilmPerf(x, a, r, f) :-
  Edge(x, "actor", a),
  Edge(x, "role", r),
  Edge(x, "film", f).

FilmPerf("x", "Harrison Ford", "Han Solo", "Star Wars").
```

With pointer-like index performance, the extra hop should not be a performance problem. However, the need to fabricate an identity of the hub node, "x", is a problem. In particular, if the user simply creates arbitrary hub identities, the FilmPerf assertion does not behave like an Edge assertion. Duplicate FilmPerf compound edges could occur. However, if we choose a systematic encoding for the hub node, say something like "actor:Harrison Ford,film:Star Wars,role:Han Solo", FilmPerf assertions will have the same identity semantics as edges. To allow the database implementation to do this encoding for us, we introduce compound graph schema: a systematic way of expressing relative identities. A compound is specified by some meta-structure which indicates that a set of predicates is used to generate a relative identity: The subject side of the predicate always refers to the relative identity, the "compound hub." The object side is specified by the user. For convenience, a compound definition implicitly generates a corresponding rule with a reserved @-suffix. Here's what the FilmPerf compound actually looks like:

```datalog
DefCompound(compound, predicate, object_cardinality, object_type) :-
  DefPred(predicate, "1", "liquid/node", object_cardinality, object_type),
  Edge(compound, "liquid/compound_predicate", predicate).

DefCompound("FilmPerf", "actor", "0", "liquid/node").
DefCompound("FilmPerf", "role", "0", "liquid/node").
DefCompound("FilmPerf", "film", "0", "liquid/node").

% Implicitly generated…
%
% FilmPerf@(cid, actor, role, film) :-
%  Edge(cid, "actor", actor),
%  Edge(cid, "role", role),
%  Edge(cid, "film", film).

FilmPerf@(cid=x, actor="Harrison Ford", role="Han Solo", film="Star Wars"),
  Edge(x, "type", "breakthrough performance").
```

Notice that we can use the generated relative identity, cid, just like any other node. Here, we indicate that this film performance was a breakthrough. In tabular relational terms, a compound edge behaves exactly like a compound primary key. Since compounds are known to the database implementation, the database is free to optimize compound relationships. In fact, a simple generalization of the index structures described in the start of this writing will allow compound edges and attributes which refer to them to be accessed as fast as atomic Edges. The database implementation can optimize away the extra hop. Effectively, we get the expressiveness and performance of a property graph without the need for the user to define or operate on properties specially. Instances of FilmPerf behave "like vertexes" in that edges can refer to them, and "like edges" in that they can be deleted. Sometimes we need compound relationships that only behave "like vertexes." We just want a scalar value with some queryable internal structure:

```datalog
DefCompound("Email", "user", "0", "liquid/string").
DefCompound("Email", "domain", "0", "liquid/string").
Edge("Email", "liquid/mutable", "false"),

Email@(cid=x, account="root" domain="xyzzy.com"),
   Edge("user:123", "contact_email", x).
```

Such vertex-compounds allow us to encode arbitrary tree structures directly in graph data, which is fully accessible to the same Datalog query machinery used on an ordinary graph. Generally, compounds define "relative identity," a notion which turns out to be surprisingly useful. In particular, we use it to define "the subject side" and "the object side" of an edge in predicate schema. The real definition of DefPred looks something like this:

```datalog
DefCompound("Ometa", "liquid/object_meta", "1", "liquid/node")
Edge("Ometa", "liquid/mutable", "false").

DefCompound("Smeta", "liquid/object_meta", "1", "liquid/node")
Edge("Smeta", "liquid/mutable", "false").

DefPred(p, sc, st, oc, ot) :-
  Smeta@(cid=sm, liquid/subject_meta=p),
  TypeAndCardinality(sm, st, sc),
  Ometa@(cid=om, liquid/object_meta=p),
  TypeAndCardinality(om, ot, oc).
```

This allows us to have distinct relative identities for a predicate: "the subject side," sm, and "the object side," om, without bothering the user to come up with unique identifiers for them.

### LIquid's nearest neighbors

Let's define what we've built. A "graph database" is an implementation of the relational model with the following properties: All relations are equal and first-class. While a useful form of index for predictable access patterns, employing tables as the physical data model results in inequities as soon as there is more than one table. Tuples expressed in a single physical table are vastly more performant than those expressed by joining two or more tables. Lastly, the lack of domain support in tabular relational systems makes the user's intentions with respect to possible joins opaque. In a graph database, the existence of an edge is exactly expressive of user intent. Edge traversal is fast and constant-time. If everything is stored as edges, we're going to be doing a lot of joining, specifically self-joins. Every query with small intermediate results will almost certainly devolve into random access. Query results are a subgraph. Application models are complex and applications need to be able to retrieve arbitrarily complex models in a single query with a single consistency guarantee. Schema-evolution is constant-time. If schema-evolution (really, the addition of new predicates) takes non-constant time, then the database implementation must be pretending to continue to operate with edges while maintaining structs or tables under the covers.

There are many things in the world that call themselves graph databases, roughly divided into three families:

1. RDF, SPARQL: triple stores
2. Property graphs: Neo4J, Facebook's Tao, various Gremlin implementations
3. SQL and SQL extensions such as SQL Server's graph support

Aside from #3, none of them have clear relationships to the relational model. None of them support n-ary compound edges. Query evaluation is either relational algebra-based, or imperative (traversal-based).

RDF is quite close to LIquid's Edge in spirit, but has introduced a great deal of complexity. Triples are asymmetric. Actually, they are quads. There is a language tag that is difficult to square with both unicode string encoding, which is multilingual, and no type system (there are several) that can express cardinality constraints like: "one name in any language." SPARQL seems to have inherited most of SQL's ailments.

Property graphs introduce a second schema for properties and force the user to choose between representing data as edges or as properties of nodes or edges. A node with properties is basically an object and suffers from all of the drawbacks of OODBs. The obvious encoding of a property graph as a table of vertices (with properties) and a table of edges (with properties) will force two lookups for any query involving properties of vertices and edges.

While many graph database implementations are effectively "in RAM," none explicitly optimize for random access with hash-based indexing, or exploit fast random access to set sizes in query evaluation. Nothing targets complex queries or supports the query composition necessary to do so easily.

We have defined what a graph database is and described the implementation of one that supports the Datalog query language. We have also suggested how to index graph data for fast, constant-time access and how to construct a dynamic query planner based on constraint satisfaction. The graph data model is simple and universal, and features a self-describing schema which allows us to specify relationship cardinality and value domains. And, this schema allows us to define n-ary compound edges, which we can use to implement property-graphs.
