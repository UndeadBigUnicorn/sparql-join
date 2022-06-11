## SPARQL joins
This repository contains Implementation of Join Algorithm for SPARQL Query Processing

---
### Join Algorithms
Implemented Join algorithms are used on Waterloo SPARQL Diversity Test Suite (WatDiv) dataset. 
Dateset is being vertical partitioned into tables, that lately are joined to the specific SPARQL query.
The project implements Join Algorithms:
- Hash Join
- Partitioned Parallel Hash Join
- Radix Hash Join
- Sort-Merge Join

### SPARQL Queries
One example of SPARQL query on WatDiv dataset is:
```
(?a)---follows--->(?b)---friendOf--->(?c)---likes--->(?d)---hasReview--->(?e)
```
In the query, `(?a)`, `(?b)`, `(?c)`, `(?d)`, `(?e)` are variables, and `follows`, `friendOf`, `likes`, `hasReview` are properties. 
The answer of the query is the list of mapped values of all the variables `(?a)`, `(?b)`, `(?c)`, `(?d)`, `(?e)`.

Obviously, above query can be expressed in the form of SQL given the data set yield by vertically partitioned approach. 
The corresponding SQL expression is as follows:
```sql
SELECT follows.subject, follows.object, friendOf.object, likes.object, hasReview.object
  FROM follows, friendOf, likes, hasReview
  WHERE follows.object = friendOf.subject
        AND friendOf.object = likes.subject
        AND likes.object = hasReview.subject
```
The query in the form of relational algebra is as follows:
- `follows` join `friendOf` on `follows.object=friendOf.subject`
- `friendOf` join `likes` on `friendOf.object=likes.subject`
- `likes` join `hasReview` on `likes.object=hasReview.subject` 