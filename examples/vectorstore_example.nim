import std/options

import ../src/boogie/stores/vectorstore

# The vector store keeps float embeddings next to the ids they belong to
# and answers nearest-neighbor queries with cosine, dot or L2 distance.
# Vectors can also carry a partition tag that bounds a search to one
# locality scope, which keeps large collections fast.

let vs = newInMemoryVectorStore()

# A collection fixes the dimensionality every vector in it must have.
vs.createCollection(newCollection("embeddings", 3))

# The trailing argument groups each row into a partition. Rows without
# one simply live in the default, unpartitioned space.
vs.insert("embeddings", "doc-1", @[0.1'f32, 0.2, 0.3], "tenant-7")
vs.insert("embeddings", "doc-2", @[0.9'f32, 0.8, 0.7], "tenant-7")

if vs.get("embeddings", "doc-1").isSome:
  echo "doc-1 is stored"

let q = @[0.11'f32, 0.19, 0.31]

# Scoping the search to "tenant-7" scores only that partition's rows
# instead of the whole collection.
for (pk, score) in vs.nearest("embeddings", q, k = 2, dmCosine, "tenant-7"):
  echo pk, " (", score, ")"
