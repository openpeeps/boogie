<p align="center">
  <img src="https://github.com/openpeeps/boogie/blob/main/.github/boogie.png" width="100px"><br>
  A suite of embedded data stores in Nim with<br>
  write-ahead log (WAL) support for durability and crash recovery
</p>

<p align="center">
  <code>nimble install boogie</code>
</p>

<p align="center">
  <a href="https://openpeeps.github.io/boogie">API reference</a><br>
  <img src="https://github.com/openpeeps/boogie/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/boogie/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- BTrees storage and Hash Tables for fast lookups
- Write-ahead log (WAL) for durability and crash recovery, with batched group commits, CRC32 record framing, and O(1) LSN recovery
- Simple API for inserting, updating, deleting, and querying records
- Configurable options for performance tuning, such as batch sizes and flush intervals
- In-memory or On-disk storage modes
- Primitive data types (`string`, `int`, `float`, `bool`, `json`, `null`)

#### What's included?

1. **Key/Value Store** - A simple Key-Value store implementation with WAL support, plus an optional lazy-read mode that keeps values on disk
2. **RDBMS Store** - A relational WAL-based database with support for **schemas**, **primary keys**, typed columns and **foreign keys** with `RESTRICT` delete actions
3. **Vector Store** - Vector store implementation with WAL support and nearest-neighbor search (cosine / dot / L2), including named partitions that bound a search to a locality scope
4. **Columnar Store** - Columnar storage engine for analytics workloads with WAL support and in-memory column caching
5. **Graph Store** - A simple graph database with support for nodes, relationships, and basic graph queries (e.g., neighbors, BFS) with WAL support
6. **Document Store** - A flexible document store on top of the WAL, storing JSON (or BSON) documents

>[!NOTE]
> Boogie is an experimental project mostly made with the chatbot for fun and learning. It is still in early stages, so expect data loss and breaking changes. Use at your own risk.

This can be used as a simple embedded database for your Nim applications. If you want,
you can use [openpeeps/e2ee](https://github.com/openpeeps/e2ee) to encrypt the data before
inserting it into Boogie database.

## Examples

Every store supports an in-memory mode and a disk-backed mode with WAL, group
commits, and periodic checkpoints. Benchmark numbers come from the bundled test
suites (`nim r -d:release tests/testX.nim`) on the author's development machine.

### Key/Value Store

```nim
import boogie/stores/kv
import std/options

let kvStore = newInMemoryKvStore()  # or newKvStore("mydb", ksmDisk, enableWal = true)
kvStore.put("greeting", "hello")
kvStore.put("user:1", "alice")
if kvStore.hasKey("greeting"):
  echo kvStore.get("greeting").get()   # "hello"
discard kvStore.delete("user:1")       # true
for k, v in kvStore.pairsUnordered:    # unordered iteration
  echo k, " -> ", v
```

> For very large datasets, `newKvStore("mydb", ksmDisk, enableWal = true, lazyReads = true)`
> keeps only a key → offset index in memory and reads values back from the WAL on demand.
> Values are never resident in RAM (0 bytes retained for 50k×256B in the benchmark), at the
> cost of a disk seek+read per `get` instead of a memory lookup.

| Operation | ops/s |
|---|---|
| put (memory) | ~3.13 M |
| get (memory) | ~4.75 M |
| delete (memory) | ~4.08 M |
| put (disk + WAL) | ~1.39 M |
| get (disk + WAL) | ~7.52 M |
| delete (disk + WAL) | ~2.18 M |
| get (disk, lazy offset read) | ~0.06 M |

### RDBMS Store

```nim
import boogie/stores/rdbms
import std/options

let store = newStore("blogdata", smDisk, enableWal = true, checkpointEveryOps = 10)

let users = newTable(
  name = "users",
  primaryKey = "id",
  columns = [
    newColumn("id", dtInt, false),
    newColumn("name", dtText, false),
    newColumn("email", dtText, true)
  ],
  primaryKeyMode = pkmSerial
)
store.createTableIfNotExist(users)

# Auto-increment primary key
let userPk = store.insertRow("users", row({
  "name": newTextValue("Alice"),
  "email": newTextValue("alice@example.com")
}))

# Explicit primary key
store.insertRow("users", "42", row({"name": newTextValue("Bob")}))

# Point lookup + indexed equality search
echo store.getRow("users", userPk).isSome
let usersT = store.getTable("users").get()
usersT.createIndex("name")
for (pk, r) in usersT.where("name", newTextValue("Alice")):
  echo pk, " -> ", r

discard store.deleteRow("users", userPk)
store.checkpoint()
```

| Operation | ops/s |
|---|---|
| insert | ~0.27 M |
| lookup (by pk) | ~0.89 M |
| ordered scan | ~0.12 M |

### Vector Store

```nim
import boogie/stores/vectorstore
import std/options

let vs = newInMemoryVectorStore()  # or newVectorStore("vecdb", smDisk, enableWal = true)
vs.createCollection(newCollection("embeddings", 3))
vs.insert("embeddings", "doc-1", @[0.1'f32, 0.2, 0.3], "tenant-7")
vs.insert("embeddings", "doc-2", @[0.9'f32, 0.8, 0.7], "tenant-7")

echo vs.get("embeddings", "doc-1").isSome
let q = @[0.11'f32, 0.19, 0.31]
# scope the search to a partition to bound the scanned candidate set
for (pk, score) in vs.nearest("embeddings", q, k = 2, dmCosine, "tenant-7"):
  echo pk, " (", score, ")"
```

> Vectors can be grouped into named **partitions** (a locality scope, like a ring in
> [KoutenDB](https://github.com/puffball1567/koutendb)). A partition-scoped `nearest`
> scores only that partition's rows: with 20k vectors across 100 partitions it scans
> 200 instead of 20,000 (99% reduction) and runs ~50x faster in the benchmark.
> `collection.partitionSize("tenant-7")` reports the bounded candidate set size.

| Operation | ops/s |
|---|---|
| insert | ~3.22 M |
| get | ~6.68 M |
| delete | ~4.95 M |
| nearest (k=10, dim=32, 20k vectors) | ~2.17 K queries/s |
| nearest, partition-scoped (1/100 candidate set) | ~116 K queries/s |

### Columnar Store

```nim
import boogie/stores/columnar
import std/json

var s = openColumnarStore("analytics")

s.createTable(TableSchema(
  name: "events",
  primaryKey: "id",
  rowCount: 0,
  columns: @[
    ColumnSchema(name: "id", kind: ctInt64, nullable: false, codec: ccNone),
    ColumnSchema(name: "user", kind: ctString, nullable: false, codec: ccNone),
    ColumnSchema(name: "amount", kind: ctFloat64, nullable: false, codec: ccNone)
  ]
))

s.insertBatch("events", @[
  %*{"id": 1, "user": "alice", "amount": 10.5},
  %*{"id": 2, "user": "bob", "amount": 25.0}
])

# Projection + filter
let rows = s.scan("events", @["id", "user"], filters = @[
  Filter(column: "amount", op: foGt, value: newJFloat(10.0))
])

# Aggregation
let ag = s.aggregate("events", @[
  AggregateSpec(column: "", kind: akCount, alias: "cnt"),
  AggregateSpec(column: "amount", kind: akAvg, alias: "avg_amount")
])
echo ag["cnt"].getInt()          # 2
```

Columns are parsed from disk once and cached in memory, so repeated scans/filters
over the same columns are much faster than the first (cold) scan.

| Operation | ops/s |
|---|---|
| insert (batch) | ~0.37 M |
| scan (cold, first load) | ~0.43 M |
| scan (warm, cached) | ~3.01 M |
| filter (cached) | ~3.92 M |

### Graph Store

```nim
import boogie/stores/graphstore
import std/json

var gs = openGraphStore("graphdb")

var tx = beginTx(gs)
let alice = createNode(tx, @["Person"], %*{"name": "Alice"})
let bob   = createNode(tx, @["Person"], %*{"name": "Bob"})
discard createRelationship(tx, alice, bob, "KNOWS", %*{"since": 2024})
commit(tx)

echo gs.neighbors(alice)          # @[bob]
for r in gs.outgoing(alice, "KNOWS"):
  echo r.toId
for n in gs.findNodesByLabel("Person"):
  echo n.id
echo gs.traverseBfs(alice, maxDepth = 2).len

closeGraphStore(gs)
```

| Operation | ops/s |
|---|---|
| commit (nodes + rels) | ~0.11 M |
| getNode | ~6 M |
| neighbors | ~4 M |
| findNodesByLabel (5k nodes) | ~330 µs |

### Document Store

```nim
import boogie/stores/docstore
import std/json, std/options

var store = openDocumentStore("doctest", name = "documents", defaultEncoding = deJson)
store.insert("k1", %*{"name": "Alice", "age": 30})
store.upsert("k1", %*{"name": "Alice", "age": 31})

if store.hasKey("k1"):
  echo store.get("k1").get()["age"]   # 31
discard store.delete("k1")
store.checkpoint()
```

| Operation | ops/s |
|---|---|
| insert | ~0.61 M |
| get | ~1.99 M |
| lookup | ~4.88 M |
| delete | ~0.21 M |

> [!TIP]
> Run the full test + benchmark suites with `nimble test -d:release` (the `-d:release`
> flag is required for accurate benchmarks).

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/boogie/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/boogie/fork)

|  |  |
|---|---|
| <a href="https://opencode.ai/go?ref=BHMEEK48QX"><img src="https://github.com/openpeeps/pistachio/blob/main/.github/opencode.png" alt="OpenCode"></a> | Switch to **Open-Source LLMs** via OpenCode GO, choosing from a variety of powerful models such as DeepSeek, Qwen, Kimi, GLM-5, MiniMax, MiMo. 🍕 [Use our referral link to get started!](https://opencode.ai/go?ref=BHMEEK48QX)|

### 🎩 License
LGPLv3 license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
