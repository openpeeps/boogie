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
- Write-ahead log (WAL) for durability and crash recovery
- Simple API for inserting, updating, deleting, and querying records
- Configurable options for performance tuning, such as batch sizes and flush intervals
- In-memory or On-disk storage modes
- Primitive data types (`string`, `int`, `float`, `bool`, `json`, `null`)

What's included?
- **Key/Value Store** &mdash; A simple key-value store implementation with WAL support
- **RDBMS Store** &mdash; A relational wal-based database with support for **schemas**, **primary keys** and typed columns (However, it currently lacks explicit support for foreign keys, joins, or advanced relational features (relations between tables)
- **Vector Store** &mdash; Vector store implementation with WAL support
- **Columnar Store** &mdash; Columnar storage engine for analytics workloads with WAL support
- **Graph Store** &mdash; A simple graph database with support for nodes, relationships, and basic graph queries (e.g., neighbors, shortest path) with WAL support

>[!NOTE]
> Boogie is an experimental project mostly made with the chatbot for fun and learning. It is still in early stages, so expect data loss and breaking changes. Use at your own risk.

This can be used as a simple embedded database for your Nim applications. If you want, you can use [openpeeps/e2ee](https://github.com/openpeeps/e2ee) to encrypt the data before inserting it into Boogie database.

## Examples
Here is a simple example of how to use Boogie

### RDBMS Store
Here is an example of using the RDBMS store to create a table, insert some data, and query it:
```nim
import boogie/stores/rdbms

var db = newStore("tests" / "data" / "myboogie.db", StorageMode.smDisk,
            enableWal = true, walFlushEveryOps = 100'u32)

# Create a table with some columns
db.createTable(newTable(
  name = "users",
  primaryKey = "id",
  columns = [
    newColumn("id", DataType.dtInt, false),
    newColumn("name", DataType.dtText, false),
    newColumn("age", DataType.dtInt, false),
    newColumn("active", DataType.dtBool, false),
    newColumn("meta", DataType.dtJson, true)
  ]
))

# insert some data
db.insertRow("users", row({
  "name": newTextValue("Alice"),
  "age": newIntValue(30),
  "active": newBoolValue(true),
  "meta": newJsonValue(%*{"hobbies": ["reading", "hiking"]})
}))

# flush the WAL to disk (when enabled)
db.checkpoint()

# Query the data
for row in db.getTable("users").get().allRows():
  for key, col in row[1]:
    echo fmt"{key}: {$col}"
```

### Key/Value Store
Boogie also provides a simple key-value store implementation with WAL support.
```nim
import boogie/stores/kv

let kv = newKVStore("./mykv.db", StorageMode.ksmDisk,
            enableWal = true,
            checkpointEveryOps = 50'u32)

kv.put("name", "Alice")
assert kv.get("name") == "Alice"

kv.delete("name")
assert kv.hasKey("name") == false
```

### Vector Store


### Columnar Store


### Graph Store

>[!NOTE]
>Check the [tests](https://github.com/openpeeps/boogie/tree/main/src/boogie/tests) for more examples.

## Benchmarks
Here you can find some benchmarks for the available stores. You can run it yourself by cloning the repo and running `nimble test -d:release` (note `-d:release` flag is required for accurate benchmarks)

#### RDBMS Store Benchmarks
```
[Suite] No WAL + memory store tests
Database opened in 0.000 seconds
  [OK] init database without WAL
  [OK] create table
Insert: 0.594 s for 100000 rows
  [OK] insert rows
Lookup: 0.076 s for 100000 gets (hits=100000)
  [OK] lookup rows
Ordered scan: 0.978 s for 100000 rows
  [OK] ordered scan
Unsorted scan: 0.067 s for 100000 rows
  [OK] ordered scan #2
Where scan: 0.065 s for 3334 matches
  [OK] where scan

[Suite] No WAL + disk store tests
Database opened in 0.000 seconds
  [OK] init database without WAL
  [OK] create table
Insert: 0.502 s for 100000 rows
  [OK] insert rows
Lookup: 0.108 s for 100000 gets (hits=100000)
  [OK] lookup rows
Ordered scan: 0.754 s for 100000 rows
  [OK] ordered scan
Unsorted scan: 0.103 s for 100000 rows
  [OK] ordered scan #2
Where scan: 0.081 s for 3334 matches
  [OK] where scan

[Suite] WAL + disk store tests
Database opened in 0.001 seconds
  [OK] init database without WAL
  [OK] create table
Insert: 0.056 s for 10000 rows
  [OK] insert rows
Lookup: 0.010 s for 10000 gets (hits=10000)
  [OK] lookup rows
Ordered scan: 0.075 s for 10000 rows
  [OK] ordered scan
Unsorted scan: 0.010 s for 10000 rows
  [OK] ordered scan #2
Where scan: 0.008 s for 334 matches
  [OK] where scan

[Suite] WAL + memory store tests
Database opened in 0.000 seconds
  [OK] init database without WAL
  [OK] create table
Insert: 0.046 s for 10000 rows
  [OK] insert rows
Lookup: 0.010 s for 10000 gets (hits=10000)
  [OK] lookup rows
Ordered scan: 0.076 s for 10000 rows
  [OK] ordered scan
Unsorted scan: 0.010 s for 10000 rows
  [OK] ordered scan #2
Where scan: 0.008 s for 334 matches
  [OK] where scan

[Suite] WAL functions tests
Recovered rows: 1000
  [OK] WAL crash recovery
```

As you can see, the performance of the RDBMS store is quite good, especially when using the WAL. The in-memory store is faster than the disk store, but the disk store provides durability and crash recovery. The WAL also significantly improves the performance of inserts and lookups.

#### Key/Value Store Benchmarks
todo

### Todos
- [x] Add support for multiple tables
- [x] Add basic tests and benchmarks


### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/boogie/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/boogie/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
LGPLv3 license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
