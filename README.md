<p align="center">
  <img src="https://github.com/openpeeps/boogie/blob/main/.github/boogie.png" width="100px"><br>
  A stupid simple WAL-based embedded database in Nim 👑<br>
</p>

<p align="center">
  <code>nimble install boogie</code>
</p>

<p align="center">
  <a href="https://openpeeps.github.io/boogie">API reference</a><br>
  <img src="https://github.com/openpeeps/boogie/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/boogie/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- Red-black tree ordered storage of rows and Hash Tables for fast lookups
- Write-ahead log (WAL) for durability and crash recovery
- Simple API for inserting, updating, deleting, and querying records
- Configurable options for performance tuning, such as batch sizes and flush intervals
- In-memory or On-disk storage modes
- Primitive data types (`string`, `int`, `float`, `bool`, `json`, `null`)

>[!NOTE]
> Boogie is an experimental project mostly made with the chatbot for fun and learning. It is still in early stages, so expect data loss and breaking changes. Use at your own risk.

>[!NOTE]
> Fun fact, this package is using [pkg/rbtree](https://github.com/Nycto/RBTreeNim), a 11-year-old Nim package that implements a red-black tree. It is a great example of how the Nim ecosystem has some hidden gems that can be used in new projects. 📦

This can be used as a simple embedded database for your Nim applications. If you want, you can use [openpeeps/e2ee](https://github.com/openpeeps/e2ee) to encrypt the data before inserting it into Boogie database.

## Examples
Here is a simple example of how to use Boogie
```nim
import pkg/boogie

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

# no data pushed to main store file yet since WAL flush is pending
check db.getTable("users").get().isEmpty()

# flush the WAL to disk (when enabled)
db.checkpoint()

# Query the data
for row in db.getTable("users").get().allRows():
  for key, col in row[1]:
    echo fmt"{key}: {$col}"
```

Check the [tests](https://github.com/openpeeps/boogie/tree/main/src/boogie/tests) for more examples.


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
