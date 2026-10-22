import std/[strutils, options, tables]

import ../src/boogie/stores/rdbms

# A small blog model shows off the relational store: typed tables with a
# primary key, a foreign key from posts back to users, and an index for
# name lookups. The store below is disk-backed with a write-ahead log,
# so everything committed here survives a restart.

let userIdCol = newColumn("id", dtInt, false)
let userNameCol = newColumn("name", dtText, false)
let userEmailCol = newColumn("email", dtText, true)

# Columns can declare a default as raw JSON; inserts that omit the column
# get the default value instead of failing the NOT NULL check.
let userRoleCol = ColumnDef(name: "role", kind: dtText, nullable: false,
                            defaultValue: "\"member\"")

let postIdCol = newColumn("id", dtInt, false)
let postUserIdCol = newColumn("user_id", dtInt, false)
let postContentCol = newColumn("content", dtText, false)

let users = newTable(
  name = "users",
  primaryKey = "id",
  columns = [userIdCol, userNameCol, userEmailCol, userRoleCol],
  primaryKeyMode = pkmSerial
)

let fkUser = newForeignKey(
  name = "fk_post_user",
  column = "user_id",
  refTable = "users",
  refColumn = "id"
)

let posts = newTable(
  name = "posts",
  primaryKey = "id",
  columns = [postIdCol, postUserIdCol, postContentCol],
  primaryKeyMode = pkmSerial,
  foreignKeys = [fkUser]
)

# Checkpoint every ten mutations keeps the snapshot fresh while the WAL
# still batches writes between checkpoints.
let store = newStore("blogdata", smDisk, enableWal = true, checkpointEveryOps = 10)

# Register tables (idempotent)
store.createTableIfNotExist(users)
store.createTableIfNotExist(posts)

# IMPORTANT: use live table refs from store
let usersTOpt = store.getTable("users")
let postsTOpt = store.getTable("posts")
if usersTOpt.isNone or postsTOpt.isNone:
  raise newException(StoreError, "failed to fetch tables from store")

let usersT = usersTOpt.get()
let postsT = postsTOpt.get()

# Insert a user (auto-increment PK). The missing role falls back to its
# default, so Alice becomes a plain member with no extra code.
let userPk = store.insertRow("users", row({
  "name": newTextValue("Alice"),
  "email": newTextValue("alice@example.com")
}))

# A primary key can also be chosen explicitly when the rows already have
# natural ids; the serial sequence moves past it automatically.
store.insertRow("users", "42", row({
  "name": newTextValue("Bob")
}))

# Insert a post for Alice
let postPk = store.insertRow("posts", row({
  "user_id": newIntValue(userPk.parseBiggestInt.int64),
  "content": newTextValue("Hello, world!")
}))

# Point lookups by primary key are a single hash probe.
let alice = store.getRow("users", userPk)
if alice.isSome:
  echo "Alice: ", alice.get()

# Query users by name (with index)
usersT.createIndex("name")
for (pk, r) in usersT.where("name", newTextValue("Alice")):
  echo "User: ", pk, " -> ", r

# Query all posts for Alice
for (pk, r) in postsT.where("user_id", newIntValue(userPk.parseBiggestInt.int64)):
  echo "Post: ", pk, " -> ", r

# Updates replace the whole row: read it, change what you need, write it
# back under the same key. Everything else on the row is preserved.
var edited = store.getRow("posts", postPk).get()
edited["content"] = newTextValue("Hello, edited world!")
store.updateRow("posts", postPk, edited)
echo "Edited: ", store.getRow("posts", postPk).get()

# Ordered iteration walks the table in primary key order, which is handy
# for dumps and for paging through small tables.
for (pk, r) in usersT.allRows:
  echo "Row: ", pk, " -> ", r

# FK is RESTRICT: delete child rows first
for (pk, _) in postsT.where("user_id", newIntValue(userPk.parseBiggestInt.int64)):
  discard store.deleteRow("posts", pk)

# Now delete user
discard store.deleteRow("users", userPk)
echo "Has Alice: ", store.getRow("users", userPk).isSome

# Force a checkpoint (snapshot)
store.checkpoint()
