import std/options

import ../src/boogie/stores/kv

# The key/value store is the simplest building block: plain string keys
# holding string values, with optional write-ahead logging when you need
# the data to survive a restart. Here we use the in-memory flavor, so
# nothing touches the disk at all.

let store = newInMemoryKvStore()

# Writes are plain puts; reading back a missing key gives none instead
# of raising, which makes cache-style lookups easy to express.
store.put("greeting", "hello")
store.put("user:1", "alice")

if store.hasKey("greeting"):
  echo store.get("greeting").get()

# Deletes report whether anything was actually removed.
discard store.delete("user:1")

# Iteration order is unspecified, which is perfect for dumps and migrations.
for k, v in store.pairsUnordered:
  echo k, " -> ", v

# For a persistent store that still feels the same, open a disk-backed one
# instead: newKvStore("mydb", ksmDisk, enableWal = true). The API above
# does not change; durability and crash recovery come for free.
