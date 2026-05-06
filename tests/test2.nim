import unittest, options, json, times, strformat, os, tables
import ../src/boogie/stores/kvstore

# discard existsOrCreateDir("tests" / "data")
# for p in walkDir("tests" / "data"):
#   removeFile(p.path)

suite "WAL + memory store tests":
  test "in-memory put/get/delete":
    let kv = newInMemoryKvStore()
    check kv.isEmpty
    kv.put("foo", "bar")
    check kv.len == 1
    check kv.get("foo").get() == "bar"
    check kv.hasKey("foo")
    kv.put("foo", "baz")
    check kv.get("foo").get() == "baz"
    check kv.delete("foo")
    check not kv.hasKey("foo")
    check kv.isEmpty

  test "disk store with WAL and recovery":
    let dbPath = "test_kvstore"
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & ".db"): removeFile(dbPath & ".db")
    if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

    block:
      let kv = newKvStore(dbPath, ksmDisk, enableWal = true, checkpointEveryOps = 2)
      kv.put("a", "1")
      kv.put("b", "2")
      kv.put("c", "3")
      check kv.get("a").get() == "1"
      check kv.get("b").get() == "2"
      check kv.get("c").get() == "3"
      kv.delete("b")
      check not kv.hasKey("b")
      kv.checkpoint()

    block:
      let kv2 = newKvStore(dbPath, ksmDisk, enableWal = true)
      check kv2.get("a").get() == "1"
      check kv2.get("c").get() == "3"
      check not kv2.hasKey("b")
      kv2.put("d", "4")
      check kv2.get("d").get() == "4"

    # Cleanup
    for ext in ["", ".db", ".wal"]:
      if fileExists(dbPath & ext): removeFile(dbPath & ext)

  test "iterator pairsUnordered":
    let kv = newInMemoryKvStore()
    kv.put("x", "1")
    kv.put("y", "2")
    var keys: seq[string]
    for k, v in kv.pairsUnordered:
      keys.add(k)
      check v in ["1", "2"]
    check keys.len == 2