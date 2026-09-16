import std/[unittest, os, times, monotimes, strutils, options]
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# WAL size bounds: a checkpoint must also clear the .wal file so it cannot
# grow without bound.
# - single-threaded stores truncate the log after the snapshot checkpoint;
# - concurrent (WAL-only) stores rewrite the log from live state once it
#   reaches `walMaxBytes`.
# Run with: clue test
# ---------------------------------------------------------------------------

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_walcap_" & unique)
  createDir(result)

proc countRows(db: Store, table: string): int =
  for _ in db.getTable(table).get.allRows:
    inc result

suite "wal bounds: checkpoint clears the log":
  test "non-concurrent checkpoint truncates the WAL, LSNs stay monotonic":
    let root = testRoot()
    let dbFile = root / "cap"
    block:
      var db = newStore(dbFile, smDisk, enableWal = true,
                        walFlushEveryOps = 1000'u32)
      db.createTable(newTable(
        name = "items",
        primaryKey = "id",
        primaryKeyMode = pkmManual,
        columns = [
          newColumn("id", dtText, false),
          newColumn("v", dtText, false)]))
      for i in 0 ..< 300:
        db.insertRow("items", "k" & $i, row({
          "id": newTextValue("k" & $i),
          "v": newTextValue("value-" & $i & "-0123456789abcdef")}))
      db.checkpoint()
      # snapshot holds the state; the log must be back to a bare header
      check getFileSize(dbFile.changeFileExt(".wal")) < 1024
      check db.getRow("items", "k0").isSome
      db.close()
    block:
      # snapshot + empty WAL recovers fully
      var db = newStore(dbFile, smDisk, enableWal = true)
      check countRows(db, "items") == 300
      # post-truncation appends reuse low LSNs; they must still replay
      for i in 300 ..< 350:
        db.insertRow("items", "k" & $i, row({
          "id": newTextValue("k" & $i),
          "v": newTextValue("value-" & $i)}))
      db.checkpoint()
      check getFileSize(dbFile.changeFileExt(".wal")) < 1024
      db.close()
    block:
      var db = newStore(dbFile, smDisk, enableWal = true)
      check countRows(db, "items") == 350
      check db.getRow("items", "k349").isSome
      db.close()

  test "concurrent checkpoint compacts the WAL under a size budget":
    let root = testRoot()
    let dbFile = root / "ccap"
    block:
      var db = newStore(dbFile, smDisk, enableWal = true,
                        walFlushEveryOps = 50'u32,
                        walMaxBytes = 8192'u32,
                        enableConcurrency = true)
      db.createTable(newTable(
        name = "docs",
        primaryKey = "id",
        columns = [
          newColumn("id", dtInt, false),
          newColumn("body", dtText, false)]))
      for i in 0 ..< 400:
        discard db.insertRow("docs", row({
          "body": newTextValue("payload-" & $i & "-0123456789abcdef")}))
      for i in 1 .. 200:
        discard db.deleteRow("docs", $i)
      let before = getFileSize(dbFile.changeFileExt(".wal"))
      db.checkpoint()
      let after = getFileSize(dbFile.changeFileExt(".wal"))
      # dead entries (deleted rows + their deletes) are gone from the log
      check after < before
      check after < 1024 * 1024
      check countRows(db, "docs") == 200
      # the store stays usable and the serial sequence survives compaction
      check db.insertRow("docs",
        row({"body": newTextValue("after-compact")})) == "401"
      db.close()
    block:
      var db = newStore(dbFile, smDisk, enableWal = true,
                        walMaxBytes = 8192'u32,
                        enableConcurrency = true)
      check countRows(db, "docs") == 201
      check db.getRow("docs", "401").isSome
      check db.getRow("docs", "1").isNone
      db.close()
