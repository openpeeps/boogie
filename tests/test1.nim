import unittest, options, json, times, strformat, os, tables
import ../src/boogie/stores/rdbms

if dirExists("tests" / "data"):
  removeDir("tests" / "data")

discard existsOrCreateDir("tests" / "data")

suite "No WAL + memory store tests":
  var db: Store
  test "init database without WAL":
    let t00 = cpuTime()
    db = newStore("tests" / "data" / "bench1", smInMemory,
              enableWal = false,
              walFlushEveryOps = 1000'u32
            )
    let t0 = cpuTime() - t00
    echo fmt"Database opened in {t0:.3f} seconds"

  test "create table":
    if not db.hasTable("users"):
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

  const N = 100_000
  test "insert rows":
    let t0 = cpuTime()
    let d = %*{"index": 0}
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(d)
      }))
    let tInsert = cpuTime() - t0
    echo fmt"Insert: {tInsert:.3f} s for {N} rows"
    
    db.checkpoint() 
  test "lookup rows":
    let t1 = cpuTime()
    var hits = 0
    for i in 1..N:
      if db.getRow("users", $i).isSome:
        inc hits
    let tLookup = cpuTime() - t1
    echo fmt"Lookup: {tLookup:.3f} s for {N} gets (hits={hits})"

  test "ordered scan":
    let t2 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRows():
      inc scanned
    let tScan = cpuTime() - t2
    echo fmt"Ordered scan: {tScan:.3f} s for {scanned} rows"

  test "ordered scan #2":
    # unsorted via rowsByPk to show max read speed without order index
    let t4 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRowsByPk:
      inc scanned
    let tUnsortedScan = cpuTime() - t4
    echo fmt"Unsorted scan: {tUnsortedScan:.3f} s for {scanned} rows"

  test "where scan":
    let t5 = cpuTime()
    var matches = 0
    for v in db.getTable("users").get().where("age", newIntValue(25)):
      inc matches
    let tWhere = cpuTime() - t5
    echo fmt"Where scan: {tWhere:.3f} s for {matches} matches"

suite "No WAL + disk store tests":
  var db: Store
  test "init database without WAL":
    let t00 = cpuTime()
    db = newStore("tests" / "data" / "bench2", smDisk,
              enableWal = false,
              walFlushEveryOps = 1000'u32
            )
    let t0 = cpuTime() - t00
    echo fmt"Database opened in {t0:.3f} seconds"

  test "create table":
    if not db.hasTable("users"):
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

  const N = 100_000
  test "insert rows":
    let t0 = cpuTime()
    let d = %*{"index": 0}
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(d)
      }))
    let tInsert = cpuTime() - t0
    echo fmt"Insert: {tInsert:.3f} s for {N} rows"
    
    db.checkpoint() 
  test "lookup rows":
    let t1 = cpuTime()
    var hits = 0
    for i in 1..N:
      if db.getRow("users", $i).isSome:
        inc hits
    let tLookup = cpuTime() - t1
    echo fmt"Lookup: {tLookup:.3f} s for {N} gets (hits={hits})"

  test "ordered scan":
    let t2 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRows():
      inc scanned
    let tScan = cpuTime() - t2
    echo fmt"Ordered scan: {tScan:.3f} s for {scanned} rows"

  test "ordered scan #2":
    # unsorted via rowsByPk to show max read speed without order index
    let t4 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRowsByPk:
      inc scanned
    let tUnsortedScan = cpuTime() - t4
    echo fmt"Unsorted scan: {tUnsortedScan:.3f} s for {scanned} rows"

  test "where scan":
    let t5 = cpuTime()
    var matches = 0
    for v in db.getTable("users").get().where("age", newIntValue(25)):
      inc matches
    let tWhere = cpuTime() - t5
    echo fmt"Where scan: {tWhere:.3f} s for {matches} matches"

suite "WAL + disk store tests":
  var db: Store
  test "init database without WAL":
    let t00 = cpuTime()
    db = newStore("tests" / "data" / "bench3", smDisk,
              enableWal = true,
              walFlushEveryOps = 1000'u32
            )
    let t0 = cpuTime() - t00
    echo fmt"Database opened in {t0:.3f} seconds"

  test "create table":
    if not db.hasTable("users"):
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

  const N = 10_000
  test "insert rows":
    let t0 = cpuTime()
    let d = %*{"index": 0}
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(d)
      }))
    let tInsert = cpuTime() - t0
    echo fmt"Insert: {tInsert:.3f} s for {N} rows"
    
    db.checkpoint() 
  test "lookup rows":
    let t1 = cpuTime()
    var hits = 0
    for i in 1..N:
      if db.getRow("users", $i).isSome:
        inc hits
    let tLookup = cpuTime() - t1
    echo fmt"Lookup: {tLookup:.3f} s for {N} gets (hits={hits})"

  test "ordered scan":
    let t2 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRows():
      inc scanned
    let tScan = cpuTime() - t2
    echo fmt"Ordered scan: {tScan:.3f} s for {scanned} rows"

  test "ordered scan #2":
    # unsorted via rowsByPk to show max read speed without order index
    let t4 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRowsByPk:
      inc scanned
    let tUnsortedScan = cpuTime() - t4
    echo fmt"Unsorted scan: {tUnsortedScan:.3f} s for {scanned} rows"

  test "where scan":
    let t5 = cpuTime()
    var matches = 0
    for v in db.getTable("users").get().where("age", newIntValue(25)):
      inc matches
    let tWhere = cpuTime() - t5
    echo fmt"Where scan: {tWhere:.3f} s for {matches} matches"

suite "WAL + memory store tests":
  var db: Store
  test "init database without WAL":
    let t00 = cpuTime()
    db = newStore("tests" / "data" / "bench4", smInMemory,
              enableWal = true,
              walFlushEveryOps = 1000'u32
            )
    let t0 = cpuTime() - t00
    echo fmt"Database opened in {t0:.3f} seconds"

  test "create table":
    if not db.hasTable("users"):
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

  const N = 10_000
  test "insert rows":
    let t0 = cpuTime()
    let d = %*{"index": 0}
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(d)
      }))
    let tInsert = cpuTime() - t0
    echo fmt"Insert: {tInsert:.3f} s for {N} rows"
    
    db.checkpoint() 
  test "lookup rows":
    let t1 = cpuTime()
    var hits = 0
    for i in 1..N:
      if db.getRow("users", $i).isSome:
        inc hits
    let tLookup = cpuTime() - t1
    echo fmt"Lookup: {tLookup:.3f} s for {N} gets (hits={hits})"

  test "ordered scan":
    let t2 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRows():
      inc scanned
    let tScan = cpuTime() - t2
    echo fmt"Ordered scan: {tScan:.3f} s for {scanned} rows"

  test "ordered scan #2":
    # unsorted via rowsByPk to show max read speed without order index
    let t4 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRowsByPk:
      inc scanned
    let tUnsortedScan = cpuTime() - t4
    echo fmt"Unsorted scan: {tUnsortedScan:.3f} s for {scanned} rows"

  test "where scan":
    let t5 = cpuTime()
    var matches = 0
    for v in db.getTable("users").get().where("age", newIntValue(25)):
      inc matches
    let tWhere = cpuTime() - t5
    echo fmt"Where scan: {tWhere:.3f} s for {matches} matches"

suite "WAL functions tests":
  test "WAL crash recovery":
    var db = newStore("tests" / "data" / "crashbench", smDisk,
      enableWal = true,
      walFlushEveryOps = 100'u32
        # strict flush after every 100 ops to maximize chance of pending entries on crash
    )
    if not db.hasTable("users"):
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
    const N = 1000
    let d = %*{"index": 0}
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(d)
      }))
    # we simualte a crash by not calling `db.checkpoint`
    # so the data is only in the WAL and not yet flushed to the main store file

    # re-open and verify recovery
    db = newStore("tests" / "data" / "crashbench", smDisk,
      enableWal = true,
      walFlushEveryOps = 1000'u32
    )
    check db.hasTable("users")
    var count = 1
    for _ in db.getTable("users").get().allRows():
      inc count
    echo "Recovered rows: ", count
    check count == N


suite "Foreign key functionality tests":
  test "FK insert: valid parent passes, missing parent fails":
    let db = newStore("tests" / "data" / "fk_mem", smInMemory,
      enableWal = false
    )

    db.createTable(newTable(
      name = "users",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("name", DataType.dtText, false)
      ]
    ))

    db.createTable(newTable(
      name = "posts",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("user_id", DataType.dtInt, false),
        newColumn("title", DataType.dtText, false)
      ],
      foreignKeys = [
        newForeignKey("fk_posts_user", "user_id", "users", "id", fkaRestrict)
      ]
    ))

    discard db.insertRow("users", row({
      "name": newTextValue("George")
    }))

    discard db.insertRow("posts", row({
      "user_id": newIntValue(1),
      "title": newTextValue("hello")
    }))

    check db.getTable("posts").get().where("user_id", newIntValue(1)).len == 1

    expect(StoreError):
      discard db.insertRow("posts", row({
        "user_id": newIntValue(999),
        "title": newTextValue("invalid")
      }))

  test "FK delete RESTRICT blocks parent delete when children exist":
    let db = newStore("tests" / "data" / "fk_mem_restrict", smInMemory,
      enableWal = false
    )

    db.createTable(newTable(
      name = "users",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("name", DataType.dtText, false)
      ]
    ))

    db.createTable(newTable(
      name = "posts",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("user_id", DataType.dtInt, false),
        newColumn("title", DataType.dtText, false)
      ],
      foreignKeys = [
        newForeignKey("fk_posts_user", "user_id", "users", "id", fkaRestrict)
      ]
    ))

    discard db.insertRow("users", row({
      "name": newTextValue("Owner")
    }))
    discard db.insertRow("posts", row({
      "user_id": newIntValue(1),
      "title": newTextValue("owned")
    }))

    expect(StoreError):
      discard db.deleteRow("users", "1")

    check db.getRow("users", "1").isSome
    check db.getRow("posts", "1").isSome

  test "FK metadata + checks survive WAL recovery":
    let path = "tests" / "data" / "fk_wal"
    if dirExists(path):
      removeDir(path)

    var db = newStore(path, smDisk,
      enableWal = true,
      walFlushEveryOps = 1'u32
    )

    db.createTable(newTable(
      name = "users",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("name", DataType.dtText, false)
      ]
    ))

    db.createTable(newTable(
      name = "posts",
      primaryKey = "id",
      columns = [
        newColumn("id", DataType.dtInt, false),
        newColumn("user_id", DataType.dtInt, false),
        newColumn("title", DataType.dtText, false)
      ],
      foreignKeys = [
        newForeignKey("fk_posts_user", "user_id", "users", "id", fkaRestrict)
      ]
    ))

    discard db.insertRow("users", row({
      "name": newTextValue("Recovered")
    }))
    discard db.insertRow("posts", row({
      "user_id": newIntValue(1),
      "title": newTextValue("from wal")
    }))

    # Simulate restart without explicit checkpoint
    db = newStore(path, smDisk,
      enableWal = true,
      walFlushEveryOps = 1'u32
    )

    check db.hasTable("users")
    check db.hasTable("posts")
    check db.getRow("users", "1").isSome
    check db.getRow("posts", "1").isSome

    expect(StoreError):
      discard db.insertRow("posts", row({
        "user_id": newIntValue(404),
        "title": newTextValue("should fail")
      }))


suite "RDBMS Store benchmarks":
  test "rdbms ops/sec benchmark (insert/lookup/scan)":
    const N = 20000
    let db = newStore("tests" / "data" / "bench_rdbms_mem",
                smInMemory, enableWal = true, walFlushEveryOps = 0'u32)
    if not db.hasTable("users"):
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

    # Insert
    var t0 = cpuTime()
    for i in 1..N:
      db.insertRow("users", row({
        "name": newTextValue("User"),
        "age": newIntValue(20 + (i mod 30)),
        "active": newBoolValue((i and 1) == 0),
        "meta": newJSONValue(%*{"index": i})
      }))
    let insertSecs = cpuTime() - t0

    # Lookup
    t0 = cpuTime()
    var hits = 0
    for i in 1..N:
      if db.getRow("users", $i).isSome:
        inc hits
    let lookupSecs = cpuTime() - t0

    # Scan
    t0 = cpuTime()
    var scanned = 0
    for _ in db.getTable("users").get().allRows():
      inc scanned
    let scanSecs = cpuTime() - t0

    let insertOps = float(N) / max(insertSecs, 1e-9)
    let lookupOps = float(N) / max(lookupSecs, 1e-9)
    let scanOps = float(N) / max(scanSecs, 1e-9)

    echo fmt"[bench][rdbms][mem] insert={insertOps:>10.0f} ops/s lookup={lookupOps:>10.0f} ops/s scan={scanOps:>10.0f} ops/s"

    check insertOps > 0
    check lookupOps > 0
    check scanOps > 0

    