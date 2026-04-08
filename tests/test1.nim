import unittest, options, json, times, strformat, os
import ../src/boogie

discard existsOrCreateDir("tests" / "data")
for p in walkDir("tests" / "data"):
  removeFile(p.path)

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