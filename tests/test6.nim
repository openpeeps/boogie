import std/[unittest, os, times, options, json, strformat, monotimes]
import pkg/boogie/stores/docstore

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_docstore_tests_" & unique)
  createDir(result)

suite "DocumentStore":
  let root = testRoot()

  test "insert/get supports flexible per-document schema":
    var ds = openDocumentStore(root, "flex_schema")
    let docA = %*{"name": "ana", "age": 30}
    let docB = %*{"title": "post", "active": true, "meta": {"k": "v"}}

    ds.insert("u1", docA)
    ds.insert("p1", docB)

    check ds.len == 2
    check ds.get("u1").isSome
    check ds.get("p1").isSome
    check ds.get("u1").get["name"].getStr == "ana"
    check ds.get("p1").get["active"].getBool == true

  test "insert enforces unique key":
    var ds = openDocumentStore(root, "unique_key")
    let doc = %*{"a": 1}

    ds.insert("k1", doc)
    expect DocumentStoreError:
      ds.insert("k1", doc)

  test "upsert updates existing document and delete removes it":
    var ds = openDocumentStore(root, "upsert_delete")

    ds.upsert("k", %*{"v": 1})
    check ds.get("k").isSome
    check ds.get("k").get["v"].getInt == 1

    ds.upsert("k", %*{"v": 2})
    check ds.get("k").get["v"].getInt == 2

    check ds.delete("k") == true
    check ds.get("k").isNone
    check ds.delete("k") == false

  test "putObj/getObj roundtrip Nim object":
    type User = object
      name: string
      age: int

    var ds = openDocumentStore(root, "obj_roundtrip")
    let u = User(name: "geo", age: 42)

    ds.putObj("u1", u)
    let obj = ds.getObj("u1", User)

    check obj.isSome
    check obj.get.name == "geo"
    check obj.get.age == 42

  test "BSON encoding path works":
    var ds = openDocumentStore(root, "bson_mode")
    let doc = %*{"x": 123, "s": "ok", "arr": [1, 2, 3]}

    ds.insert("b1", doc, enc = deBson)
    let raw = ds.getBson("b1")

    check raw.isSome
    let decoded = fromBson(raw.get)
    check decoded == doc

  test "WAL replay restores state across reopen":
    block:
      var ds = openDocumentStore(root, "replay_state")
      ds.insert("a", %*{"n": 1}, sync = true, enc = deJson)
      ds.upsert("b", %*{"n": 2}, sync = true, enc = deBson)
      discard ds.delete("a", sync = true)

    block:
      let ds2 = openDocumentStore(root, "replay_state")
      check ds2.get("a").isNone
      check ds2.get("b").isSome
      check ds2.get("b").get["n"].getInt == 2

  test "sync=false keeps entries buffered until flush":
    var ds = openDocumentStore(root, "buffered_flush")
    ds.insert("x", %*{"v": 99}, sync = false)

    let beforeFlush = openDocumentStore(root, "buffered_flush")
    check beforeFlush.get("x").isNone

    ds.wal.flush()
    let afterFlush = openDocumentStore(root, "buffered_flush")
    check afterFlush.get("x").isSome
    check afterFlush.get("x").get["v"].getInt == 99

  test "checkpoint writes snapshot (.ddb) to disk":
    let name = "snapshot_manual_checkpoint"
    let ddbPath = (root / name).changeFileExt(".ddb")

    block:
      var ds = openDocumentStore(root, name, enableSnapshots = true)
      ds.insert("k1", %*{"v": 1}, sync = true)
      ds.checkpoint()

    check fileExists(ddbPath)

    block:
      let ds2 = openDocumentStore(root, name, enableSnapshots = true)
      check ds2.get("k1").isSome
      check ds2.get("k1").get["v"].getInt == 1

  test "auto snapshot triggers at checkpointEveryOps":
    let name = "snapshot_auto_checkpoint"
    let ddbPath = (root / name).changeFileExt(".ddb")

    var ds = openDocumentStore(
      root,
      name,
      enableSnapshots = true,
      checkpointEveryOps = 2'u32,
      walFlushEveryOps = 1'u32
    )
    
    check fileExists(ddbPath)
    
    ds.insert("a", %*{"n": 1}, sync = true)
    ds.insert("b", %*{"n": 2}, sync = true)

  test "snapshot + WAL tail recovery restores full state":
    let name = "snapshot_plus_wal_tail"

    block:
      var ds = openDocumentStore(
        root,
        name,
        enableSnapshots = true,
        checkpointEveryOps = 1000'u32,
        walFlushEveryOps = 1'u32
      )
      ds.insert("a", %*{"n": 1}, sync = true)
      ds.checkpoint() # 'a' in snapshot
      ds.insert("b", %*{"n": 2}, sync = true) # 'b' only in WAL tail

    block:
      let ds2 = openDocumentStore(root, name, enableSnapshots = true)
      check ds2.get("a").isSome
      check ds2.get("b").isSome
      check ds2.get("a").get["n"].getInt == 1
      check ds2.get("b").get["n"].getInt == 2

  test "disable snapshots keeps .ddb absent while WAL still recovers":
    let name = "no_snapshot_mode"
    let ddbPath = (root / name).changeFileExt(".ddb")

    block:
      var ds = openDocumentStore(root, name, enableSnapshots = false)
      ds.insert("k", %*{"v": 9}, sync = true)
      ds.checkpoint() # no-op for snapshot file
    check not fileExists(ddbPath)

    block:
      let ds2 = openDocumentStore(root, name, enableSnapshots = false)
      check ds2.get("k").isSome
      check ds2.get("k").get["v"].getInt == 9


  test "writeBSONDocument writes store document to disk with version":
    var ds = openDocumentStore(root, "bson_write_disk")
    let doc = %*{"x": 123, "s": "ok", "arr": [1, 2, 3]}
    ds.insert("b1", doc, enc = deBson, sync = true)

    let path = root / "b1.bson"
    if fileExists(path): removeFile(path)

    check ds.writeBSONDocument("b1", path, version = 7'i32) == true
    check fileExists(path)

    let bdoc = openBSONDocument(path)
    check bdoc.version == 7'i32
    check bdoc.toJsonNode() == doc

  test "writeBSONDocument returns false for missing key":
    var ds = openDocumentStore(root, "bson_write_missing_key")
    let path = root / "missing-key.bson"
    if fileExists(path): removeFile(path)

    check ds.writeBSONDocument("does-not-exist", path) == false
    check not fileExists(path)

  test "openBSONDocument imports BSON file into store and persists via WAL":
    let path = root / "import_me.bson"
    let srcDoc = newBSONDocument(%*{"name": "neo", "age": 33}, version = 2'i32)
    writeBSONDocument(path, srcDoc)

    block:
      var ds = openDocumentStore(root, "bson_import_store")
      check ds.openBSONDocument("u1", path, sync = true) == true
      check ds.get("u1").isSome
      check ds.get("u1").get["name"].getStr == "neo"
      check ds.get("u1").get["age"].getInt == 33

    block:
      let ds2 = openDocumentStore(root, "bson_import_store")
      check ds2.get("u1").isSome
      check ds2.get("u1").get["name"].getStr == "neo"

  test "openBSONDocument returns false for missing file":
    var ds = openDocumentStore(root, "bson_import_missing_file")
    let missing = root / ("missing_" & $getTime().toUnix() & ".bson")
    if fileExists(missing): removeFile(missing)
    check ds.openBSONDocument("k1", missing) == false

proc reportBench(name: string, ops: int, elapsedSec: float) =
  let safe = if elapsedSec <= 1e-12: 1e-12 else: elapsedSec
  let opsPerSec = float(ops) / safe
  echo fmt"[bench] {name}: ops={ops}, cpuTime={elapsedSec:.6f}s, ops/s={opsPerSec:.2f}"

suite "DocumentStore (core + benchmark)":
  let root = testRoot()

  test "insert/get supports flexible per-document schema":
    var ds = openDocumentStore(root, "flex_schema")
    let docA = %*{"name": "ana", "age": 30}
    let docB = %*{"title": "post", "active": true, "meta": {"k": "v"}}

    ds.insert("u1", docA)
    ds.insert("p1", docB)

    check ds.len == 2
    check ds.get("u1").isSome
    check ds.get("p1").isSome
    check ds.get("u1").get["name"].getStr == "ana"
    check ds.get("p1").get["active"].getBool

  test "insert enforces unique key":
    var ds = openDocumentStore(root, "unique_key")
    let doc = %*{"a": 1}
    ds.insert("k1", doc)
    expect DocumentStoreError:
      ds.insert("k1", doc)

  test "upsert updates and delete removes":
    var ds = openDocumentStore(root, "upsert_delete")
    ds.upsert("k", %*{"v": 1})
    check ds.get("k").isSome
    check ds.get("k").get["v"].getInt == 1

    ds.upsert("k", %*{"v": 2})
    check ds.get("k").get["v"].getInt == 2

    check ds.delete("k") == true
    check ds.get("k").isNone
    check ds.delete("k") == false

  test "putObj/getObj roundtrip Nim object":
    type User = object
      name: string
      age: int

    var ds = openDocumentStore(root, "obj_roundtrip")
    ds.putObj("u1", User(name: "geo", age: 42))

    let obj = ds.getObj("u1", User)
    check obj.isSome
    check obj.get.name == "geo"
    check obj.get.age == 42

  test "BSON encoding path works":
    var ds = openDocumentStore(root, "bson_mode")
    let doc = %*{"x": 123, "s": "ok", "arr": [1, 2, 3]}

    ds.insert("b1", doc, enc = deBson)
    let raw = ds.getBson("b1")
    check raw.isSome

    let decoded = fromBson(raw.get)
    check decoded == doc

  test "WAL replay restores state across reopen":
    block:
      var ds = openDocumentStore(root, "replay_state")
      ds.insert("a", %*{"n": 1}, sync = true, enc = deJson)
      ds.upsert("b", %*{"n": 2}, sync = true, enc = deBson)
      discard ds.delete("a", sync = true)

    block:
      let ds2 = openDocumentStore(root, "replay_state")
      check ds2.get("a").isNone
      check ds2.get("b").isSome
      check ds2.get("b").get["n"].getInt == 2

  test "sync=false keeps entries buffered until flush":
    var ds = openDocumentStore(root, "buffered_flush")
    ds.insert("x", %*{"v": 99}, sync = false)

    let beforeFlush = openDocumentStore(root, "buffered_flush")
    check beforeFlush.get("x").isNone

    ds.wal.flush()
    let afterFlush = openDocumentStore(root, "buffered_flush")
    check afterFlush.get("x").isSome
    check afterFlush.get("x").get["v"].getInt == 99

  test "checkpoint writes snapshot (.ddb) and allows restore":
    let name = "snapshot_manual_checkpoint"
    let ddbPath = (root / name).changeFileExt(".ddb")

    block:
      var ds = openDocumentStore(root, name, enableSnapshots = true)
      ds.insert("k1", %*{"v": 1}, sync = true)
      ds.checkpoint()

    check fileExists(ddbPath)

    block:
      let ds2 = openDocumentStore(root, name, enableSnapshots = true)
      check ds2.get("k1").isSome
      check ds2.get("k1").get["v"].getInt == 1

  test "disable snapshots keeps .ddb absent while WAL recovers":
    let name = "no_snapshot_mode"
    let ddbPath = (root / name).changeFileExt(".ddb")

    block:
      var ds = openDocumentStore(root, name, enableSnapshots = false)
      ds.insert("k", %*{"v": 9}, sync = true)
      ds.checkpoint()
    check not fileExists(ddbPath)

    block:
      let ds2 = openDocumentStore(root, name, enableSnapshots = false)
      check ds2.get("k").isSome
      check ds2.get("k").get["v"].getInt == 9

  test "writeBSONDocument and openBSONDocument integration":
    let exportPath = root / "export_doc.bson"

    block:
      var ds = openDocumentStore(root, "bson_io")
      ds.insert("b1", %*{"name": "neo", "age": 33}, enc = deBson, sync = true)
      if fileExists(exportPath): removeFile(exportPath)

      check ds.writeBSONDocument("b1", exportPath, version = 7'i32)
      check fileExists(exportPath)

      let bdoc = openBSONDocument(exportPath)
      check bdoc.version == 7'i32
      check bdoc.toJsonNode()["name"].getStr == "neo"

    block:
      var ds2 = openDocumentStore(root, "bson_io_import")
      check ds2.openBSONDocument("u1", exportPath, sync = true)
      check ds2.get("u1").isSome
      check ds2.get("u1").get["age"].getInt == 33

  test "benchmark insert throughput (cpuTime)":
    const N = 10000
    var ds = openDocumentStore(
      root,
      "bench_insert",
      enableSnapshots = false,
      walFlushEveryOps = 0'u32
    )

    let t0 = cpuTime()
    for i in 0 ..< N:
      ds.insert("k" & $i, %*{"i": i, "s": "value"}, sync = false, enc = deJson)
    ds.wal.flush()
    let dt = cpuTime() - t0

    check ds.len == N
    check dt >= 0.0
    reportBench("insert", N, dt)

  test "benchmark get throughput (cpuTime)":
    const N = 10000
    var ds = openDocumentStore(
      root,
      "bench_get",
      enableSnapshots = false,
      walFlushEveryOps = 0'u32
    )

    for i in 0 ..< N:
      ds.insert("k" & $i, %*{"i": i}, sync = false)
    ds.wal.flush()

    var sum = 0
    let t0 = cpuTime()
    for i in 0 ..< N:
      let v = ds.get("k" & $i)
      if v.isSome:
        sum += v.get["i"].getInt
    let dt = cpuTime() - t0

    check sum == ((N - 1) * N) div 2
    check dt >= 0.0
    reportBench("get", N, dt)

  test "benchmark upsert throughput (cpuTime)":
    const N = 8000
    var ds = openDocumentStore(
      root,
      "bench_upsert",
      enableSnapshots = false,
      walFlushEveryOps = 0'u32
    )

    for i in 0 ..< N:
      ds.insert("k" & $i, %*{"v": 0}, sync = false)
    ds.wal.flush()

    let t0 = cpuTime()
    for i in 0 ..< N:
      ds.upsert("k" & $i, %*{"v": i}, sync = false, enc = deBson)
    ds.wal.flush()
    let dt = cpuTime() - t0

    check ds.get("k123").isSome
    check ds.get("k123").get["v"].getInt == 123
    check dt >= 0.0
    reportBench("upsert", N, dt)