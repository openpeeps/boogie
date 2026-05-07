import std/[unittest, os, times, options, json]
import pkg/boogie/stores/docstore
import pkg/openparser/bson

proc testRoot(): string =
  result = getTempDir() / ("boogie_docstore_tests_" & $getTime().toUnix())
  if not dirExists(result):
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