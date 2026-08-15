import std/[unittest, os, options, strformat]
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------- thread helpers

type
  RdbWriter = object
    st: Store
    id: int
    n: int

  RdbReader = object
    st: Store
    id: int
    n: int

proc rdbWriter(a: RdbWriter) {.thread.} =
  for i in 0..<a.n:
    let pk = fmt"w{a.id}_{i}"
    a.st.insertRow("users", pk, row({
      "name": newTextValue(fmt"user{a.id}_{i}"),
      "age": newIntValue(i mod 60)
    }))
  # deletes must stay in sync with the writer's own inserts
  for i in 0..<a.n:
    discard a.st.deleteRow("users", fmt"w{a.id}_{i}")

proc rdbReader(a: RdbReader) {.thread.} =
  let t = a.st.getTable("users").get
  var misses = 0
  for i in 0..<a.n:
    if a.st.getRow("users", fmt"r{i}").isNone:
      inc misses
    discard t.where("age", newIntValue(i mod 60)).len
  echo "  rdb reader ", a.id, " misses=", misses

proc seedRdb(st: Store) =
  st.createTable(newTable(
    name = "users",
    primaryKey = "id",
    primaryKeyMode = pkmManual,
    columns = [
      newColumn("id", dtText, false),
      newColumn("name", dtText, false),
      newColumn("age", dtInt, false)]))

suite "rdbms concurrency (enableConcurrency = true)":
  test "concurrent insert/delete/get across writers and readers":
    const M = 4
    const N = 4000
    const Seed = 1000
    let st = newStore("", smInMemory, false, enableConcurrency = true)
    seedRdb(st)
    for i in 0..<Seed:
      st.insertRow("users", fmt"r{i}", row({
        "name": newTextValue(fmt"user{i}"),
        "age": newIntValue(i mod 60)}))

    var writers: array[M, Thread[RdbWriter]]
    for i in 0..<M:
      createThread(writers[i], rdbWriter, RdbWriter(st: st, id: i, n: N))
    var readers: array[2, Thread[RdbReader]]
    for i in 0..1:
      createThread(readers[i], rdbReader, RdbReader(st: st, id: i, n: N))
    joinThreads(writers)
    joinThreads(readers)

    let t = st.getTable("users").get
    var rows = 0
    for _ in t.allRows:
      inc rows
    check rows == Seed
    check st.getRow("users", "r0").isSome
    check not st.getRow("users", "w0_0").isSome
    st.close()
