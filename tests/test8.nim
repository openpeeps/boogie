import std/[unittest, os, options, strformat, strutils]
import ../src/boogie/stores/vectorstore

# ---------------------------------------------------------------- thread helpers

type
  VecWriter = object
    vs: VectorStore
    id: int
    n: int
    collection: string

  VecReader = object
    vs: VectorStore
    id: int
    n: int
    collection: string

proc vecWriter(a: VecWriter) {.thread.} =
  for i in 0..<a.n:
    a.vs.insert(a.collection, fmt"w{a.id}_{i}", @[float32(i), float32(a.id)], "p" & $a.id)

proc vecReader(a: VecReader) {.thread.} =
  # Reads against a live collection while other threads keep writing to it.
  var misses = 0
  for i in 0..<a.n:
    if a.vs.get(a.collection, fmt"r{i}").isNone:
      inc misses
    discard a.vs.nearest(a.collection, @[1.0'f32, 0.0], 5, dmCosine)
  echo "  vec reader ", a.id, " misses=", misses

suite "vectorstore concurrency (enableConcurrency = true)":
  test "concurrent insert/get/nearest across writers and readers":
    const M = 4
    const N = 4000
    const Seed = 1000
    let vs = newVectorStore("", smInMemory, false, enableConcurrency = true)
    vs.createCollection(newCollection("a", 2))
    vs.createCollection(newCollection("b", 2))
    for i in 0..<Seed:
      vs.insert("a", fmt"r{i}", @[float32(i mod 7), float32(i mod 11)])
      vs.insert("b", fmt"r{i}", @[float32(i mod 5), float32(i mod 13)])

    var writers: array[M, Thread[VecWriter]]
    for i in 0..<M:
      createThread(writers[i], vecWriter,
        VecWriter(vs: vs, id: i, n: N, collection: if i mod 2 == 0: "a" else: "b"))
    var readers: array[2, Thread[VecReader]]
    for i in 0..1:
      createThread(readers[i], vecReader,
        VecReader(vs: vs, id: i, n: N, collection: if i mod 2 == 0: "a" else: "b"))
    joinThreads(writers)
    joinThreads(readers)

    for name in ["a", "b"]:
      let c = vs.getCollection(name).get
      check c.len == Seed + (M * N) div 2
      # every writer row is visible (synchronous visibility)
      for i in 0..<M:
        if (if i mod 2 == 0: "a" else: "b") == name:
          let last = N - 1
          check vs.get(name, fmt"w{i}_{last}").isSome
    vs.close()

suite "vectorstore crash-safety WAL flush (concurrent)":
  test "concurrent inserts survive a close-time WAL flush":
    const N = 4000
    const M = 4
    let path = "tests" / "data" / "vec_concurrent"
    discard existsOrCreateDir("tests" / "data")
    for p in walkDir("tests" / "data"):
      if p.path.endsWith("vec_concurrent.wal"):
        removeFile(p.path)
    block:
      let vs = newVectorStore(path, smDisk, enableWal = true, walFlushEveryOps = 100000,
        enableConcurrency = true)
      vs.createCollection(newCollection("embeddings", 2))
      var writers: array[M, Thread[VecWriter]]
      for i in 0..<M:
        createThread(writers[i], vecWriter,
          VecWriter(vs: vs, id: i, n: N, collection: "embeddings"))
      joinThreads(writers)
      vs.close()
    block:
      let vs = newVectorStore(path, smDisk, enableWal = true, enableConcurrency = true)
      let c = vs.getCollection("embeddings").get
      check c.len == M * N
      check vs.get("embeddings", "w0_0").isSome
      vs.close()
