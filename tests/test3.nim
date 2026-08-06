import std/[unittest, os, sequtils, options, times, strformat]
import ../src/boogie/stores/vectorstore

const testDir = "tests/data_vector"
discard existsOrCreateDir(testDir)
for p in walkDir(testDir):
  removeFile(p.path)

suite "VectorStore basic API":
  test "create in-memory vector store and collection":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 3)
    vs.createCollection(coll)
    check vs.hasCollection("embeddings")

  test "insert and get vector":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 3)
    vs.createCollection(coll)
    vs.insert("embeddings", "id1", @[1.0'f32, 2.0, 3.0])
    let v = vs.get("embeddings", "id1")
    check v.isSome
    check v.get == @[1.0'f32, 2.0, 3.0]

  test "delete vector":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    vs.insert("embeddings", "id1", @[1.0'f32, 2.0])
    check vs.delete("embeddings", "id1")
    check vs.get("embeddings", "id1").isNone

  test "dimension mismatch raises":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    expect VectorStoreError:
      vs.insert("embeddings", "id1", @[1.0'f32, 2.0, 3.0])

suite "VectorStore nearest neighbor search":
  test "nearest neighbor (cosine)":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    vs.insert("embeddings", "a", @[1.0'f32, 0.0])
    vs.insert("embeddings", "b", @[0.0'f32, 1.0])
    vs.insert("embeddings", "c", @[0.7'f32, 0.7])
    let res = vs.nearest("embeddings", @[1.0'f32, 0.0], 2, dmCosine)
    check res.len == 2
    check res[0][0] == "a"
    check res[1][0] == "c" or res[1][0] == "b"

  test "partition-scoped nearest bounds the candidate set":
    let vs = newInMemoryVectorStore()
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    vs.insert("embeddings", "a", @[1.0'f32, 0.0], "t1")
    vs.insert("embeddings", "b", @[0.0'f32, 1.0], "t2")
    vs.insert("embeddings", "c", @[0.8'f32, 0.8], "t1")

    let c = vs.getCollection("embeddings").get()
    check c.len == 3
    check c.partitionSize("t1") == 2
    check c.partitionSize("t2") == 1

    # global scan sees all three
    check vs.nearest("embeddings", @[1.0'f32, 0.0], 3, dmCosine).len == 3
    # t1-scoped scan sees only t1 rows, best first
    let res = vs.nearest("embeddings", @[1.0'f32, 0.0], 3, dmCosine, "t1")
    check res.len == 2
    check res[0][0] == "a"
    check res[1][0] == "c"
    # unknown partition -> empty
    check vs.nearest("embeddings", @[1.0'f32, 0.0], 3, dmCosine, "nope").len == 0

    # deletes keep the partition index in sync (incl. swap-with-last)
    check vs.delete("embeddings", "c")
    check c.partitionSize("t1") == 1
    check vs.delete("embeddings", "a")
    check c.partitionSize("t1") == 0
    check c.partitionSize("t2") == 1
    check c.get("b").isSome

  test "partitions survive WAL replay and snapshot reload":
    let path = testDir / "vecpart"
    block:
      var vs = newVectorStore(path, smDisk, enableWal = true, checkpointEveryOps = 1)
      vs.createCollection(newCollection("embeddings", 2))
      vs.insert("embeddings", "a", @[1.0'f32, 0.0], "t1")
      vs.insert("embeddings", "b", @[0.0'f32, 1.0], "t2")
      vs.checkpoint()
    block:
      let vs = newVectorStore(path, smDisk, enableWal = true)
      let c = vs.getCollection("embeddings").get()
      check c.partitionSize("t1") == 1
      check c.partitionSize("t2") == 1
      check vs.nearest("embeddings", @[1.0'f32, 0.0], 2, dmCosine, "t1").len == 1

suite "VectorStore WAL/snapshot recovery":
  test "disk WAL + recovery":
    let path = testDir / "vecwal"
    var vs = newVectorStore(path, smDisk, enableWal=true, walFlushEveryOps=1)
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    vs.insert("embeddings", "x", @[1.0'f32, 2.0])
    vs.insert("embeddings", "y", @[2.0'f32, 1.0])
    # don't checkpoint, force WAL-only
    vs = newVectorStore(path, smDisk, enableWal=true, walFlushEveryOps=1)
    check vs.hasCollection("embeddings")
    check vs.get("embeddings", "x").isSome
    check vs.get("embeddings", "y").isSome

  test "disk snapshot":
    let path = testDir / "vecsnap"
    var vs = newVectorStore(path, smDisk, enableWal=true, checkpointEveryOps=1)
    let coll = newCollection("embeddings", 2)
    vs.createCollection(coll)
    vs.insert("embeddings", "z", @[3.0'f32, 4.0])
    vs.checkpoint()
    vs = newVectorStore(path, smDisk, enableWal=true)
    check vs.get("embeddings", "z").isSome


suite "VectorStore benchmarks":
  test "vectorstore ops/sec benchmark (insert/get/del)":
    const N = 20000
    let collName = "bench"
    let dim = 8
    let vs = newInMemoryVectorStore()
    let coll = newCollection(collName, dim)
    vs.createCollection(coll)

    # Insert
    var t0 = cpuTime()
    for i in 0..<N:
      vs.insert(collName, "id" & $i, newSeqWith(dim, float32(i)))
    let insertSecs = cpuTime() - t0

    # Get
    t0 = cpuTime()
    for i in 0..<N:
      discard vs.get(collName, "id" & $i)
    let getSecs = cpuTime() - t0

    # Delete
    t0 = cpuTime()
    for i in 0..<N:
      discard vs.delete(collName, "id" & $i)
    let delSecs = cpuTime() - t0

    let insertOps = float(N) / max(insertSecs, 1e-9)
    let getOps = float(N) / max(getSecs, 1e-9)
    let delOps = float(N) / max(delSecs, 1e-9)

    echo fmt"[bench][vectorstore] insert={insertOps:>10.0f} ops/s get={getOps:>10.0f} ops/s del={delOps:>10.0f} ops/s"

    check insertOps > 0
    check getOps > 0
    check delOps > 0

  test "nearest neighbor search throughput":
    const N = 20000
    const dim = 32
    const K = 10
    const Q = 200
    let collName = "bench"
    let vs = newInMemoryVectorStore()
    vs.createCollection(newCollection(collName, dim))

    for i in 0..<N:
      var vec = newSeq[float32](dim)
      for d in 0..<dim:
        vec[d] = float32((i * 31 + d * 7) mod 1000) / 100.0'f32
      vs.insert(collName, "id" & $i, vec)

    var t0 = cpuTime()
    for q in 0..<Q:
      var query = newSeq[float32](dim)
      for d in 0..<dim:
        query[d] = float32((q * 17 + d * 13) mod 1000) / 100.0'f32
      discard vs.nearest(collName, query, K, dmCosine)
    let secs = cpuTime() - t0
    let ops = float(Q) / max(secs, 1e-9)

    echo fmt"[bench][vectorstore] nearest(k={K}, dim={dim}, n={N})= {ops:>10.0f} queries/s"

    check ops > 0

  test "partition-scoped nearest reduces scanned vectors":
    const N = 20000
    const P = 100
    const dim = 32
    const K = 10
    const Q = 200
    let collName = "bench-part"
    let vs = newInMemoryVectorStore()
    vs.createCollection(newCollection(collName, dim))

    for i in 0..<N:
      var vec = newSeq[float32](dim)
      for d in 0..<dim:
        vec[d] = float32((i * 31 + d * 7) mod 1000) / 100.0'f32
      vs.insert(collName, "id" & $i, vec, "p" & $(i mod P))

    var t0 = cpuTime()
    for q in 0..<Q:
      var query = newSeq[float32](dim)
      for d in 0..<dim:
        query[d] = float32((q * 17 + d * 13) mod 1000) / 100.0'f32
      discard vs.nearest(collName, query, K, dmCosine)
    let globalSecs = cpuTime() - t0
    let globalRate = float(Q) / max(globalSecs, 1e-9)

    t0 = cpuTime()
    for q in 0..<Q:
      var query = newSeq[float32](dim)
      for d in 0..<dim:
        query[d] = float32((q * 17 + d * 13) mod 1000) / 100.0'f32
      discard vs.nearest(collName, query, K, dmCosine, "p" & $(q mod P))
    let partSecs = cpuTime() - t0
    let partRate = float(Q) / max(partSecs, 1e-9)

    let c = vs.getCollection(collName).get()
    let scannedGlobal = c.len
    let scannedPart = c.partitionSize("p0")
    let reduction = 1.0 - float(scannedPart) / float(scannedGlobal)

    echo fmt"[bench][vectorstore] nearest global={globalRate:>9.0f} q/s part={partRate:>9.0f} q/s scanned {scannedGlobal}->{scannedPart} ({reduction * 100:>5.1f}% reduction)"
    check globalRate > 0
    check partRate > 0
    check scannedPart < scannedGlobal