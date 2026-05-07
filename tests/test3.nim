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