# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | LGPL-3.0-or-later License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[tables, options, strformat, strutils, json, os, math, algorithm]
import pkg/[flatty, sorta]
import ../wal

## A simple vector store implementation with optional disk persistence and write-ahead logging (WAL) for 
## durability. The vector store supports multiple named collections, each with a specified dimension
## for the vectors.
## 
## Vectors are stored in-memory in an ordered table for fast lookups, and can be persisted to
## disk as a snapshot. The WAL allows for durability and crash recovery by logging all changes
## to the vector store before they are applied, enabling the store to be reconstructed after a
## crash by replaying the WAL entries.

type
  VectorStorageMode* = enum
    ## Defines the storage mode for the vector store,
    ## which can be either in-memory or disk-based.
    smInMemory, smDisk

  DistanceMetric* = enum
    ## Defines the distance metric to use for nearest neighbor search. This allows the `nearest`
    ## procedure to compute similarity or distance between vectors in different ways, depending on the use case
    ## - `dmCosine`: cosine similarity, where higher is more similar (range -1 to 1)
    ## - `dmDot`: dot product, where higher is more similar (unbounded)
    ## - `dmL2`: L2 distance (squared), where lower is more similar (range 0 to inf)
    dmCosine, dmDot, dmL2

  VectorCollection* {.acyclic.} = ref object
    ## Represents a collection of vectors, identified by a name and a fixed dimension.
    ## Each vector is associated with a primary key (pk) for lookup.
    ##
    ## Vectors are stored in a single flat `vecs` buffer (stride = `dimension`) with
    ## parallel `pks`/`norms` arrays for cache-friendly brute-force scans. `byPk`
    ## maps a primary key to its row index; deletes use swap-with-last to keep the
    ## arrays dense.
    name*: string
    dimension*: int
    vecs: seq[float32]
    pks: seq[string]
    norms: seq[float32]
    byPk: Table[string, int]

  VectorStore* {.acyclic.} = ref object
    ## The main data structure for the vector store, containing multiple collections of vectors
    collections: SortedTable[string, VectorCollection]
    storageMode: VectorStorageMode
    hasWal: bool
    wal: Wal
    hasDbFile: bool
    dbPath: string
    checkpointLsn: uint64
    pendingOps: uint32
    checkpointEveryOps: uint32
    walFlushEveryOps: uint32
    pendingWalOps: uint32

  VectorStoreError* = object of CatchableError
    ## Custom exception type for errors related to the vector store operations,
    ## such as invalid input, collection not found, or WAL issues.

type
  SnapshotOnDisk = tuple
    version: uint32
    checkpointLsn: uint64
    collections: seq[tuple[
      name: string,
      dimension: int,
      rows: seq[(string, seq[float32])]
    ]]

# fwd
proc recoverFromWal*(s: VectorStore)

proc writeTextAtomic(path, content: string) =
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc newCollection*(name: string, dimension: int): VectorCollection =
  ## Creates a new `VectorCollection` with the specified name and dimension. The collection is initialized
  ## with an empty sorted table to store vectors by their primary key (pk)
  if name.len == 0:
    raise newException(VectorStoreError, "collection name cannot be empty")
  if dimension <= 0:
    raise newException(VectorStoreError, "dimension must be > 0")
  VectorCollection(
    name: name,
    dimension: dimension,
    vecs: @[],
    pks: @[],
    norms: @[],
    byPk: initTable[string, int]()
  )

proc hasCollection*(s: VectorStore, name: string): bool =
  ## Checks if a collection with the specified name exists in the vector store
  s.collections.hasKey(name)

proc getCollection*(s: VectorStore, name: string): Option[VectorCollection] =
  ## Retrieves a collection by name from the vector store
  if s.collections.hasKey(name):
    some(s.collections[name])
  else:
    none(VectorCollection)

proc validateVector(c: VectorCollection, v: seq[float32]) =
  if v.len != c.dimension:
    raise newException(
      VectorStoreError,
      fmt"invalid vector dimension for '{c.name}': expected {c.dimension}, got {v.len}"
    )

proc createCollectionNoWal(s: VectorStore, c: VectorCollection) =
  if s.collections.hasKey(c.name):
    raise newException(VectorStoreError, fmt"collection already exists: {c.name}")
  s.collections[c.name] = c

proc dropCollectionNoWal(s: VectorStore, name: string) =
  if not s.collections.hasKey(name):
    raise newException(VectorStoreError, fmt"collection not found: {name}")
  s.collections.del(name)

proc vectorNorm(v: seq[float32]): float32 =
  ## Returns the L2 norm of the vector
  var s = 0.0'f32
  for x in v:
    s += x * x
  sqrt(s)

proc insertNoWal(c: VectorCollection, pk: string, vec: seq[float32]) =
  if pk.len == 0:
    raise newException(VectorStoreError, "pk cannot be empty")
  c.validateVector(vec)
  if c.byPk.hasKey(pk):
    raise newException(VectorStoreError, fmt"duplicate primary key '{pk}' in '{c.name}'")
  c.byPk[pk] = c.pks.len
  c.vecs.add(vec)
  c.pks.add(pk)
  c.norms.add(vectorNorm(vec))

proc deleteNoWal(c: VectorCollection, pk: string): bool =
  if not c.byPk.hasKey(pk):
    return false
  let i = c.byPk[pk]
  let last = c.pks.high
  if i != last:
    copyMem(addr c.vecs[i * c.dimension], addr c.vecs[last * c.dimension],
      c.dimension * sizeof(float32))
    c.pks[i] = c.pks[last]
    c.norms[i] = c.norms[last]
    c.byPk[c.pks[i]] = i
  c.byPk.del(pk)
  c.vecs.setLen(last * c.dimension)
  c.pks.setLen(last)
  c.norms.setLen(last)
  true

proc get*(c: VectorCollection, pk: string): Option[seq[float32]] =
  if c.byPk.hasKey(pk):
    let i = c.byPk[pk]
    some(c.vecs[i * c.dimension ..< i * c.dimension + c.dimension])
  else:
    none(seq[float32])

iterator all*(c: VectorCollection): (string, seq[float32]) =
  for i in 0 ..< c.pks.len:
    yield (c.pks[i], c.vecs[i * c.dimension ..< i * c.dimension + c.dimension])

proc vecToPayload(v: seq[float32]): string =
  var a = newJArray()
  for x in v:
    a.add(%x)
  $a

proc vecFromPayload(payload: string): seq[float32] =
  let n = parseJson(payload)
  if n.kind != JArray:
    raise newException(VectorStoreError, "invalid WAL payload for vector")
  for x in n.items:
    result.add(float32(x.getFloat()))

proc schemaToPayload(c: VectorCollection): string =
  $(%*{"dimension": c.dimension})

proc collectionFromPayload(name, payload: string): VectorCollection =
  let n = parseJson(payload)
  newCollection(name, n["dimension"].getInt())

proc flushWalIfNeeded(s: VectorStore, force = false) =
  if not s.hasWal:
    return
  if force:
    s.wal.flush()
    s.pendingWalOps = 0'u32
    return
  if s.walFlushEveryOps == 0'u32:
    return
  if s.pendingWalOps >= s.walFlushEveryOps:
    s.wal.flush()
    s.pendingWalOps = 0'u32

proc appendWalIfEnabled(s: VectorStore, op: WalOp, table, pk, payload: string): uint64 =
  if not s.hasWal:
    return 0'u64
  let lsn = s.wal.append(
    WalEntry(op: op, table: table, pk: pk, payload: payload),
    sync = false
  )
  inc s.pendingWalOps
  s.flushWalIfNeeded(force = false)
  lsn

proc buildSnapshot(s: VectorStore): SnapshotOnDisk =
  result.version = 1'u32
  result.checkpointLsn = s.checkpointLsn
  for _, c in s.collections.pairs:
    var rows: seq[(string, seq[float32])] = @[]
    for pk, vec in c.all:
      rows.add((pk, vec))
    result.collections.add((name: c.name, dimension: c.dimension, rows: rows))

proc loadSnapshotIntoStore(s: VectorStore, snap: SnapshotOnDisk) =
  if snap.version != 1'u32:
    raise newException(VectorStoreError, "unsupported snapshot version")
  s.collections = initSortedTable[string, VectorCollection]()
  s.checkpointLsn = snap.checkpointLsn
  for cd in snap.collections:
    var c = newCollection(cd.name, cd.dimension)
    for (pk, vec) in cd.rows:
      c.insertNoWal(pk, vec)
    s.collections[c.name] = c

proc saveSnapshotIfEnabled(s: VectorStore) =
  if not s.hasDbFile:
    return
  let blob = toFlatty(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: VectorStore) =
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = fromFlatty(blob, SnapshotOnDisk)
  s.loadSnapshotIntoStore(snap)

proc markCommitted(s: VectorStore, lsn: uint64) =
  if lsn > s.checkpointLsn:
    s.checkpointLsn = lsn
  if s.hasDbFile and s.checkpointEveryOps > 0'u32:
    inc s.pendingOps
    if s.pendingOps >= s.checkpointEveryOps:
      s.saveSnapshotIfEnabled()
      s.pendingOps = 0'u32

proc checkpoint*(s: VectorStore) =
  ## Manually triggers a checkpoint by flushing the WAL and saving a snapshot to disk, ensuring that all
  ## committed operations are persisted and the WAL is truncated up to the checkpoint LSN. This can be used
  ## to reduce recovery time after a crash by minimizing the number of WAL entries that need to be replayed.
  if not s.hasDbFile:
    return
  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32

proc applyWalEntry(s: VectorStore, e: WalEntry) =
  case e.op
  of woCreateTable:
    s.createCollectionNoWal(collectionFromPayload(e.table, e.payload))
  of woDropTable:
    s.dropCollectionNoWal(e.table)
  of woInsertRow:
    if not s.collections.hasKey(e.table):
      raise newException(VectorStoreError, "WAL replay: collection not found: " & e.table)
    let c = s.collections[e.table]
    c.insertNoWal(e.pk, vecFromPayload(e.payload))
  of woDeleteRow:
    if s.collections.hasKey(e.table):
      let c = s.collections[e.table]
      discard c.deleteNoWal(e.pk)
  of woUpdateRow:
    raise newException(VectorStoreError, "WAL replay: woUpdateRow not implemented")

proc recoverFromWal*(s: VectorStore) =
  ## Recovers the vector store state by loading a snapshot from disk (if available) and then
  ## replaying any WAL entries that have an LSN greater than the checkpoint LSN, ensuring that the
  ## vector store is up-to-date with all committed operations. This procedure is typically called
  ## during initialization of the vector store to restore its state after a crash or restart
  s.collections = initSortedTable[string, VectorCollection]()
  s.checkpointLsn = 0'u64
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

  s.loadSnapshotIfPresent()
  if s.hasWal:
    for e in s.wal.entries:
      if e.lsn <= s.checkpointLsn:
        continue
      s.applyWalEntry(e)
      s.checkpointLsn = e.lsn

  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

proc newVectorStore*(path: string, mode: VectorStorageMode = smDisk, enableWal: bool = true,
          checkpointEveryOps: uint32 = 0'u32, walFlushEveryOps: uint32 = 1000'u32): VectorStore =
  ## Creates a new `VectorStore` instance with the specified storage mode, WAL settings,
  ## and checkpointing configuration.
  var
    dbPath: string
    hasDb: bool
    hasWal: bool
    walObj: Wal

  case mode
  of smInMemory:
    discard
  of smDisk:
    if path.len == 0:
      raise newException(VectorStoreError, "path cannot be empty in disk mode")
    hasDb = true
    dbPath = path.changeFileExt(".vdb")
    if enableWal:
      hasWal = true
      walObj = openWal(path) # creates .wal next to path

  result = VectorStore(
    storageMode: mode,
    hasWal: hasWal,
    wal: walObj,
    hasDbFile: hasDb,
    dbPath: dbPath,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps
  )
  recoverFromWal(result)

proc newInMemoryVectorStore*: VectorStore =
  ## Convenience procedure to create a new in-memory vector store without
  ## persistence or WAL, useful for testing or ephemeral use cases
  newVectorStore("", smInMemory, false)

proc createCollection*(s: VectorStore, c: VectorCollection) =
  ## Creates a new collection in the vector store with the specified name and dimension,
  ## logging the operation in the WAL if enabled
  let lsn = s.appendWalIfEnabled(woCreateTable, c.name, "", schemaToPayload(c))
  s.createCollectionNoWal(c)
  s.markCommitted(lsn)

proc dropCollection*(s: VectorStore, name: string) =
  ## Drops the specified collection from the vector store,
  ## logging the operation in the WAL if enabled
  let lsn = s.appendWalIfEnabled(woDropTable, name, "", "")
  s.dropCollectionNoWal(name)
  s.markCommitted(lsn)

proc insert*(s: VectorStore, collection, pk: string, vec: seq[float32]) =
  ## Inserts a vector into the specified collection with the given primary key (pk). The vector is
  ## validated against the collection's dimension, and the operation is logged in the WAL if enabled
  if not s.collections.hasKey(collection):
    raise newException(VectorStoreError, fmt"collection not found: {collection}")
  let c = s.collections[collection]
  let lsn = s.appendWalIfEnabled(woInsertRow, collection, pk, vecToPayload(vec))
  c.insertNoWal(pk, vec)
  s.markCommitted(lsn)

proc delete*(s: VectorStore, collection, pk: string): bool =
  ## Delete a vector from the specified collection by primary key (pk). The operation
  ## is logged in the WAL if enabled. Returns true if successfully deleted,
  ## false if the pk was not found.
  if not s.collections.hasKey(collection):
    return false
  let lsn = s.appendWalIfEnabled(woDeleteRow, collection, pk, "")
  let removed = s.collections[collection].deleteNoWal(pk)
  s.markCommitted(lsn)
  removed

proc get*(s: VectorStore, collection, pk: string): Option[seq[float32]] =
  ## Retrieves a vector from the specified collection by primary key (pk).
  ## Returns an option containing the vector if found, or none if not found
  ## or if the collection does not exist.
  if not s.collections.hasKey(collection):
    return none(seq[float32])
  s.collections[collection].get(pk)

proc dotFlat(a: seq[float32], off: int, b: seq[float32]): float32 =
  ## Dot product of the contiguous segment `a[off ..< off+b.len]` with `b`,
  ## accumulated via 4 independent partial sums to break the serial FMA
  ## dependency chain and allow out-of-order execution
  var s0, s1, s2, s3 = 0.0'f32
  var i = 0
  let n = b.len
  while i + 4 <= n:
    s0 += a[off + i] * b[i]
    s1 += a[off + i + 1] * b[i + 1]
    s2 += a[off + i + 2] * b[i + 2]
    s3 += a[off + i + 3] * b[i + 3]
    i += 4
  while i < n:
    s0 += a[off + i] * b[i]
    inc i
  (s0 + s1) + (s2 + s3)

proc l2sqFlat(a: seq[float32], off: int, b: seq[float32]): float32 =
  var s0, s1, s2, s3 = 0.0'f32
  var i = 0
  let n = b.len
  while i + 4 <= n:
    let d0 = a[off + i] - b[i]
    let d1 = a[off + i + 1] - b[i + 1]
    let d2 = a[off + i + 2] - b[i + 2]
    let d3 = a[off + i + 3] - b[i + 3]
    s0 += d0 * d0
    s1 += d1 * d1
    s2 += d2 * d2
    s3 += d3 * d3
    i += 4
  while i < n:
    let d = a[off + i] - b[i]
    s0 += d * d
    inc i
  (s0 + s1) + (s2 + s3)

proc siftDown(h: var seq[(string, float32)], i, hi: int, higherIsBetter: bool) =
  ## Binary-heap sift-down where the root is the worst candidate
  var i = i
  while true:
    var child = i * 2 + 1
    if child > hi: break
    if child < hi:
      let childWorse = if higherIsBetter:
          h[child + 1][1] < h[child][1]
        else:
          h[child + 1][1] > h[child][1]
      if childWorse:
        inc child
    let swapWorse = if higherIsBetter:
        h[child][1] < h[i][1]
      else:
        h[child][1] > h[i][1]
    if swapWorse:
      swap(h[i], h[child])
      i = child
    else:
      break

proc keepTopK(h: var seq[(string, float32)], k: int, item: (string, float32),
              higherIsBetter: bool) =
  ## Maintains a bounded binary heap of the k best candidates. The root is the
  ## worst candidate and is replaced whenever a better one arrives.
  if h.len < k:
    h.add(item)
    var i = h.high
    while i > 0:
      let parent = (i - 1) shr 1
      let swapWorse = if higherIsBetter:
          h[i][1] < h[parent][1]
        else:
          h[i][1] > h[parent][1]
      if swapWorse:
        swap(h[i], h[parent])
        i = parent
      else:
        break
    return
  let rootWorse = if higherIsBetter:
      item[1] > h[0][1]
    else:
      item[1] < h[0][1]
  if rootWorse:
    h[0] = item
    siftDown(h, 0, h.high, higherIsBetter)

proc nearest*(c: VectorCollection, query: seq[float32],
          k: int, metric: DistanceMetric = dmCosine): seq[(string, float32)] =
  ## Finds the k nearest vectors to the query in the collection using the given
  ## distance metric. The result is a sequence of tuples containing the primary
  ## key and the corresponding similarity score or distance, sorted by relevance
  ## according to the specified metric.
  if k <= 0:
    return
  c.validateVector(query)

  let higherIsBetter = metric != dmL2
  let nq = if metric == dmCosine: vectorNorm(query) else: 0.0'f32
  let dim = c.dimension

  var heap = newSeqOfCap[(string, float32)](k)
  for i in 0 ..< c.pks.len:
    let score = case metric
      of dmCosine:
        let norm = c.norms[i]
        if nq == 0.0'f32 or norm == 0.0'f32: 0.0'f32
        else: dotFlat(c.vecs, i * dim, query) / (nq * norm)
      of dmDot: dotFlat(c.vecs, i * dim, query)         # higher is better
      of dmL2: l2sqFlat(c.vecs, i * dim, query)         # lower is better
    keepTopK(heap, k, (c.pks[i], score), higherIsBetter)

  result = move(heap)
  result.sort(proc(a, b: (string, float32)): int =
    if higherIsBetter:
      if a[1] > b[1]: -1 elif a[1] < b[1]: 1 else: 0
    else:
      if a[1] < b[1]: -1 elif a[1] > b[1]: 1 else: 0
  )

proc nearest*(s: VectorStore, collection: string, query: seq[float32],
        k: int, metric: DistanceMetric = dmCosine): seq[(string, float32)] =
  ## Finds the k nearest vectors to the query in the specified
  ## collection using the given distance metric
  if not s.collections.hasKey(collection):
    raise newException(VectorStoreError, fmt"collection not found: {collection}")
  s.collections[collection].nearest(query, k, metric)

proc exportCollection*(s: VectorStore, collection: string, path: string) =
  ## Exports the specified collection to a file in a simple text format, where each
  ## line contains the primary key and the corresponding vector serialized as a JSON array
  if not s.collections.hasKey(collection):
    raise newException(VectorStoreError, fmt"collection not found: {collection}")
  let c = s.collections[collection]
  var lines: seq[string] = @[]
  for pk, vec in c.all:
    lines.add(pk & " " & $(%vec))
  writeFile(path, lines.join("\n"))