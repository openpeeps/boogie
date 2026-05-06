# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie


## This module implements a simple graph store on top of the Boogie WAL for durability.
## The graph store supports basic operations for creating, updating, and deleting nodes and relationships,
## as well as simple traversal and querying capabilities. The graph data is stored in-memory for fast access,
## with periodic snapshots to disk for durability. The WAL is used to log all changes to the graph, allowing
## for crash recovery and durability guarantees.
## 
## The main types are:
## - `GraphNode`: represents a graph node with an ID, labels, and properties
## - `Relationship`: represents a directed relationship between two nodes, with an ID, type, and properties
## - `GraphStore`: the main graph store object that manages the in-memory graph and interacts with the WAL
## 
## The main operations supported by the graph store include:
## - Creating, updating, and deleting nodes and relationships
## - Querying nodes by label and traversing relationships

import std/[os, tables, sets, strutils, json]
import pkg/threading/rwlock
import ../wal

type
  GraphError* = object of CatchableError

  GraphNode* = object
    id*: uint64
    labels*: seq[string]
    props*: JsonNode

  Relationship* = object
    id*: uint64
    fromId*: uint64
    toId*: uint64
    relType*: string
    props*: JsonNode

  TxOpKind = enum
    tokCreateNode,
    tokUpsertNode,
    tokDeleteNode,
    tokCreateRel,
    tokUpsertRel,
    tokDeleteRel

  TxOp = object
    kind: TxOpKind
    node: GraphNode
    rel: Relationship
    id: uint64

  GraphStore* {.acyclic.} = ref object
    rootDir: string
    wal*: Wal
    mu: RWLock

    nextNodeId: uint64
    nextRelId: uint64

    nodes: Table[uint64, GraphNode]
    rels: Table[uint64, Relationship]
    outAdj: Table[uint64, seq[uint64]]
    inAdj: Table[uint64, seq[uint64]]

  GraphTx* = object
    store: GraphStore
    ops: seq[TxOp]
    finished: bool

let
  NodesMagic = "BGNODE1\0"
  RelsMagic  = "BGREL01\0"
  MetaMagic  = "BGMETA1\0"

const
  NodesFile = "nodes.gbin"
  RelsFile  = "rels.gbin"
  MetaFile  = "meta.gbin"

#
# low-level binary helpers
#
proc writeExact(f: File, p: pointer, n: int) =
  if n <= 0: return
  if f.writeBuffer(p, n) != n:
    raise newException(GraphError, "graphstore write failed")

proc readExact(f: File, p: pointer, n: int): bool =
  if n <= 0: return true
  f.readBuffer(p, n) == n

proc writeU8(f: File, v: uint8) =
  var b = v
  writeExact(f, addr b, 1)

proc readU8(f: File, outv: var uint8): bool =
  var b: uint8
  if not readExact(f, addr b, 1): return false
  outv = b
  result = true

proc writeU32Le(f: File, v: uint32) =
  var b: array[4, uint8]
  b[0] = uint8(v and 0xFF'u32)
  b[1] = uint8((v shr 8) and 0xFF'u32)
  b[2] = uint8((v shr 16) and 0xFF'u32)
  b[3] = uint8((v shr 24) and 0xFF'u32)
  writeExact(f, addr b[0], 4)

proc readU32Le(f: File, outv: var uint32): bool =
  var b: array[4, uint8]
  if not readExact(f, addr b[0], 4): return false
  outv = uint32(b[0]) or (uint32(b[1]) shl 8) or (uint32(b[2]) shl 16) or (uint32(b[3]) shl 24)
  result = true

proc writeU64Le(f: File, v: uint64) =
  var b: array[8, uint8]
  for i in 0..7:
    b[i] = uint8((v shr (i * 8)) and 0xFF'u64)
  writeExact(f, addr b[0], 8)

proc readU64Le(f: File, outv: var uint64): bool =
  var b: array[8, uint8]
  if not readExact(f, addr b[0], 8): return false
  outv = 0'u64
  for i in 0..7:
    outv = outv or (uint64(b[i]) shl (i * 8))
  result = true

proc writeStringBin(f: File, s: string) =
  if s.len > int(high(uint32)):
    raise newException(GraphError, "string too large")
  writeU32Le(f, uint32(s.len))
  if s.len > 0:
    writeExact(f, unsafeAddr s[0], s.len)

proc readStringBin(f: File, s: var string): bool =
  var n: uint32
  if not readU32Le(f, n): return false
  if n == 0'u32:
    s = ""
    return true
  s = newString(int(n))
  readExact(f, addr s[0], int(n))

proc atomicWrite(path: string, writer: proc(f: File)) =
  let tmp = path & ".tmp"
  var f = open(tmp, fmWrite)
  try:
    writer(f)
    f.flushFile()
  finally:
    f.close()
  moveFile(tmp, path)

proc ensureObj(props: JsonNode): JsonNode =
  if props.isNil: newJObject()
  else: props

proc parseProps(s: string): JsonNode =
  if s.len == 0: return newJObject()
  try:
    result = parseJson(s)
  except CatchableError:
    result = newJObject()

#
# graph serialization
#
proc writeNode(f: File, n: GraphNode) =
  writeU64Le(f, n.id)
  writeU32Le(f, uint32(n.labels.len))
  for l in n.labels:
    writeStringBin(f, l)
  writeStringBin(f, $(ensureObj(n.props)))

proc readNode(f: File, n: var GraphNode): bool =
  var id: uint64
  if not readU64Le(f, id): return false
  var labelsCount: uint32
  if not readU32Le(f, labelsCount): return false
  var labels = newSeq[string](int(labelsCount))
  for i in 0 ..< labels.len:
    if not readStringBin(f, labels[i]): return false
  var p: string
  if not readStringBin(f, p): return false

  n.id = id
  n.labels = labels
  n.props = parseProps(p)
  result = true

proc writeRel(f: File, r: Relationship) =
  writeU64Le(f, r.id)
  writeU64Le(f, r.fromId)
  writeU64Le(f, r.toId)
  writeStringBin(f, r.relType)
  writeStringBin(f, $(ensureObj(r.props)))

proc readRel(f: File, r: var Relationship): bool =
  var id, fromId, toId: uint64
  if not readU64Le(f, id): return false
  if not readU64Le(f, fromId): return false
  if not readU64Le(f, toId): return false
  var relType, p: string
  if not readStringBin(f, relType): return false
  if not readStringBin(f, p): return false

  r.id = id
  r.fromId = fromId
  r.toId = toId
  r.relType = relType
  r.props = parseProps(p)
  result = true

#
# indexing + apply ops
#
proc removeRid(v: var seq[uint64], rid: uint64) =
  var i = 0
  while i < v.len:
    if v[i] == rid: v.delete(i)
    else: inc i

proc indexRelNoLock(s: GraphStore, r: Relationship) =
  s.outAdj.mgetOrPut(r.fromId, @[]).add(r.id)
  s.inAdj.mgetOrPut(r.toId, @[]).add(r.id)

proc deindexRelNoLock(s: GraphStore, r: Relationship) =
  if s.outAdj.hasKey(r.fromId):
    var outv = s.outAdj[r.fromId]
    outv.removeRid(r.id)
    s.outAdj[r.fromId] = outv
  if s.inAdj.hasKey(r.toId):
    var inv = s.inAdj[r.toId]
    inv.removeRid(r.id)
    s.inAdj[r.toId] = inv

proc upsertNodeNoLock(s: GraphStore, n: GraphNode) =
  s.nodes[n.id] = n
  if n.id >= s.nextNodeId: s.nextNodeId = n.id + 1'u64

proc deleteNodeNoLock(s: GraphStore, id: uint64) =
  if not s.nodes.hasKey(id): return

  var toDelete: seq[uint64] = @[]
  if s.outAdj.hasKey(id):
    for rid in s.outAdj[id]: toDelete.add(rid)
  if s.inAdj.hasKey(id):
    for rid in s.inAdj[id]:
      if rid notin toDelete: toDelete.add(rid)

  for rid in toDelete:
    if s.rels.hasKey(rid):
      let r = s.rels[rid]
      deindexRelNoLock(s, r)
      s.rels.del(rid)

  s.outAdj.del(id)
  s.inAdj.del(id)
  s.nodes.del(id)

proc upsertRelNoLock(s: GraphStore, r: Relationship) =
  if not s.nodes.hasKey(r.fromId) or not s.nodes.hasKey(r.toId):
    raise newException(GraphError, "relationship endpoint node does not exist")

  if s.rels.hasKey(r.id):
    deindexRelNoLock(s, s.rels[r.id])

  s.rels[r.id] = r
  indexRelNoLock(s, r)
  if r.id >= s.nextRelId: s.nextRelId = r.id + 1'u64

proc deleteRelNoLock(s: GraphStore, id: uint64) =
  if not s.rels.hasKey(id): return
  let r = s.rels[id]
  deindexRelNoLock(s, r)
  s.rels.del(id)

proc applyOpNoLock(s: GraphStore, op: TxOp) =
  case op.kind
  of tokCreateNode, tokUpsertNode:
    upsertNodeNoLock(s, op.node)
  of tokDeleteNode:
    deleteNodeNoLock(s, op.id)
  of tokCreateRel, tokUpsertRel:
    upsertRelNoLock(s, op.rel)
  of tokDeleteRel:
    deleteRelNoLock(s, op.id)

#
# WAL conversion + recovery
#
proc nodeToPayload(n: GraphNode): string =
  let j = %*{
    "id": n.id,
    "labels": n.labels,
    "props": ensureObj(n.props)
  }
  $j

proc relToPayload(r: Relationship): string =
  let j = %*{
    "id": r.id,
    "fromId": r.fromId,
    "toId": r.toId,
    "relType": r.relType,
    "props": ensureObj(r.props)
  }
  $j

proc nodeFromPayload(payload: string): GraphNode =
  let j = parseJson(payload)
  result.id = j["id"].getBiggestInt().uint64
  result.labels = @[]
  for it in j["labels"].items:
    result.labels.add(it.getStr())
  result.props = if j.hasKey("props"): j["props"] else: newJObject()

proc relFromPayload(payload: string): Relationship =
  let j = parseJson(payload)
  result.id = j["id"].getBiggestInt().uint64
  result.fromId = j["fromId"].getBiggestInt().uint64
  result.toId = j["toId"].getBiggestInt().uint64
  result.relType = j["relType"].getStr()
  result.props = if j.hasKey("props"): j["props"] else: newJObject()

proc walEntryFor(op: TxOp): WalEntry =
  case op.kind
  of tokCreateNode:
    WalEntry(op: woInsertRow, table: "node", pk: $op.node.id, payload: nodeToPayload(op.node))
  of tokUpsertNode:
    WalEntry(op: woUpdateRow, table: "node", pk: $op.node.id, payload: nodeToPayload(op.node))
  of tokDeleteNode:
    WalEntry(op: woDeleteRow, table: "node", pk: $op.id, payload: "")
  of tokCreateRel:
    WalEntry(op: woInsertRow, table: "rel", pk: $op.rel.id, payload: relToPayload(op.rel))
  of tokUpsertRel:
    WalEntry(op: woUpdateRow, table: "rel", pk: $op.rel.id, payload: relToPayload(op.rel))
  of tokDeleteRel:
    WalEntry(op: woDeleteRow, table: "rel", pk: $op.id, payload: "")

proc applyWalEntryNoLock(s: GraphStore, e: WalEntry) =
  case e.table
  of "node":
    case e.op
    of woInsertRow, woUpdateRow:
      upsertNodeNoLock(s, nodeFromPayload(e.payload))
    of woDeleteRow:
      deleteNodeNoLock(s, parseUInt(e.pk).uint64)
    else:
      discard
  of "rel":
    case e.op
    of woInsertRow, woUpdateRow:
      upsertRelNoLock(s, relFromPayload(e.payload))
    of woDeleteRow:
      deleteRelNoLock(s, parseUInt(e.pk).uint64)
    else:
      discard
  else:
    discard

#
# snapshot read/write
#
proc saveSnapshotNoLock(s: GraphStore) =
  let nodesPath = joinPath(s.rootDir, NodesFile)
  let relsPath = joinPath(s.rootDir, RelsFile)
  let metaPath = joinPath(s.rootDir, MetaFile)

  atomicWrite(nodesPath, proc(f: File) =
    writeExact(f, unsafeAddr NodesMagic[0], NodesMagic.len)
    writeU64Le(f, uint64(s.nodes.len))
    for _, n in s.nodes.pairs:
      writeNode(f, n)
  )

  atomicWrite(relsPath, proc(f: File) =
    writeExact(f, unsafeAddr RelsMagic[0], RelsMagic.len)
    writeU64Le(f, uint64(s.rels.len))
    for _, r in s.rels.pairs:
      writeRel(f, r)
  )

  atomicWrite(metaPath, proc(f: File) =
    writeExact(f, unsafeAddr MetaMagic[0], MetaMagic.len)
    writeU64Le(f, s.nextNodeId)
    writeU64Le(f, s.nextRelId)
  )

proc readMagic(f: File, m: string): bool =
  var got = newString(m.len)
  if not readExact(f, addr got[0], m.len): return false
  got == m

proc loadSnapshotNoLock(s: GraphStore) =
  let nodesPath = joinPath(s.rootDir, NodesFile)
  let relsPath = joinPath(s.rootDir, RelsFile)
  let metaPath = joinPath(s.rootDir, MetaFile)

  s.nodes.clear()
  s.rels.clear()
  s.outAdj.clear()
  s.inAdj.clear()
  s.nextNodeId = 1
  s.nextRelId = 1

  if fileExists(nodesPath):
    let f = open(nodesPath, fmRead)
    defer: f.close()
    if not readMagic(f, NodesMagic):
      raise newException(GraphError, "invalid nodes snapshot magic")
    var count: uint64
    if not readU64Le(f, count):
      raise newException(GraphError, "invalid nodes snapshot")
    for _ in 0 ..< int(count):
      var n: GraphNode
      if not readNode(f, n):
        raise newException(GraphError, "corrupt node record")
      s.nodes[n.id] = n
      if n.id >= s.nextNodeId: s.nextNodeId = n.id + 1'u64

  if fileExists(relsPath):
    let f = open(relsPath, fmRead)
    defer: f.close()
    if not readMagic(f, RelsMagic):
      raise newException(GraphError, "invalid rels snapshot magic")
    var count: uint64
    if not readU64Le(f, count):
      raise newException(GraphError, "invalid rels snapshot")
    for _ in 0 ..< int(count):
      var r: Relationship
      if not readRel(f, r):
        raise newException(GraphError, "corrupt relationship record")
      s.rels[r.id] = r
      indexRelNoLock(s, r)
      if r.id >= s.nextRelId: s.nextRelId = r.id + 1'u64

  if fileExists(metaPath):
    let f = open(metaPath, fmRead)
    defer: f.close()
    if not readMagic(f, MetaMagic):
      raise newException(GraphError, "invalid meta snapshot magic")
    var nn, nr: uint64
    if not readU64Le(f, nn) or not readU64Le(f, nr):
      raise newException(GraphError, "invalid meta snapshot")
    if nn > s.nextNodeId: s.nextNodeId = nn
    if nr > s.nextRelId: s.nextRelId = nr

proc checkpointNoLock(s: GraphStore) =
  saveSnapshotNoLock(s)
  s.wal.reset()

#
# Public API
#
var graphStoreRwLock = createRwLock()
proc openGraphStore*(rootDir: string): GraphStore =
  ## Opens or creates a graph store at the specified root directory. The graph store
  ## will load any existing snapshot and WAL entries to reconstruct the in-memory graph state.
  ## 
  ## If the directory does not exist, it will be created. The returned `GraphStore` object can be used
  ## to perform graph operations and transactions.
  createDir(rootDir)
  new(result)
  result.rootDir = rootDir
  result.nodes = initTable[uint64, GraphNode]()
  result.rels = initTable[uint64, Relationship]()
  result.outAdj = initTable[uint64, seq[uint64]]()
  result.inAdj = initTable[uint64, seq[uint64]]()
  
  # initialize WAL and load existing state
  result.wal = openWal(joinPath(rootDir, "graph"))

  let s = result
  writeWith graphStoreRwLock:
    loadSnapshotNoLock(s)
    var replayed = false
    for e in s.wal.entries:
      applyWalEntryNoLock(s, e)
      replayed = true
    if replayed:
      checkpointNoLock(s)

proc closeGraphStore*(s: GraphStore) =
  writeWith graphStoreRwLock:
    checkpointNoLock(s)

proc beginTx*(s: GraphStore): GraphTx =
  GraphTx(store: s, ops: @[], finished: false)

proc rollback*(tx: var GraphTx) =
  if tx.finished: return
  tx.ops.setLen(0)
  tx.finished = true

proc reserveNodeIdNoLock(s: GraphStore): uint64 =
  result = s.nextNodeId
  inc s.nextNodeId

proc reserveRelIdNoLock(s: GraphStore): uint64 =
  result = s.nextRelId
  inc s.nextRelId

proc createNode*(tx: var GraphTx, labels: seq[string] = @[], props: JsonNode = newJObject()): uint64 =
  if tx.finished: raise newException(GraphError, "transaction already finished")
  writeWith graphStoreRwLock:
    result = reserveNodeIdNoLock(tx.store)
  tx.ops.add(TxOp(kind: tokCreateNode, node: GraphNode(id: result, labels: labels, props: ensureObj(props))))

proc upsertNode*(tx: var GraphTx, node: GraphNode) =
  if tx.finished: raise newException(GraphError, "transaction already finished")
  tx.ops.add(TxOp(kind: tokUpsertNode, node: node))

proc deleteNode*(tx: var GraphTx, id: uint64) =
  if tx.finished: raise newException(GraphError, "transaction already finished")
  tx.ops.add(TxOp(kind: tokDeleteNode, id: id))

proc createRelationship*(
  tx: var GraphTx,
  fromId, toId: uint64,
  relType: string,
  props: JsonNode = newJObject()
): uint64 =
  if tx.finished: raise newException(GraphError, "transaction already finished")
  writeWith graphStoreRwLock:
    result = reserveRelIdNoLock(tx.store)
  tx.ops.add(TxOp(
    kind: tokCreateRel,
    rel: Relationship(id: result, fromId: fromId, toId: toId, relType: relType, props: ensureObj(props))
  ))

proc upsertRelationship*(tx: var GraphTx, r: Relationship) =
  ## Upserts the specified relationship. The relationship must have a
  ## valid ID and endpoint node IDs.
  if tx.finished: raise newException(GraphError, "transaction already finished")
  tx.ops.add(TxOp(kind: tokUpsertRel, rel: r))

proc deleteRelationship*(tx: var GraphTx, id: uint64) =
  ## Deletes the relationship with the specified ID
  if tx.finished: raise newException(GraphError, "transaction already finished")
  tx.ops.add(TxOp(kind: tokDeleteRel, id: id))

proc commit*(tx: var GraphTx) =
  ## Commits the transaction, applying all operations atomically to the graph store. The operations
  ## are first appended to the WAL for durability, then applied to the in-memory graph state. Finally, a checkpoint
  ## is performed to save the current state and reset the WAL. After commit, the transaction is marked as finished
  ## and cannot be used for further operations.
  if tx.finished: raise newException(GraphError, "transaction already finished")
  let s = tx.store

  writeWith graphStoreRwLock:
    # 1) WAL append (durable intent)
    for op in tx.ops:
      discard s.wal.append(walEntryFor(op), sync = false)
    s.wal.flush()

    # 2) Apply in-memory
    for op in tx.ops:
      applyOpNoLock(s, op)

    # 3) Snapshot + WAL reset (checkpoint)
    checkpointNoLock(s)

  tx.finished = true

proc getNode*(s: GraphStore, id: uint64, outNode: var GraphNode): bool =
  ## Retrieves the node with the specified ID. Returns true if found, false otherwise.
  writeWith graphStoreRwLock:
    if not s.nodes.hasKey(id): return false
    outNode = s.nodes[id]
    return true

proc getRelationship*(s: GraphStore, id: uint64, outRel: var Relationship): bool =
  ## Retrieves the relationship with the specified ID. Returns true if found, false otherwise.
  writeWith graphStoreRwLock:
    if not s.rels.hasKey(id): return false
    outRel = s.rels[id]
    return true

proc outgoing*(s: GraphStore, nodeId: uint64, relType = ""): seq[Relationship] =
  ## Returns the outgoing relationships from the specified node. If `relType` is provided,
  ## only relationships of that type are returned.
  writeWith graphStoreRwLock:
    if not s.outAdj.hasKey(nodeId): return @[]
    for rid in s.outAdj[nodeId]:
      if not s.rels.hasKey(rid): continue
      let r = s.rels[rid]
      if relType.len == 0 or r.relType == relType:
        result.add(r)

proc neighbors*(s: GraphStore, nodeId: uint64, relType = ""): seq[uint64] =
  for r in s.outgoing(nodeId, relType):
    result.add(r.toId)

proc findNodesByLabel*(s: GraphStore, label: string): seq[GraphNode] =
  writeWith graphStoreRwLock:
    for _, n in s.nodes.pairs:
      if label in n.labels:
        result.add(n)

proc traverseBfs*(s: GraphStore, startNode: uint64, maxDepth: int, relType = ""): seq[uint64] =
  if maxDepth < 0: return @[]
  writeWith graphStoreRwLock:
    if not s.nodes.hasKey(startNode): return @[]

    var visited = initHashSet[uint64]()
    var q: seq[(uint64, int)] = @[(startNode, 0)]
    var head = 0
    visited.incl(startNode)
    result.add(startNode)

    while head < q.len:
      let (nid, depth) = q[head]
      inc head
      if depth >= maxDepth: continue
      if not s.outAdj.hasKey(nid): continue

      for rid in s.outAdj[nid]:
        if not s.rels.hasKey(rid): continue
        let r = s.rels[rid]
        if relType.len > 0 and r.relType != relType: continue
        let nxt = r.toId
        if nxt notin visited:
          visited.incl(nxt)
          result.add(nxt)
          q.add((nxt, depth + 1))
