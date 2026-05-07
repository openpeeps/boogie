# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | LGPL-3.0-or-later License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## This module implements a simple document store on top of the WAL infrastructure. It allows
## storing JSON documents with flexible schemas, using string keys. The documents are encoded
## as JSON or BSON and stored in the WAL for durability. The store supports basic CRUD operations
## and can be extended with additional features like indexing or querying in the future.
## 
## Currently there are some inefficiencies in the way Nim objects are converted to JsonNode and back, which involves
## an intermediate string representation. This is something that could be optimized in the future by implementing
## a more direct conversion between Nim objects and JsonNode (related to `/pkg/openparser/json`).

import std/[tables, options, base64, os]

import pkg/[flatty, sorta]
import pkg/openparser/[json, bson]

import ../wal
export wal, bson

type
  DocumentEncoding* = enum
    ## The encoding format for documents stored in the document
    ## store. This determines how the document is serialized
    deJson, deBson

  DocumentStoreError* = object of CatchableError

  SnapshotOnDisk = tuple
    version: uint32
    checkpointLsn: uint64
    docs: seq[(string, string)] # (key, jsonText)

  DocumentStore* = object
    ## A simple document store that supports storing
    ## JSON documents with flexible schemas. Each document
    ## is identified by a unique string key.
    name*: string
    wal*: Wal
    defaultEncoding*: DocumentEncoding
    docs: SortedTable[string, JsonNode]

    # snapshot + durability policy (similar to rdbms)
    hasDbFile: bool
    dbPath: string
    checkpointLsn: uint64
    pendingOps: uint32
    checkpointEveryOps: uint32
    walFlushEveryOps: uint32
    pendingWalOps: uint32

proc fail(msg: string) {.noreturn.} =
  raise newException(DocumentStoreError, msg)

proc writeTextAtomic(path, content: string) =
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc bytesToString(b: openArray[byte]): string =
  result = newString(b.len)
  for i in 0 ..< b.len:
    result[i] = char(b[i])

proc stringToBytes(s: string): seq[byte] =
  result = newSeq[byte](s.len)
  for i, ch in s:
    result[i] = byte(ord(ch))

proc encodePayload(doc: JsonNode, enc: DocumentEncoding): string =
  if doc.kind != JObject:
    fail("Document must be a JSON object")
  case enc
  of deJson:
    "J" & toJson(doc)
  of deBson:
    let bdoc = newBSONDocument(doc, 1'i32)
    "B" & base64.encode(bytesToString(toBytes(bdoc)))

proc decodePayload(payload: string): JsonNode =
  if payload.len < 2:
    fail("Invalid WAL payload")
  case payload[0]
  of 'J':
    fromJson(payload[1 .. ^1])
  of 'B':
    let bdoc = fromBytes(stringToBytes(base64.decode(payload[1 .. ^1])))
    toJsonNode(bdoc)
  else:
    fail("Unknown payload encoding")

proc buildSnapshot(s: DocumentStore): SnapshotOnDisk =
  result.version = 1'u32
  result.checkpointLsn = s.checkpointLsn
  for k, v in s.docs.pairs:
    result.docs.add((k, toJson(v)))

proc loadSnapshotIntoStore(s: var DocumentStore, snap: SnapshotOnDisk) =
  if snap.version != 1'u32:
    fail("unsupported document snapshot version")
  s.docs = initSortedTable[string, JsonNode]()
  s.checkpointLsn = snap.checkpointLsn
  for (k, jsonText) in snap.docs:
    s.docs[k] = fromJson(jsonText)

proc saveSnapshotIfEnabled(s: var DocumentStore) =
  if not s.hasDbFile:
    return
  let blob = toFlatty(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: var DocumentStore) =
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = fromFlatty(blob, SnapshotOnDisk)
  s.loadSnapshotIntoStore(snap)

proc flushWalIfNeeded(s: var DocumentStore, force = false) =
  if force:
    s.wal.flush()
    s.pendingWalOps = 0'u32
    return

  if s.walFlushEveryOps == 0'u32:
    return
  if s.pendingWalOps >= s.walFlushEveryOps:
    s.wal.flush()
    s.pendingWalOps = 0'u32

proc appendWal(s: var DocumentStore, op: WalOp, key, payload: string, sync: bool): uint64 =
  let lsn = s.wal.append(
    WalEntry(op: op, table: s.name, pk: key, payload: payload),
    sync = false
  )
  inc s.pendingWalOps
  if sync:
    s.flushWalIfNeeded(force = true)
  else:
    s.flushWalIfNeeded(force = false)
  result = lsn

proc markCommitted(s: var DocumentStore, lsn: uint64) =
  if lsn > s.checkpointLsn:
    s.checkpointLsn = lsn

  if s.hasDbFile and s.checkpointEveryOps > 0'u32:
    inc s.pendingOps
    if s.pendingOps >= s.checkpointEveryOps:
      s.saveSnapshotIfEnabled()
      s.pendingOps = 0'u32

proc applyWalEntry(s: var DocumentStore, e: WalEntry) =
  if e.table != s.name:
    return
  case e.op
  of woInsertRow, woUpdateRow:
    s.docs[e.pk] = decodePayload(e.payload)
  of woDeleteRow:
    if s.docs.hasKey(e.pk):
      s.docs.del(e.pk)
  else:
    discard

proc recoverFromWal*(s: var DocumentStore) =
  s.docs = initSortedTable[string, JsonNode]()
  s.checkpointLsn = 0'u64
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

  s.loadSnapshotIfPresent()

  for e in s.wal.entries:
    if e.lsn <= s.checkpointLsn:
      continue
    s.applyWalEntry(e)
    s.checkpointLsn = e.lsn

  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

proc openDocumentStore*(path: string, name = "documents",
                  defaultEncoding = deJson,
                  enableSnapshots = true,
                  checkpointEveryOps: uint32 = 1000'u32,
                  walFlushEveryOps: uint32 = 1000'u32): DocumentStore =
  ## Opens a document store with the given name at the specified path.
  ## 
  ## If the WAL file already exists, it will be replayed to reconstruct the in-memory state of the store.
  ## The default encoding for documents can be set to either JSON or BSON.
  if path.len > 0 and not dirExists(path):
    createDir(path)

  let base = path / name
  result = DocumentStore(
    name: name,
    wal: openWal(base),
    defaultEncoding: defaultEncoding,
    docs: initSortedTable[string, JsonNode](),
    hasDbFile: enableSnapshots,
    dbPath: base.changeFileExt(".ddb"),
    checkpointLsn: 0'u64,
    pendingOps: 0'u32,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps,
    pendingWalOps: 0'u32
  )
  result.recoverFromWal()

proc checkpoint*(store: var DocumentStore) =
  ## Force snapshot checkpoint.
  store.flushWalIfNeeded(force = true)
  store.saveSnapshotIfEnabled()
  store.pendingOps = 0'u32

proc len*(store: DocumentStore): int =
  ## Returns the number of documents currently stored in the document store.
  store.docs.len

proc hasKey*(store: DocumentStore, key: string): bool =
  ## Checks if a document with the given key exists in the store.
  store.docs.hasKey(key)

proc get*(store: DocumentStore, key: string): Option[JsonNode] =
  ## Retrieves the document with the given key, if it exists. Returns `none` if the key is not found.
  if store.docs.hasKey(key):
    some(store.docs[key])
  else:
    none(JsonNode)

proc insert*(
  store: var DocumentStore,
  key: string,
  doc: JsonNode,
  sync = true,
  enc: DocumentEncoding = deJson
) =
  if store.docs.hasKey(key):
    fail("Duplicate key: " & key)
  let payload = encodePayload(doc, enc)
  let lsn = store.appendWal(woInsertRow, key, payload, sync)
  store.docs[key] = doc
  store.markCommitted(lsn)

proc upsert*(
  store: var DocumentStore,
  key: string,
  doc: JsonNode,
  sync = true,
  enc: DocumentEncoding = deJson
) =
  ## Inserts or updates a document with the given key. If the key already exists,
  ## it will be updated with the new document.
  let payload = encodePayload(doc, enc)
  let op = if store.docs.hasKey(key): woUpdateRow else: woInsertRow
  let lsn = store.appendWal(op, key, payload, sync)
  store.docs[key] = doc
  store.markCommitted(lsn)

proc delete*(store: var DocumentStore, key: string, sync = true): bool =
  ## Deletes the document with the given key. Returns true if the document
  ## was found and deleted, false if the key was not found.
  if not store.docs.hasKey(key):
    return false
  let lsn = store.appendWal(woDeleteRow, key, "", sync)
  store.docs.del(key)
  store.markCommitted(lsn)
  true

proc putObj*[T: object|ref object](
  store: var DocumentStore,
  key: string,
  value: T,
  sync = true,
  enc: DocumentEncoding = deJson
) =
  ## This is a bit hacky - we convert the Nim object to JSON and then parse it back into
  ## a JsonNode to store in the document store. This allows us to leverage the existing
  ## JSON encoding logic for storing arbitrary Nim objects, but it does involve an
  ## extra conversion step. Ideally, we would have a more direct way to convert
  ## Nim objects to JsonNode without going through an intermediate string representation.
  let doc = fromJson(toJson(value))
  store.upsert(key, doc, sync, enc)

proc getObj*[T](store: DocumentStore, key: string, _: typedesc[T]): Option[T] =
  ## This is a bit hacky - we convert the stored JSON document to a string
  ## and then parse it back into the desired type using `pkg/openparser/json`.
  ## 
  ## **The OpenParser JSON module should provide a way to directly convert `JsonNode` to a Nim object without
  ## the intermediate string step, but this works for now.**
  if not store.docs.hasKey(key):
    return none(T)
  let s = toJson(store.docs[key])
  some(fromJson(s, T))

proc getBson*(store: DocumentStore, key: string, version: int32 = 1'i32): Option[BSONDocument] =
  ## Returns the raw BSON bytes for the document with the given key, if it exists. This allows
  ## clients to retrieve the original BSON data without converting it to JSON first. If the document
  ## was stored using JSON encoding, this will return the JSON string as bytes instead.
  if not store.docs.hasKey(key):
    return none(BSONDocument)
  some(newBSONDocument(store.docs[key], version))

proc writeBSONDocument*(store: DocumentStore, key, path: string,
                    version: int32 = 1'i32): bool {.discardable.} =
  ## Writes the document with the given key to a file in BSON format. Returns true if the document
  ## was found and written, false if the key was not found
  let d = store.getBson(key, version)
  if d.isNone:
    return false
  bson.writeBSONDocument(path, d.get)
  true

proc openBSONDocument*(store: var DocumentStore, key, path: string, sync = true): bool {.discardable.} =
  ## Reads a BSON document from a file and upserts it into the store with the given key.
  ## 
  ## Returns true if the file was found and read, false if the file does not exist.
  if not fileExists(path):
    return false
  let d = bson.openBSONDocument(path)
  store.upsert(key, d.toJsonNode(), sync = sync, enc = deBson)
  true

iterator pairs*(store: DocumentStore): (string, JsonNode) =
  ## Iterates over all key-document pairs in the store. The order is the same as insertion order.
  for k, v in store.docs.pairs:
    yield (k, v)
