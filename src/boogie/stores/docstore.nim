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
import pkg/openparser/[json, bson]
import ../wal
export wal

type
  DocumentEncoding* = enum
    deJson, deBson

  DocumentStoreError* = object of CatchableError

  DocumentStore* = object
    name*: string
    wal*: Wal
    defaultEncoding*: DocumentEncoding
    docs: OrderedTable[string, JsonNode]

proc fail(msg: string) {.noreturn.} =
  raise newException(DocumentStoreError, msg)

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
    "B" & base64.encode(bytesToString(toBson(doc)))

proc decodePayload(payload: string): JsonNode =
  if payload.len < 2:
    fail("Invalid WAL payload")
  case payload[0]
  of 'J':
    fromJson(payload[1 .. ^1])
  of 'B':
    fromBson(stringToBytes(base64.decode(payload[1 .. ^1])))
  else:
    fail("Unknown payload encoding")

proc applyReplay(store: var DocumentStore) =
  for e in store.wal.entries:
    if e.table != store.name: continue
    case e.op
    of woInsertRow, woUpdateRow:
      store.docs[e.pk] = decodePayload(e.payload)
    of woDeleteRow:
      if store.docs.hasKey(e.pk):
        store.docs.del(e.pk)
    else:
      discard

proc openDocumentStore*(
  path: string,
  name = "documents",
  defaultEncoding = deJson
): DocumentStore =
  let walPath = path / name
  result = DocumentStore(
    name: name,
    wal: openWal(walPath),
    defaultEncoding: defaultEncoding,
    docs: initOrderedTable[string, JsonNode]()
  )
  result.applyReplay()

proc len*(store: DocumentStore): int {.inline.} = store.docs.len
proc hasKey*(store: DocumentStore, key: string): bool {.inline.} = store.docs.hasKey(key)

proc get*(store: DocumentStore, key: string): Option[JsonNode] =
  if store.docs.hasKey(key): some(store.docs[key]) else: none(JsonNode)

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
  discard store.wal.append(WalEntry(op: woInsertRow, table: store.name, pk: key, payload: payload), sync)
  store.docs[key] = doc

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
  discard store.wal.append(WalEntry(op: op, table: store.name, pk: key, payload: payload), sync)
  store.docs[key] = doc

proc delete*(store: var DocumentStore, key: string, sync = true): bool =
  ## Deletes the document with the given key. Returns true if the document
  ## was found and deleted, false if the key was not found.
  if not store.docs.hasKey(key):
    return false
  discard store.wal.append(WalEntry(op: woDeleteRow, table: store.name, pk: key, payload: ""), sync)
  store.docs.del(key)
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

proc getBson*(store: DocumentStore, key: string): Option[seq[byte]] =
  ## Returns the raw BSON bytes for the document with the given key, if it exists. This allows
  ## clients to retrieve the original BSON data without converting it to JSON first. If the document
  ## was stored using JSON encoding, this will return the JSON string as bytes instead.
  if not store.docs.hasKey(key):
    return none(seq[byte])
  some(toBson(store.docs[key]))

iterator pairs*(store: DocumentStore): (string, JsonNode) =
  ## Iterates over all key-document pairs in the store. The order is the same as insertion order.
  for k, v in store.docs.pairs:
    yield (k, v)
