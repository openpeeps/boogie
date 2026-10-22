import std/[os, options, json]

import ../src/boogie/stores/docstore

# The document store maps string keys to whole JSON documents. Unlike the
# relational tables, every document may have its own shape, which makes it
# a natural fit for blobs of application state. A temp directory keeps
# this example side-effect free.

let dir = getTempDir() / "boogie_doc_example"
if dirExists(dir): removeDir(dir)
createDir(dir)

var store = openDocumentStore(dir, name = "notes")

store.insert("k1", %*{"title": "shopping", "items": ["milk", "bread"]})

# Upsert replaces the stored document when the key exists and inserts it
# when it does not, so callers never need a read-before-write round trip.
store.upsert("k1", %*{"title": "shopping", "items": ["milk", "bread", "eggs"]})

if store.hasKey("k1"):
  echo store.get("k1").get()["items"]

discard store.delete("k1")

# A checkpoint flushes buffered writes so a later open replays less log.
store.checkpoint()

removeDir(dir)
