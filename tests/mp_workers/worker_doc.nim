# Multi-process race worker: Document store.
# Usage: worker_doc <basePath> <workerId> <numOps>
import std/[os, strutils, json]
import ../../src/boogie/stores/docstore

proc main() =
  if paramCount() < 3:
    quit("usage: worker_doc <basePath> <workerId> <numOps>", 1)
  let base = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  var store = openDocumentStore(base, name = "documents",
    checkpointEveryOps = 0'u32, walFlushEveryOps = 1'u32)
  for i in 0 ..< n:
    store.upsert("w" & $wid & "_" & $i, %*{"w": wid, "i": i}, sync = false)
  store.close()

main()
