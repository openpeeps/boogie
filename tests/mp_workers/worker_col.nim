# Multi-process race worker: Columnar store.
# Usage: worker_col <rootDir> <workerId> <numOps>
import std/[os, strutils, json]
import ../../src/boogie/stores/columnar

proc main() =
  if paramCount() < 3:
    quit("usage: worker_col <rootDir> <workerId> <numOps>", 1)
  let root = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  var s = openColumnarStore(root)
  for i in 0 ..< n:
    let id = wid * n + i
    s.insertBatch("events", @[
      %*{"id": id, "user": "w" & $wid, "amount": float(id)}
    ], sync = false)
  s.checkpoint()
  s.close()

main()
