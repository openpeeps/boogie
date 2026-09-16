# Multi-process race worker: Graph store.
# Usage: worker_graph <rootDir> <workerId> <numOps>
import std/[os, strutils, json]
import ../../src/boogie/stores/graphstore

proc main() =
  if paramCount() < 3:
    quit("usage: worker_graph <rootDir> <workerId> <numOps>", 1)
  let root = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  var gs = openGraphStore(root)
  for i in 0 ..< n:
    var tx = beginTx(gs)
    discard createNode(tx, @["N"], %*{"w": wid, "i": i})
    commit(tx)
  closeGraphStore(gs)

main()
