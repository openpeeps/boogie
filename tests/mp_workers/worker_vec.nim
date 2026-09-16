# Multi-process race worker: Vector store.
# Usage: worker_vec <basePath> <workerId> <numOps>
import std/[os, strutils]
import ../../src/boogie/stores/vectorstore

proc main() =
  if paramCount() < 3:
    quit("usage: worker_vec <basePath> <workerId> <numOps>", 1)
  let base = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  let vs = newVectorStore(base, smDisk, enableWal = true,
    checkpointEveryOps = 0'u32, walFlushEveryOps = 1'u32)
  for i in 0 ..< n:
    vs.insert("emb", "w" & $wid & "_" & $i,
      @[float32(i), float32(wid)])
  vs.checkpoint()
  vs.close()

main()
