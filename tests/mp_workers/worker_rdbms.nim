# Multi-process race worker: RDBMS store.
# Usage: worker_rdbms <basePath> <workerId> <numOps>
import std/[os, strutils]
import ../../src/boogie/stores/rdbms

proc main() =
  if paramCount() < 3:
    quit("usage: worker_rdbms <basePath> <workerId> <numOps>", 1)
  let base = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  let s = newStore(base, smDisk, enableWal = true,
    checkpointEveryOps = 0'u32, walFlushEveryOps = 1'u32)
  for i in 0 ..< n:
    s.insertRow("t", "w" & $wid & "_" & $i, row({
      "v": newTextValue("v" & $i)
    }))
  s.checkpoint()
  s.close()

main()
