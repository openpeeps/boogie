# Multi-process race worker: Log store.
# Usage: worker_log <dirPath> <workerId> <numOps>
import std/[os, strutils]
import ../../src/boogie/stores/logstore

proc main() =
  if paramCount() < 3:
    quit("usage: worker_log <dirPath> <workerId> <numOps>", 1)
  let dir = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  var ls = openLogStore(dir, "logs", walFlushEveryOps = 1'u32)
  for i in 0 ..< n:
    ls.append("events", "w" & $wid & "_" & $i, sync = false)
  ls.checkpoint()
  ls.close()

main()
