# Multi-process race worker: KV store.
# Usage: worker_kv <basePath> <workerId> <numOps>
import std/[os, strutils]
import ../../src/boogie/stores/kv

proc main() =
  if paramCount() < 3:
    quit("usage: worker_kv <basePath> <workerId> <numOps>", 1)
  let base = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  let kv = newKvStore(base, ksmDisk, enableWal = true,
    checkpointEveryOps = 0'u32, walFlushEveryOps = 1'u32)
  for i in 0 ..< n:
    kv.put("w" & $wid & "_" & $i, "v" & $i)
  kv.checkpoint()
  kv.close()

main()
