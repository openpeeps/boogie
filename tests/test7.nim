import std/[unittest, os, options, strutils]
import ../src/boogie/stores/kv
import ../src/boogie/crashsafe

const DataDir = "tests" / "data"
if not dirExists(DataDir):
  createDir(DataDir)

# ---------------------------------------------------------------- thread helpers

type
  KvWriter = object
    kv: KvStore
    id: int
    n: int

proc kvWriter(a: KvWriter) {.thread.} =
  for i in 0..<a.n:
    a.kv.put("w" & $a.id & "_" & $i, "v" & $i)
  var ok = true
  for i in 0..<a.n:
    let g = a.kv.get("w" & $a.id & "_" & $i)
    if g.isNone or g.get != "v" & $i: ok = false
  echo "  kv writer ", a.id, " visible=", ok

suite "concurrency (enableConcurrency = true)":
  test "kv store: concurrent put/get across 4 writers":
    const N = 2000
    let kv = newKvStore("", ksmInMemory, false, enableConcurrency = true)
    kv.put("a", "1")
    check kv.get("a").get() == "1"

    var writers: array[4, Thread[KvWriter]]
    for i in 0..<4:
      createThread(writers[i], kvWriter, KvWriter(kv: kv, id: i, n: N))
    joinThreads(writers)

    check kv.len == 4 * N + 1
    kv.close()

suite "crash safety":
  test "flushAllStores persists unflushed WAL entries":
    let path = DataDir / "crash_kv"
    block:
      let kv = newKvStore(path, ksmDisk, enableWal = true, walFlushEveryOps = 100000)
      kv.put("a", "1")
      kv.put("b", "2")
      # nothing flushed yet (walFlushEveryOps is huge); the crash-safe registry
      # flush writes the pending WAL entries
      flushAllStores()
    block:
      let kv = newKvStore(path, ksmDisk, enableWal = true)
      check kv.get("a").get() == "1"
      check kv.get("b").get() == "2"
      kv.close()

  test "close flushes the WAL":
    let path = DataDir / "close_kv"
    block:
      let kv = newKvStore(path, ksmDisk, enableWal = true, walFlushEveryOps = 100000)
      kv.put("x", "10")
      kv.close()
    block:
      let kv = newKvStore(path, ksmDisk, enableWal = true)
      check kv.get("x").get() == "10"
      kv.close()
