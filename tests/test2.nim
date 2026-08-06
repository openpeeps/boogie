import std/[unittest, options, json, times, strformat, os, tables]
import ../src/boogie/stores/kv

suite "KeyValue Store WAL + memory store tests":
  test "in-memory put/get/delete":
    let kv = newInMemoryKvStore()
    check kv.isEmpty
    kv.put("foo", "bar")
    check kv.len == 1
    check kv.get("foo").get() == "bar"
    check kv.hasKey("foo")
    kv.put("foo", "baz")
    check kv.get("foo").get() == "baz"
    check kv.delete("foo")
    check not kv.hasKey("foo")
    check kv.isEmpty

  test "disk store with WAL and recovery":
    let dbPath = "test_kvstore"

    block:
      let kv = newKvStore(dbPath, ksmDisk, enableWal = true, checkpointEveryOps = 2)
      kv.put("a", "1")
      kv.put("b", "2")
      kv.put("c", "3")
      check kv.get("a").get() == "1"
      check kv.get("b").get() == "2"
      check kv.get("c").get() == "3"
      kv.delete("b")
      check not kv.hasKey("b")
      kv.checkpoint()

    block:
      let kv2 = newKvStore(dbPath, ksmDisk, enableWal = true)
      check kv2.get("a").get() == "1"
      check kv2.get("c").get() == "3"
      check not kv2.hasKey("b")
      kv2.put("d", "4")
      check kv2.get("d").get() == "4"

  test "iterator pairsUnordered":
    let kv = newInMemoryKvStore()
    kv.put("x", "1")
    kv.put("y", "2")
    var keys: seq[string]
    for k, v in kv.pairsUnordered:
      keys.add(k)
      check v in ["1", "2"]
    check keys.len == 2

  test "lazy-read disk store (offset-based)":
    let dbPath = "tests" / "data" / "bench_kvstore_lazy"
    block:
      let kv = newKvStore(dbPath, ksmDisk, enableWal = true, lazyReads = true)
      kv.put("a", "1")
      kv.put("b", "2")
      kv.put("c", "3")
      check kv.get("a").get() == "1"
      check kv.get("b").get() == "2"
      check kv.get("c").get() == "3"
      check kv.delete("b")
      check not kv.hasKey("b")
      var n = 0
      for (k, v) in kv.pairsUnordered:
        inc n
        check v in ["1", "2", "3"]
      check n == 2
    block:
      let kv2 = newKvStore(dbPath, ksmDisk, enableWal = true, lazyReads = true)
      check kv2.get("a").get() == "1"
      check kv2.get("c").get() == "3"
      check not kv2.hasKey("b")
      kv2.put("c", "33")
      check kv2.get("c").get() == "33"

suite "KeyValue Store throughput benchmarks":
  test "in-memory put/get/delete ops per second":
    const n = 100_000
    let kv = newInMemoryKvStore()

    var t0 = cpuTime()
    for i in 0..<n:
      kv.put("k" & $i, "v" & $i)
    let putSecs = cpuTime() - t0

    t0 = cpuTime()
    for i in 0..<n:
      discard kv.get("k" & $i)
    let getSecs = cpuTime() - t0

    t0 = cpuTime()
    for i in 0..<n:
      discard kv.delete("k" & $i)
    let delSecs = cpuTime() - t0

    let putOps = float(n) / max(putSecs, 1e-9)
    let getOps = float(n) / max(getSecs, 1e-9)
    let delOps = float(n) / max(delSecs, 1e-9)

    echo fmt"[bench][mem] put={putOps:>10.0f} ops/s get={getOps:>10.0f} ops/s del={delOps:>10.0f} ops/s"
    check putOps > 0
    check getOps > 0
    check delOps > 0

  test "disk+wal put/get/delete ops per second":
    const n = 20_000
    let dbPath = "tests" / "data" / "bench_kvstore"

    block:
      let kv = newKvStore(
        dbPath,
        ksmDisk,
        enableWal = true,
        checkpointEveryOps = 0,
        walFlushEveryOps = 1000
      )

      var t0 = cpuTime()
      for i in 0..<n:
        kv.put("k" & $i, "v" & $i)
      kv.checkpoint()
      let putSecs = cpuTime() - t0

      t0 = cpuTime()
      for i in 0..<n:
        discard kv.get("k" & $i)
      let getSecs = cpuTime() - t0

      t0 = cpuTime()
      for i in 0..<n:
        discard kv.delete("k" & $i)
      kv.checkpoint()
      let delSecs = cpuTime() - t0

      let putOps = float(n) / max(putSecs, 1e-9)
      let getOps = float(n) / max(getSecs, 1e-9)
      let delOps = float(n) / max(delSecs, 1e-9)

      echo fmt"[bench][disk+wal] put={putOps:>10.0f} ops/s get={getOps:>10.0f} ops/s del={delOps:>10.0f} ops/s"
      check putOps > 0
      check getOps > 0
      check delOps > 0

  test "lazy-read keeps values on disk (not in memory)":
    const N = 50000
    const V = 256
    let value = newString(V)
    let eagerPath = "tests" / "data" / "mem_kv_eager"
    let lazyPath = "tests" / "data" / "mem_kv_lazy"

    # eager: values retained in memory
    block:
      let kv = newKvStore(eagerPath, ksmDisk, enableWal = true, walFlushEveryOps = 1000)
      for i in 0..<N:
        kv.put("k" & $i, value)
      kv.checkpoint()
      let retained = kv.retainedValueBytes()
      echo fmt"[bench][kv][mem] eager retains {float(retained) / 1_048_576.0:>8.2f} MiB of values in RAM (N={N}, value={V}B)"
      check retained >= N * V

    # lazy: only the key -> offset index retained
    block:
      let kv = newKvStore(lazyPath, ksmDisk, enableWal = true, lazyReads = true)
      for i in 0..<N:
        kv.put("k" & $i, value)
      echo fmt"[bench][kv][mem] lazy retains {kv.retainedValueBytes()} bytes of values in RAM (index only)"
      check kv.retainedValueBytes() == 0

      var t0 = cpuTime()
      for i in 0..<N:
        discard kv.get("k" & $i)
      let lazyGetSecs = cpuTime() - t0
      let lazyGetRate = float(N) / max(lazyGetSecs, 1e-9)
      echo fmt"[bench][kv][latency] lazy get (disk offset read)={lazyGetRate:>10.0f} ops/s"
