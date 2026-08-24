import std/[unittest, os, times, options, strformat, monotimes, algorithm]
import ../src/boogie/stores/logstore

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_logstore_tests_" & unique)
  createDir(result)

suite "LogStore":

  let root = testRoot()

  test "append/get roundtrip assigns dense monotonic sequence numbers":
    var ls = openLogStore(root, "roundtrip")
    let s1 = ls.append("events", "one")
    let s2 = ls.append("events", "two")
    let s3 = ls.append("events", "", sync = false)

    check s1 == 1'u64
    check s2 == 2'u64
    check s3 == 3'u64

    let r1 = ls.get("events", s1)
    check r1.isSome
    check r1.get.seqNum == s1
    check r1.get.payload == "one"
    check r1.get.tsUnix > 0

    let r3 = ls.get("events", s3)
    check r3.isSome
    check r3.get.payload == ""

    check ls.get("events", 0'u64).isNone
    check ls.get("events", 99'u64).isNone
    check ls.get("missing", 1'u64).isNone

    ls.close()

  test "timestamp override is honored and auto-stamp fills current time":
    var ls = openLogStore(root, "timestamps")
    let before = getTime().toUnix()
    ls.append("t", "auto")
    let after = getTime().toUnix()
    ls.append("t", "fixed", tsUnix = 1700000000'i64)

    let autoRec = ls.get("t", 1'u64).get()
    check autoRec.tsUnix >= before and autoRec.tsUnix <= after

    let fixedRec = ls.get("t", 2'u64).get()
    check fixedRec.tsUnix == 1700000000'i64
    ls.close()

  test "forward/backward iterate in correct order":
    var ls = openLogStore(root, "iteration")
    for i in 1 .. 50:
      discard ls.append("s", "rec" & $i, sync = false)

    var count = 0
    var expectSeq: uint64 = 1
    for rec in ls.forward("s"):
      check rec.seqNum == expectSeq
      check rec.payload == "rec" & $expectSeq
      inc expectSeq
      inc count
    check count == 50

    count = 0
    expectSeq = 50
    for rec in ls.backward("s"):
      check rec.seqNum == expectSeq
      dec expectSeq
      inc count
    check count == 50
    var nopeCount = 0
    for _ in ls.backward("nope"):
      inc nopeCount
    check nopeCount == 0

    ls.close()

  test "rangeScan bounds are inclusive and clamp to the stream":
    var ls = openLogStore(root, "ranges")
    for i in 1 .. 20:
      discard ls.append("s", "r" & $i, sync = false)

    var seqs: seq[uint64] = @[]
    for rec in ls.rangeScan("s", 5'u64, 9'u64):
      seqs.add(rec.seqNum)
    check seqs == @[5'u64, 6, 7, 8, 9]

    seqs = @[]
    for rec in ls.rangeScan("s", 15'u64, 999'u64):
      seqs.add(rec.seqNum)
    check seqs == @[15'u64, 16, 17, 18, 19, 20]

    seqs = @[]
    for rec in ls.rangeScan("s", 0'u64, 3'u64):
      seqs.add(rec.seqNum)
    check seqs == @[1'u64, 2, 3]

    seqs = @[]
    for _ in ls.rangeScan("s", 9'u64, 5'u64):
      discard
    check seqs.len == 0
    ls.close()

  test "rangeByTime scans timestamp windows including out-of-order stamps":
    var ls = openLogStore(root, "bytime")
    # out-of-order timestamps exercise the sorted-insert slow path
    discard ls.append("m", "a", tsUnix = 100'i64)
    discard ls.append("m", "b", tsUnix = 300'i64)
    discard ls.append("m", "c", tsUnix = 200'i64)
    discard ls.append("m", "d", tsUnix = 400'i64)
    discard ls.append("other", "x", tsUnix = 250'i64)

    var payloads: seq[string] = @[]
    for rec in ls.rangeByTime("m", 150'i64, 350'i64):
      payloads.add(rec.payload)
    check payloads == @["c", "b"]

    payloads = @[]
    for rec in ls.rangeByTime("m", 100'i64, 400'i64):
      payloads.add(rec.payload)
    check payloads == @["a", "c", "b", "d"]

    payloads = @[]
    for rec in ls.rangeByTime("m", 401'i64, 500'i64):
      payloads.add(rec.payload)
    check payloads.len == 0
    ls.close()

  test "last(n) returns newest records first for cursor-based undo":
    var ls = openLogStore(root, "last")
    for i in 1 .. 7:
      discard ls.append("h", "action" & $i, tsUnix = int64(1000 + i))

    let lastThree = ls.last("h", 3)
    check lastThree.len == 3
    check lastThree[0].payload == "action7"
    check lastThree[1].payload == "action6"
    check lastThree[2].payload == "action5"

    check ls.last("h", 100).len == 7
    check ls.last("h", 0).len == 0
    check ls.last("nope", 5).len == 0
    ls.close()

  test "streams are isolated from each other":
    var ls = openLogStore(root, "isolation")
    discard ls.append("a", "a1")
    discard ls.append("a", "a2")
    discard ls.append("b", "b1")

    check ls.len("a") == 2
    check ls.len("b") == 1
    check ls.len("c") == 0
    check ls.tailSeq("a") == 2'u64
    check ls.firstSeq("b") == 1'u64
    check ls.firstSeq("c") == 0'u64

    let rb = ls.get("b", 1'u64).get()
    check rb.payload == "b1"
    ls.close()

  test "stream management helpers":
    var ls = openLogStore(root, "mgmt")
    check ls.hasStream("fresh") == false
    ls.createStream("fresh")
    check ls.hasStream("fresh")
    check ls.len("fresh") == 0
    expect LogStoreError:
      ls.createStream("fresh")
    expect LogStoreError:
      ls.createStream("")
    discard ls.append("auto", "x")
    check ls.hasStream("auto")

    var names: seq[string] = @[]
    for name in ls.streams:
      names.add(name)
    check names.sorted() == @["auto", "fresh"]
    ls.close()

  test "reopen rebuilds indexes and appends continue at the tail":
    block:
      var ls = openLogStore(root, "replay")
      for i in 1 .. 30:
        discard ls.append("ops", "op-" & $i, tsUnix = int64(5000 + i), sync = false)
      discard ls.append("audit", "logged", sync = true)
      ls.close()

    var ls2 = openLogStore(root, "replay")
    check ls2.len("ops") == 30
    check ls2.tailSeq("ops") == 30'u64
    check ls2.len("audit") == 1

    check ls2.get("ops", 1'u64).get().payload == "op-1"
    check ls2.get("ops", 17'u64).get().payload == "op-17"
    check ls2.get("ops", 30'u64).get().payload == "op-30"
    check ls2.get("ops", 30'u64).get().tsUnix == 5030'i64

    # time-range index survived recovery
    var cnt = 0
    for _ in ls2.rangeByTime("ops", 5010'i64, 5019'i64):
      inc cnt
    check cnt == 10

    # new appends continue after the recovered tail
    let cont = ls2.append("ops", "op-31")
    check cont == 31'u64
    check ls2.get("ops", 31'u64).get().payload == "op-31"

    var backwardCount = 0
    for rec in ls2.backward("ops"):
      if backwardCount == 0:
        check rec.payload == "op-31"
      inc backwardCount
    check backwardCount == 31
    ls2.close()

  test "group commit batches async appends into one flush":
    var ls = openLogStore(root, "groups", walFlushEveryOps = 100'u32)
    for i in 1 .. 99:
      discard ls.append("g", "v" & $i, sync = false)
    check ls.len("g") == 99

    # the 100th append crosses the flush threshold
    discard ls.append("g", "v100", sync = false)
    ls.close()

    var ls2 = openLogStore(root, "groups")
    check ls2.len("g") == 100
    check ls2.get("g", 100'u64).get().payload == "v100"
    ls2.close()

  test "cache on and off return identical results":
    for cap in [0, 1024]:
      var ls = openLogStore(root, "cache" & $cap, cacheCapacity = cap)
      for i in 1 .. 25:
        discard ls.append("c", "cached" & $i, tsUnix = int64(900 + i), sync = true)

      # double reads to exercise both cache hits and misses
      for round in 1 .. 2:
        var seen: seq[string] = @[]
        for rec in ls.backward("c"):
          seen.add(rec.payload)
        check seen.len == 25
        check seen[0] == "cached25"
        check seen[^1] == "cached1"

        let win = ls.last("c", 4)
        check win.len == 4
        check win[0].payload == "cached25"

        var byTime: seq[string] = @[]
        for rec in ls.rangeByTime("c", 905'i64, 910'i64):
          byTime.add(rec.payload)
        check byTime == @["cached5", "cached6", "cached7", "cached8",
                          "cached9", "cached10"]
      ls.close()

  test "in-memory store mirrors the disk API":
    var ls = newInMemoryLogStore()
    discard ls.append("m", "mem1", tsUnix = 10'i64)
    discard ls.append("m", "mem2", tsUnix = 42'i64)
    discard ls.append("n", "other")

    check ls.len("m") == 2
    check ls.get("m", 2'u64).get().payload == "mem2"
    check ls.get("m", 2'u64).get().tsUnix == 42'i64

    var payloads: seq[string] = @[]
    for rec in ls.backward("m"):
      payloads.add(rec.payload)
    check payloads == @["mem2", "mem1"]

    payloads = @[]
    for rec in ls.rangeByTime("m", 0'i64, 100'i64):
      payloads.add(rec.payload)
    check payloads.len == 2

    check ls.last("m", 1)[0].payload == "mem2"
    ls.close()

  test "append rejects empty stream names":
    var ls = openLogStore(root, "validate")
    expect LogStoreError:
      discard ls.append("", "orphan")
    ls.close()

suite "LogStore benchmarks":

  test "logstore ops/sec benchmark (append/scan/random get)":
    let N = 200_000

    let benchRoot = "tests" / "data" / "bench_logstore"
    if not dirExists(benchRoot):
      createDir(benchRoot)

    block:
      var ls = openLogStore(benchRoot, "bench_append",
                            walFlushEveryOps = 1000'u32, cacheCapacity = 1024)
      let t0 = getMonoTime()
      for i in 1 .. N:
        discard ls.append("b", "payload-" & $i & "-padding-to-realistic-size", sync = false)
      ls.checkpoint()
      let appendSecs = float((getMonoTime() - t0).inMilliSeconds) / 1000.0

      let t1 = getMonoTime()
      var scanned = 0
      for _ in ls.forward("b"):
        inc scanned
      let fwdSecs = float((getMonoTime() - t1).inMilliSeconds) / 1000.0
      check scanned == N

      let t2 = getMonoTime()
      var got = 0
      for i in 1 .. 10_000:
        let r = ls.get("b", uint64((i * 7919) mod N + 1))
        if r.isSome:
          inc got
      let getSecs = float((getMonoTime() - t2).inMilliSeconds) / 1000.0
      check got == 10_000

      let t3 = getMonoTime()
      var bwd = 0
      for _ in ls.backward("b"):
        inc bwd
        if bwd >= 10_000:
          break
      let bwdSecs = float((getMonoTime() - t3).inMilliSeconds) / 1000.0
      check bwd == 10_000

      echo fmt"[bench][logstore][disk] append={float(N) / appendSecs:>12.0f} ops/s forward={float(N) / fwdSecs:>10.0f} recs/s randomGet={10_000.0 / getSecs:>10.0f} ops/s backward={10_000.0 / bwdSecs:>10.0f} recs/s"
      ls.close()

    block:
      var ls = newInMemoryLogStore()
      let t0 = getMonoTime()
      for i in 1 .. N:
        discard ls.append("b", "payload-" & $i)
      let memSecs = float((getMonoTime() - t0).inMilliSeconds) / 1000.0
      echo fmt"[bench][logstore][mem] append={float(N) / memSecs:>12.0f} ops/s"
