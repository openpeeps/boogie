import std/[unittest, os, osproc, strutils, times, monotimes, json, sequtils, options]
import ../src/boogie/stores/kv
import ../src/boogie/stores/rdbms
import ../src/boogie/db_boogie
import ../src/boogie/stores/vectorstore
import ../src/boogie/stores/columnar
import ../src/boogie/stores/graphstore
import ../src/boogie/stores/docstore
import ../src/boogie/stores/logstore

# ---------------------------------------------------------------------------
# Multi-process same-file race repro: two OS processes open the SAME disk
# path at the SAME time with disjoint key ranges, then a single handle
# reopens and verifies nothing was lost. Disk stores hold a cross-process
# exclusive lock (`boogie/filelock`) for their whole lifetime, so the second
# opener blocks (waits) until the first closes instead of interleaving WAL
# appends, snapshots, or LSN allocation. Run with: clue test
# ---------------------------------------------------------------------------

const
  BinDir = "tests" / "mp_workers" / "bin"
  WorkerNames = ["kv", "rdbms", "sql", "vec", "col", "graph", "doc", "log"]

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_mp_race_" & unique)
  createDir(result)

proc ensureWorkers() =
  ## Compiles worker binaries once (via clue, the project toolchain wrapper).
  ## Skips workers whose binary already exists so the suite stays fast.
  if not dirExists(BinDir):
    createDir(BinDir)
  for w in WorkerNames:
    let bin = BinDir / ("worker_" & w)
    if fileExists(bin):
      continue
    let src = "tests" / "mp_workers" / ("worker_" & w & ".nim")
    let code = execShellCmd("clue build " & src & " --out:" & bin & " --release")
    if code != 0:
      raise newException(CatchableError, "failed to build " & src)

proc runParallel(bin: string, runs: seq[seq[string]]): seq[int] =
  ## Starts one process per arg-set simultaneously, waits for all, returns
  ## exit codes in spawn order.
  var procs: seq[Process] = @[]
  for args in runs:
    procs.add(startProcess(bin, args = args,
      options = {poUsePath, poParentStreams}))
  for p in procs:
    result.add(p.waitForExit())
    p.close()

proc checkWorkersOk(bin: string, codes: seq[int]) =
  for i, c in codes:
    if c != 0:
      echo "  worker ", bin, "[", i, "] exited with code ", c
  check codes.allIt(it == 0)

suite "mp race repro (multi-process, same file)":
  ensureWorkers()

  test "kv: 2 procs x 1000 puts on same path":
    const P = 2
    const N = 1000
    let root = testRoot()
    let base = root / "kv"
    checkWorkersOk(BinDir / "worker_kv", runParallel(absolutePath(BinDir / "worker_kv"),
      @[@[base, "0", $N], @[base, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      let kv = newKvStore(base, ksmDisk, enableWal = true)
      count = kv.len
      if kv.get("w0_0").isNone or kv.get("w1_" & $(N - 1)).isNone:
        ok = false
        msg = "spot read missing after mp writers"
      kv.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [kv] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "rdbms: 2 procs x 500 inserts on same path":
    const P = 2
    const N = 500
    let root = testRoot()
    let base = root / "rdbms"
    block:
      let s = newStore(base, smDisk, enableWal = true)
      s.createTable(newTable(name = "t", primaryKey = "id",
        primaryKeyMode = pkmManual,
        columns = [newColumn("id", dtText, false),
                   newColumn("v", dtText, false)]))
      s.checkpoint()
      s.close()
    checkWorkersOk(BinDir / "worker_rdbms", runParallel(absolutePath(BinDir / "worker_rdbms"),
      @[@[base, "0", $N], @[base, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      let s = newStore(base, smDisk, enableWal = true)
      let t = s.getTable("t").get()
      count = 0
      for _, _ in t.allRows:
        inc count
      if s.getRow("t", "w0_0").isNone or s.getRow("t", "w1_" & $(N - 1)).isNone:
        ok = false
        msg = "spot read missing after mp writers"
      s.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [rdbms] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "sql driver: 2 procs x 500 inserts on same path":
    const P = 2
    const N = 500
    let root = testRoot()
    let base = root / "sql.db"
    block:
      let db = open(base, "", "", "mp")
      db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
      db.close()
    checkWorkersOk(BinDir / "worker_sql", runParallel(absolutePath(BinDir / "worker_sql"),
      @[@[base, "0", $N], @[base, "1", $N]]))
    var ok = true
    var msg = ""
    var count = ""
    try:
      let db = open(base, "", "", "mp")
      count = db.getValue(sql"SELECT COUNT(*) FROM b")
      db.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [sql] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == $(P * N)

  test "vector: 2 procs x 500 inserts on same path":
    const P = 2
    const N = 500
    let root = testRoot()
    let base = root / "vec"
    block:
      let vs = newVectorStore(base, smDisk, enableWal = true)
      vs.createCollection(newCollection("emb", 2))
      vs.checkpoint()
      vs.close()
    checkWorkersOk(BinDir / "worker_vec", runParallel(absolutePath(BinDir / "worker_vec"),
      @[@[base, "0", $N], @[base, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      let vs = newVectorStore(base, smDisk, enableWal = true)
      let c = vs.getCollection("emb").get()
      count = c.len
      if vs.get("emb", "w0_0").isNone or vs.get("emb", "w1_" & $(N - 1)).isNone:
        ok = false
        msg = "spot read missing after mp writers"
      vs.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [vec] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "columnar: 2 procs x 200 batches on same root":
    const P = 2
    const N = 200
    let root = testRoot()
    let croot = root / "colstore"
    block:
      var s = openColumnarStore(croot)
      s.createTable(TableSchema(name: "events", primaryKey: "id", rowCount: 0,
        columns: @[ColumnSchema(name: "id", kind: ctInt64, nullable: false, codec: ccNone),
                   ColumnSchema(name: "user", kind: ctString, nullable: false, codec: ccNone),
                   ColumnSchema(name: "amount", kind: ctFloat64, nullable: false, codec: ccNone)]),
        sync = true)
    checkWorkersOk(BinDir / "worker_col", runParallel(absolutePath(BinDir / "worker_col"),
      @[@[croot, "0", $N], @[croot, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      var s = openColumnarStore(croot)
      count = s.scan("events", @["id"]).len
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [col] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "graph: 2 procs x 150 nodes on same root":
    const P = 2
    const N = 150
    let root = testRoot()
    let groot = root / "graphstore"
    createDir(groot)
    checkWorkersOk(BinDir / "worker_graph", runParallel(absolutePath(BinDir / "worker_graph"),
      @[@[groot, "0", $N], @[groot, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      var gs = openGraphStore(groot)
      count = gs.findNodesByLabel("N").len
      closeGraphStore(gs)
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [graph] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "docstore: 2 procs x 500 upserts on same path":
    const P = 2
    const N = 500
    let root = testRoot()
    let droot = root / "docstore"
    checkWorkersOk(BinDir / "worker_doc", runParallel(absolutePath(BinDir / "worker_doc"),
      @[@[droot, "0", $N], @[droot, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      var store = openDocumentStore(droot, name = "documents")
      count = store.len
      if store.get("w0_0").isNone or store.get("w1_" & $(N - 1)).isNone:
        ok = false
        msg = "spot read missing after mp writers"
      store.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [doc] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N

  test "logstore: 2 procs x 1000 appends on same path":
    const P = 2
    const N = 1000
    let root = testRoot()
    let lroot = root / "logstore"
    checkWorkersOk(BinDir / "worker_log", runParallel(absolutePath(BinDir / "worker_log"),
      @[@[lroot, "0", $N], @[lroot, "1", $N]]))
    var ok = true
    var msg = ""
    var count = -1
    try:
      var ls = openLogStore(lroot, "logs")
      count = ls.len("events")
      ls.close()
    except CatchableError as e:
      ok = false
      msg = "reopen failed: " & e.msg
    echo "  [log] count=", count, " expected=", P * N, " ", msg
    check ok
    check count == P * N
