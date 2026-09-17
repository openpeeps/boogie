import std/[unittest, os, osproc, strutils, times, monotimes, options, streams]
import ../src/boogie/stores/kv
import ../src/boogie/stores/rdbms
import ../src/boogie/filelock

# ---------------------------------------------------------------------------
# Multi-process concurrent-read regression test for the ".db hang" report:
# two processes reading the same path at the same time used to hang forever
# because every disk open took a blocking whole-lifetime `flock(LOCK_EX)`,
# even for pure reads.
#
# Writers still take `LOCK_EX`; `readOnly = true` opens take `LOCK_SH`, so
# any number of reader processes can share a path while no writer holds it.
# Run with: nim r --threads:on --mm:arc tests/test17.nim
# ---------------------------------------------------------------------------

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_mp_read_" & unique)
  createDir(result)

proc selfBin(): string =
  ## Path of the currently running test binary (used to spawn reader/holder
  ## workers in `workerMode` below without an external build step).
  getAppFilename()

proc runWorker(mode, path, extra: string, timeoutMs: int): tuple[code: int, output: string] =
  ## Starts this binary as a worker, waits up to `timeoutMs`, and on timeout
  ## kills it and reports code -1 (a hang fails the check instead of hanging
  ## the suite).
  var p = startProcess(selfBin(), args = @["--worker:" & mode, path, extra],
    options = {poUsePath})
  let outp = p.outputStream()
  var code = p.waitForExit(timeoutMs)
  var output = ""
  if code == -1:
    p.terminate()
    discard p.waitForExit(2000)
    output = "<timeout: worker hung>"
  else:
    try:
      output = outp.readAll()
    except CatchableError:
      discard
  p.close()
  (code, output)

proc workerMode(): bool =
  ## When invoked as `--worker:<mode> <path> <extra>`, acts as a child
  ## process and returns true (the suite body is skipped).
  for i in 1 .. paramCount():
    let a = paramStr(i)
    if a.startsWith("--worker:"):
      let mode = a[len("--worker:") .. ^1]
      let path = paramStr(i + 1)
      let extra = if paramCount() >= i + 2: paramStr(i + 2) else: ""
      case mode
      of "kvReader":
        let s = newKvStore(path, ksmDisk, enableWal = true, readOnly = true)
        echo "v=" & $s.get("k1") & " hold=" & extra
        sleep(parseInt(extra))
        s.close()
      of "kvHolder":
        let s = newKvStore(path, ksmDisk, enableWal = true)
        s.put("k1", "v1")
        echo "holding"
        sleep(parseInt(extra))
        s.close()
      of "rdbmsReader":
        let s = newStore(path, smDisk, enableWal = true, readOnly = true)
        echo "v=" & $s.getRow("t", "k1") & " hold=" & extra
        sleep(parseInt(extra))
        s.close()
      of "rdbmsHolder":
        let s = newStore(path, smDisk, enableWal = true)
        s.createTable(newTable(name = "t", primaryKey = "id",
          primaryKeyMode = pkmManual,
          columns = [newColumn("id", dtText, false),
                     newColumn("v", dtText, false)]))
        s.insertRow("t", "k1", row({"id": newTextValue("k1"),
          "v": newTextValue("v1")}))
        s.checkpoint()
        echo "holding"
        sleep(parseInt(extra))
        s.close()
      else:
        quit("unknown worker mode: " & mode, 2)
      return true
  false

if workerMode():
  quit(0)

suite "mp concurrent reads (shared lock, readOnly)":
  test "kv: two readOnly processes read the same path concurrently":
    let base = testRoot() / "kv"
    block:
      let s = newKvStore(base, ksmDisk, enableWal = true)
      s.put("k1", "v1")
      s.close()
    var p1 = startProcess(selfBin(), args = @["--worker:kvReader", base, "3000"],
      options = {poUsePath})
    var p2 = startProcess(selfBin(), args = @["--worker:kvReader", base, "3000"],
      options = {poUsePath})
    let c1 = p1.waitForExit(15000)
    let c2 = p2.waitForExit(15000)
    if c1 == -1: p1.terminate()
    if c2 == -1: p2.terminate()
    p1.close()
    p2.close()
    check c1 == 0
    check c2 == 0

  test "rdbms: two readOnly processes read the same path concurrently":
    let base = testRoot() / "rdbms"
    block:
      let s = newStore(base, smDisk, enableWal = true)
      s.createTable(newTable(name = "t", primaryKey = "id",
        primaryKeyMode = pkmManual,
        columns = [newColumn("id", dtText, false),
                   newColumn("v", dtText, false)]))
      s.insertRow("t", "k1", row({"id": newTextValue("k1"),
        "v": newTextValue("v1")}))
      s.checkpoint()
      s.close()
    var p1 = startProcess(selfBin(), args = @["--worker:rdbmsReader", base, "3000"],
      options = {poUsePath})
    var p2 = startProcess(selfBin(), args = @["--worker:rdbmsReader", base, "3000"],
      options = {poUsePath})
    let c1 = p1.waitForExit(15000)
    let c2 = p2.waitForExit(15000)
    if c1 == -1: p1.terminate()
    if c2 == -1: p2.terminate()
    p1.close()
    p2.close()
    check c1 == 0
    check c2 == 0

  test "readOnly stores reject writes":
    let kbase = testRoot() / "kvro"
    block:
      let s = newKvStore(kbase, ksmDisk, enableWal = true)
      s.put("k1", "v1")
      s.close()
    let kro = newKvStore(kbase, ksmDisk, enableWal = true, readOnly = true)
    check kro.get("k1").get() == "v1"
    var raised = false
    try:
      kro.put("k2", "v2")
    except KvStoreError:
      raised = true
    check raised
    raised = false
    try:
      discard kro.delete("k1")
    except KvStoreError:
      raised = true
    check raised
    raised = false
    try:
      kro.checkpoint()
    except KvStoreError:
      raised = true
    check raised
    kro.close()

    let rbase = testRoot() / "rdbmsro"
    block:
      let s = newStore(rbase, smDisk, enableWal = true)
      s.createTable(newTable(name = "t", primaryKey = "id",
        primaryKeyMode = pkmManual,
        columns = [newColumn("id", dtText, false)]))
      s.insertRow("t", "k1", row({"id": newTextValue("k1")}))
      s.checkpoint()
      s.close()
    let rro = newStore(rbase, smDisk, enableWal = true, readOnly = true)
    check rro.getRow("t", "k1").isSome
    raised = false
    try:
      rro.insertRow("t", "k2", row({"id": newTextValue("k2")}))
    except StoreError:
      raised = true
    check raised
    rro.close()

  test "non-blocking lock reports busy instead of hanging":
    let base = testRoot() / "kvbusy"
    var holder = startProcess(selfBin(),
      args = @["--worker:kvHolder", base, "4000"], options = {poUsePath})
    sleep(1000) # let the holder take the exclusive lock
    var busy = false
    try:
      var l = tryAcquireFileLock(base.changeFileExt(".lock"), lmExclusive)
      l = FileLock()
    except FileLockBusyError:
      busy = true
    check busy
    # A shared (reader) request also conflicts with the held exclusive lock.
    busy = false
    try:
      var l = tryAcquireFileLock(base.changeFileExt(".lock"), lmShared)
      l = FileLock()
    except FileLockBusyError:
      busy = true
    check busy
    check holder.waitForExit(15000) == 0
    holder.close()
    # After the holder exits the path is acquirable again.
    var ok = false
    try:
      var l = tryAcquireFileLock(base.changeFileExt(".lock"), lmExclusive)
      l = FileLock()
      ok = true
    except FileLockBusyError:
      discard
    check ok

  test "runWorker helper is exercised (keeps output plumbing honest)":
    let base = testRoot() / "kvhelper"
    block:
      let s = newKvStore(base, ksmDisk, enableWal = true)
      s.put("k1", "v1")
      s.close()
    let (code, output) = runWorker("kvReader", base, "100", 15000)
    check code == 0
    check "v1" in output
