import std/[unittest, os, osproc, strutils, times, monotimes, sequtils, options,
             atomics, streams]
import ../src/boogie/stores/kv
import ../src/boogie/signals

when defined(posix):
  import std/posix as ops

# ---------------------------------------------------------------------------
# OS signal tests, in the style of powpow's signal suite but adapted: boogie
# has no event loop, so delivery runs on a watcher thread and callbacks must
# be thread-safe (atomics below, never bare `var` captures).
# - relay unit tests (listen/emit/once/unlisten/custom channels)
# - in-process self-delivery of SIGUSR1
# - multi-process: SIGHUP flushes unflushed WAL then terminates the worker
#   (nobody else listens, so crashsafe owns the outcome — a lingering worker
#   here is the hung-shell bug); the opt-out (setTerminateSignals({}))
#   flushes but survives; SIGTERM exits gracefully via a custom listener
#   (which suppresses termination); and custom signals (raw SIGUSR1 + enum
#   SIGUSR2) reach user callbacks.
# Run with: clue test
# ---------------------------------------------------------------------------

const SigBin = "tests" / "mp_workers" / "bin" / "worker_signal"

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_signals_" & unique)
  createDir(result)

proc ensureSignalWorker() =
  if not fileExists(SigBin):
    let code = execShellCmd("clue build tests/mp_workers/worker_signal.nim" &
      " --out:" & SigBin & " --release")
    if code != 0:
      raise newException(CatchableError, "failed to build worker_signal")

var relayHits: Atomic[int]
var relayOnce: Atomic[int]
var selfUsr1: Atomic[int]

template countCb(): SignalCallback =
  proc(sig: cint) {.closure, gcsafe.} =
    discard relayHits.fetchAdd(1)

template onceCb(): SignalCallback =
  proc(sig: cint) {.closure, gcsafe.} =
    discard relayOnce.fetchAdd(1)

template usr1Cb(): SignalCallback =
  proc(sig: cint) {.closure, gcsafe.} =
    discard selfUsr1.fetchAdd(1)

suite "signals: relay (in-process)":
  test "listen + emit fans out":
    relayHits.store(0)
    let a = listenCustom(9001, countCb())
    let b = listenCustom(9001, countCb())
    emitSignal(9001)
    check relayHits.load == 2
    a.unlisten()
    b.unlisten()

  test "listenOnce fires at most once":
    relayOnce.store(0)
    let h = listenCustomOnce(9002, onceCb())
    emitSignal(9002)
    emitSignal(9002)
    check relayOnce.load == 1
    h.unlisten()

  test "unlisten silences":
    relayHits.store(0)
    let h = listenCustom(9003, countCb())
    h.unlisten()
    emitSignal(9003)
    check relayHits.load == 0

  test "custom channels are isolated":
    relayHits.store(0)
    let h = listenCustom(9004, countCb())
    emitSignal(9005)
    check relayHits.load == 0
    emitSignal(9004)
    check relayHits.load == 1
    h.unlisten()

  test "signal numbers match the platform":
    check SignalHup.signalNumber == 1
    check SignalInt.signalNumber == 2
    check SignalTerm.signalNumber == 15
    check SignalKill.signalNumber == 9
    when defined(macosx) or defined(freebsd) or defined(netbsd) or
         defined(openbsd) or defined(dragonfly):
      check SignalUsr1.signalNumber == 30
      check SignalUsr2.signalNumber == 31
      check SignalBus.signalNumber == 10
    elif defined(linux):
      check SignalUsr1.signalNumber == 10
      check SignalUsr2.signalNumber == 12
      check SignalBus.signalNumber == 7
      check rtSignal(0) == 34
      check rtSignal(30) == 64

  test "platform-only signals raise":
    when defined(linux):
      expect(SignalError):
        discard SignalEmt.signalNumber
    elif defined(macosx) or defined(freebsd):
      expect(SignalError):
        discard SignalPwr.signalNumber

when defined(posix):
  test "self-delivery: SIGUSR1 reaches a listener, process survives":
    selfUsr1.store(0)
    let h = listenSignal(SignalUsr1, usr1Cb())
    discard ops.kill(ops.getpid(), SignalUsr1.signalNumber)
    var waited = 0
    while selfUsr1.load == 0 and waited < 5000:
      sleep(10)
      inc waited, 10
    check selfUsr1.load == 1
    h.unlisten()

when defined(posix):
  suite "signals: multi-process":
    ensureSignalWorker()

    proc spawnWorker(mode, dir: string): Process =
      startProcess(absolutePath(SigBin), args = @[mode, dir],
        options = {poUsePath})

    proc waitReady(p: Process) =
      var line = ""
      try:
        line = p.outputStream.readLine()
      except EOFError, OSError:
        line = "<eof>"
      if line != "READY":
        p.terminate()
        raise newException(CatchableError,
          "worker never became ready (got: " & line & ")")

    proc waitExitOk(p: Process, timeoutMs = 15000): int =
      var waited = 0
      while p.running:
        sleep(100)
        inc waited, 100
        if waited >= timeoutMs:
          p.terminate()
          raise newException(CatchableError, "worker did not exit in time")
      p.peekExitCode

    test "SIGHUP flushes unflushed WAL then terminates the process":
      let root = testRoot()
      var p = spawnWorker("hupkill", root)
      waitReady(p)
      sleep(300) # let all puts land in the group-commit buffer
      discard ops.kill(Pid(p.processID), ops.SIGHUP)
      # Nobody else listens for SIGHUP, so crashsafe terminates after
      # flushing: the worker must exit PROMPTLY on its own (a hang here is
      # the terminal-tab-close bug — the process must die, not linger).
      discard waitExitOk(p)
      check not p.running
      discard ops.kill(Pid(p.processID), ops.SIGKILL) # no-op once dead
      p.close()
      let kv = newKvStore(root / "sig", ksmDisk, enableWal = true)
      var count = 0
      for k in ["k0", "k149", "k299"]:
        if kv.get(k).isSome:
          inc count
      check count == 3
      check kv.len == 300
      kv.close()

    test "SIGHUP opt-out via setTerminateSignals({}) flushes but survives":
      let root = testRoot()
      var p = spawnWorker("hupsurvive", root)
      waitReady(p)
      sleep(300)
      discard ops.kill(Pid(p.processID), ops.SIGHUP)
      sleep(1000) # let the watcher-thread flush land on disk
      check p.running # opt-out: still alive after SIGHUP
      discard ops.kill(Pid(p.processID), ops.SIGKILL) # no cleanup possible past this point
      discard waitExitOk(p)
      p.close()
      let kv = newKvStore(root / "sig", ksmDisk, enableWal = true)
      check kv.len == 300
      check kv.get("k299").isSome
      kv.close()

    test "SIGTERM runs a custom listener and exits gracefully":
      let root = testRoot()
      var p = spawnWorker("term", root)
      waitReady(p)
      sleep(300)
      discard ops.kill(Pid(p.processID), ops.SIGTERM)
      check waitExitOk(p) == 0
      p.close()
      check fileExists(root / "term.marker")
      let kv = newKvStore(root / "sig", ksmDisk, enableWal = true)
      check kv.len == 300
      kv.close()

    test "custom signals: raw SIGUSR1 + enum SIGUSR2 reach callbacks":
      let root = testRoot()
      var p = spawnWorker("custom", root)
      waitReady(p)
      sleep(300)
      discard ops.kill(Pid(p.processID), ops.SIGUSR1)
      sleep(300)
      discard ops.kill(Pid(p.processID), ops.SIGUSR2)
      check waitExitOk(p) == 0
      p.close()
      check fileExists(root / "usr1.marker")
      check fileExists(root / "usr2.marker")

    test "SIGSEGV flushes unflushed WAL before dying (fatal path)":
      let root = testRoot()
      var p = spawnWorker("segv", root)
      waitReady(p)
      # The worker kills itself with SIGSEGV ~500ms after READY; normal exit
      # (code 0) would mean the crash never happened and the test is void.
      var waited = 0
      while p.running:
        sleep(100)
        inc waited, 100
        if waited >= 15000:
          p.terminate()
          raise newException(CatchableError, "segv worker did not die")
      check p.peekExitCode != 0
      p.close()
      let kv = newKvStore(root / "sig", ksmDisk, enableWal = true)
      check kv.len == 300
      check kv.get("k299").isSome
      kv.close()
