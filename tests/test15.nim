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
# The crashsafe-dependent half of the suite is compiled out under
# `-d:disableCrashSafe` (boogie owns no signal in that build); the relay and
# generic-signal tests still run, plus the "arms nothing" check.
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
    when defined(windows):
      # Only Ctrl+C-like signals exist on Windows; the rest raise.
      check SignalInt.signalNumber == 2
      check SignalTerm.signalNumber == 15
      check SignalKill.signalNumber == 9
      expect(SignalError):
        discard SignalHup.signalNumber
    else:
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

when defined(disableCrashSafe):
  # Only meaningful in the flagged build; the default build's counterpart is
  # the SIGHUP/SIGSEGV multi-process tests below.
  suite "crashsafe: -d:disableCrashSafe":
    const watch = [SignalSegv, SignalAbrt, SignalBus, SignalIll, SignalFpe,
                   SignalInt, SignalTerm, SignalHup, SignalQuit]

    when defined(posix):
      proc querySigaction(sig: cint, act, old: ptr ops.Sigaction): cint
        {.importc: "sigaction", header: "<signal.h>", sideEffect.}
        ## Disposition query. `std/posix`'s `sigaction` has no NULL-`act`
        ## overload, and passing a dummy action *installs* it — which would
        ## clobber exactly the handlers this test is trying to observe.

    proc dispositions(): seq[uint] =
      ## The OS's current `sa_handler` for `watch`, sampled before and after a
      ## store open. The flag promises boogie changes NOTHING, which is
      ## stronger than "everything is SIG_DFL": the Nim runtime already owns
      ## the fatal signals, and a host crash reporter owns more.
      when defined(posix):
        var old: ops.Sigaction
        for s in watch:
          discard querySigaction(cint(s.signalNumber), nil, addr old)
          result.add cast[uint](old.sa_handler)
      else:
        for s in watch:
          result.add 0'u

    test "a store open leaves every signal disposition untouched":
      # The GUI-app contract, asserted at the OS level: boogie arms no signal
      # and replaces no handler, so crash reporters keep their own fatal
      # handlers and SIGHUP/SIGTERM keep the host's own semantics.
      let armedBefore = watchedSignals()
      let dispBefore = dispositions()
      let root = testRoot()
      var kv = newKvStore(root / "nosig", ksmDisk, enableWal = true)
      kv.put("k", "v")
      kv.close()
      check watchedSignals() == armedBefore
      check dispositions() == dispBefore
      for s in watch:
        check countSignalListeners(s) == 0

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

    when not defined(disableCrashSafe):
      # The three tests below assert crashsafe's own signal behaviour (flush
      # on HUP, flush-then-terminate, flush on a fatal signal). With
      # `-d:disableCrashSafe` boogie installs no handlers at all, so they
      # would fail by construction — see the "arms nothing" test above.

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

    # Host-owned signals: crashsafe is a bystander here, so these hold with or
    # without `-d:disableCrashSafe` (the worker closes its store on the way
    # out, which is what makes the data durable).
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
