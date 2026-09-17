# Multi-process signal worker for tests/test15.nim.
# Modes:
#   hupkill <dir>  - put N keys with a huge flush interval (nothing durable),
#                    print READY, then sleep until killed. Used with SIGHUP
#                    (crashsafe flushes, then the process DIES by SIGHUP since
#                    nobody else listens for it) followed by SIGKILL (a no-op
#                    once dead; kept to prove no cleanup is needed): surviving
#                    data proves the HUP handler flushed before terminating.
#   hupsurvive <dir> - same setup, but opts out via setTerminateSignals({}):
#                    SIGHUP flushes and the process SURVIVES. The parent
#                    asserts aliveness, then SIGKILLs; reopened data proves
#                    the HUP-time flush.
#   term <dir>     - same setup, but exits gracefully on SIGTERM via a custom
#                    listener (marker file + close + quit 0). The custom
#                    listener suppresses crashsafe's terminate-on-signal, so
#                    the host owns the outcome.
#   custom <dir>   - waits for SIGUSR1 (raw number) and SIGUSR2 (enum), writes
#                    a marker per signal, then exits 0.
#   segv <dir>      - put N keys unflushed, print READY, then die with a real
#                    SIGSEGV. The fatal-signal handler must flush first: the
#                    parent reopens and finds the data (exit procs never run
#                    on a fatal signal, so only the handler could persist it).
import std/[os, strutils, atomics]
import ../../src/boogie/stores/kv
import ../../src/boogie/signals
import ../../src/boogie/crashsafe

when defined(posix):
  import std/posix as ops

const N = 300

var gotTerm: Atomic[bool]
var gotUsr1: Atomic[bool]
var gotUsr2: Atomic[bool]

proc ready() =
  echo "READY"
  flushFile(stdout)

proc putUnflushed(dir: string): KvStore =
  result = newKvStore(dir / "sig", ksmDisk, enableWal = true,
    checkpointEveryOps = 0'u32, walFlushEveryOps = 100000'u32)
  for i in 0 ..< N:
    result.put("k" & $i, "v" & $i)

proc modeHupkill(dir: string) =
  let kv = putUnflushed(dir)
  ready()
  while true:
    sleep(200)
    # Keep the store alive; reference it so it is not optimized away.
    if kv.len < 0:
      quit(1)

proc modeHupSurvive(dir: string) =
  # Opt out of flush-then-terminate: HUP flushes but the process lives on.
  setTerminateSignals({})
  let kv = putUnflushed(dir)
  ready()
  while true:
    sleep(200)
    if kv.len < 0:
      quit(1)

proc modeTerm(dir: string) =
  let kv = putUnflushed(dir)
  discard listenSignalOnce(SignalTerm, proc(sig: cint) {.closure, gcsafe.} =
    gotTerm.store(true))
  ready()
  var waited = 0
  while not gotTerm.load:
    sleep(50)
    inc waited, 50
    if waited > 20000:
      quit("timed out waiting for SIGTERM", 1)
  writeFile(dir / "term.marker", "term-ok")
  kv.close()
  quit(0)

proc modeCustom(dir: string) =
  let kv = putUnflushed(dir)
  when defined(posix):
    discard listenRawSignalOnce(ops.SIGUSR1, proc(sig: cint) {.closure, gcsafe.} =
      gotUsr1.store(true))
  discard listenSignalOnce(SignalUsr2, proc(sig: cint) {.closure, gcsafe.} =
    gotUsr2.store(true))
  ready()
  var waited = 0
  while not (gotUsr1.load and gotUsr2.load):
    sleep(50)
    inc waited, 50
    if waited > 20000:
      quit("timed out waiting for custom signals", 1)
  writeFile(dir / "usr1.marker", "usr1-ok")
  writeFile(dir / "usr2.marker", "usr2-ok")
  kv.close()
  quit(0)

proc modeSegv(dir: string) =
  let kv = putUnflushed(dir)
  ready()
  sleep(500) # let the parent observe READY before we die
  # Die with a real SIGSEGV via kill(self): exercises the fatal-signal
  # handler exactly like a genuine crash (a nil `ptr` write would work too,
  # but provable-null stores get deleted as UB at -d:release).
  when defined(posix):
    discard ops.kill(ops.getpid(), ops.SIGSEGV)
    sleep(2000)
  # Unreached; keeps `kv` alive until the crash so the handler has work.
  kv.close()
  quit(1)

proc main() =
  if paramCount() < 2:
    quit("usage: worker_signal <hupkill|hupsurvive|term|custom|segv> <dir>", 1)
  let mode = paramStr(1)
  let dir = paramStr(2)
  case mode
  of "hupkill": modeHupkill(dir)
  of "hupsurvive": modeHupSurvive(dir)
  of "term": modeTerm(dir)
  of "custom": modeCustom(dir)
  of "segv": modeSegv(dir)
  else: quit("unknown mode: " & mode, 1)

main()
