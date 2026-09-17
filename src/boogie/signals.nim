# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## OS signal handling for Boogie, in the style of `powpow/signal`.
##
## - `OsSignal` covers every standard POSIX signal (ordinals are the Linux
##   numbers; `signalNumber` maps to the macOS/BSD values, and platform-only
##   signals raise `SignalError` where they do not exist). `SIGKILL`/`SIGSTOP`
##   are listed for completeness but can never be caught.
## - "Custom" signals are first-class: `SIGUSR1`/`SIGUSR2` are in the enum,
##   Linux real-time signals come via `rtSignal(n)`, and any other number
##   (or a pure in-process id) works through `listenRawSignal`.
## - Delivery never runs user code inside the OS handler: a minimal
##   async-signal-safe `sigaction` handler forwards the signal number through
##   a self-pipe, and a dedicated watcher thread dispatches the subscribed
##   callbacks (`SignalCallback`). The only exception is the fatal-signal path
##   (`installFatalHandler`), which runs best-effort in handler context because
##   the process is dying and the watcher may never be scheduled.
## - On Windows, Ctrl+C is mapped to `SignalInt` via `setControlCHook`.
## - Subscribing (`listenSignal`) auto-arms OS delivery; removing the last
##   listener for a signal restores the previous disposition.
## - OS delivery needs `--threads:on` (one watcher thread); the in-process
##   relay (`listenCustom`/`emitSignal`) works without threads.

import std/[tables, locks, os]

when defined(posix):
  import std/posix

type
  SignalError* = object of CatchableError

  OsSignal* = enum
    ## Every standard POSIX signal. Ordinals are the Linux numbers; use
    ## `signalNumber` for the current platform's value.
    SignalHup    = 1   ## hangup / daemon reload
    SignalInt    = 2   ## Ctrl+C
    SignalQuit   = 3   ## Ctrl+\
    SignalIll    = 4   ## illegal instruction (fatal)
    SignalTrap   = 5   ## trace/breakpoint trap (fatal)
    SignalAbrt   = 6   ## abort (fatal)
    SignalBus    = 7   ## bus error, Linux (fatal; macOS uses 10, see mapping)
    SignalFpe    = 8   ## floating-point exception (fatal)
    SignalKill   = 9   ## kill (uncatchable)
    SignalUsr1   = 10  ## user-defined 1, Linux (macOS uses 30, see mapping)
    SignalSegv   = 11  ## segmentation violation (fatal)
    SignalUsr2   = 12  ## user-defined 2, Linux (macOS uses 31, see mapping)
    SignalPipe   = 13  ## write to closed pipe/socket
    SignalAlarm  = 14  ## alarm clock
    SignalTerm   = 15  ## termination request
    SignalStkflt = 16  ## stack fault, Linux only
    SignalChild  = 17  ## child stopped/exited, Linux (macOS uses 20)
    SignalCont   = 18  ## continue, Linux (macOS uses 19)
    SignalStop   = 19  ## stop, Linux (uncatchable; macOS uses 17)
    SignalTstp   = 20  ## terminal stop, Linux (macOS uses 18)
    SignalTtin   = 21  ## background read from tty
    SignalTtou   = 22  ## background write to tty
    SignalUrgent = 23  ## urgent socket data, Linux (macOS uses 16)
    SignalXcpu   = 24  ## CPU time limit exceeded
    SignalXfsz   = 25  ## file size limit exceeded
    SignalVtalrm = 26  ## virtual alarm clock
    SignalProf   = 27  ## profiling timer
    SignalWinch  = 28  ## window size change
    SignalIo     = 29  ## async I/O ready, Linux (macOS uses 23)
    SignalPwr    = 30  ## power failure, Linux only
    SignalSys    = 31  ## bad system call, Linux (macOS uses 12; fatal)
    SignalEmt    = 65  ## emulator trap, macOS/BSD only (fatal)
    SignalInfo   = 66  ## status request (Ctrl+T), macOS/BSD only

  SignalCallback* = proc(sig: cint) {.closure, gcsafe.}
    ## Callbacks run on the watcher thread (or the Ctrl+C thread on Windows),
    ## never inside the OS signal handler — except fatal handlers installed
    ## via `installFatalHandler`, which run in handler context and must only
    ## use async-signal-safe operations.

proc signalNumber*(s: OsSignal): cint {.gcsafe.} =
  ## Numeric signal value on the current platform. Raises `SignalError` for
  ## platform-only signals used on the wrong platform.
  when defined(macosx) or defined(freebsd) or defined(netbsd) or
       defined(openbsd) or defined(dragonfly):
    case s
    of SignalBus: 10
    of SignalUsr1: 30
    of SignalUsr2: 31
    of SignalSys: 12
    of SignalUrgent: 16
    of SignalStop: 17
    of SignalTstp: 18
    of SignalCont: 19
    of SignalChild: 20
    of SignalIo: 23
    of SignalEmt: 7
    of SignalInfo: 29
    of SignalStkflt, SignalPwr:
      raise newException(SignalError, $s & " does not exist on this platform")
    else: s.ord.cint
  elif defined(linux):
    case s
    of SignalEmt, SignalInfo:
      raise newException(SignalError, $s & " does not exist on this platform")
    else: s.ord.cint
  elif defined(windows):
    case s
    of SignalInt, SignalTerm, SignalKill: s.ord.cint
    else:
      raise newException(SignalError, $s & " is not supported on Windows")
  else:
    s.ord.cint

when defined(linux):
  const SigRtMin* = 34.cint
    ## First Linux real-time signal number (SIGRTMIN as reported by glibc).
  const SigRtMax* = 64.cint
    ## Last Linux real-time signal number.

  proc rtSignal*(n: int): cint =
    ## The n-th Linux real-time signal (`rtSignal(0)` == SIGRTMIN).
    if n < 0 or SigRtMin + cint(n) > SigRtMax:
      raise newException(SignalError, "real-time signal index out of range")
    SigRtMin + cint(n)

# ---------------------------------------------------------------- relay ---

type
  SignalListener = object
    cb: SignalCallback
    once: bool
    id: int

  ListenerHandle* = ref object
    ## Returned by the `listen*` procs; `unlisten` removes the subscription.
    signo: int
    id: int
    alive: bool

  SigRegistry = ref object
    mu: Lock
    listeners: Table[int, seq[SignalListener]]
    nextId: int
    armed: Table[int, bool]
    when defined(posix):
      oldHandlers: Table[int, Sigaction]
    watcherStarted: bool
    when compileOption("threads"):
      watcher: Thread[SigRegistry]

var gRegistry: SigRegistry

proc registry(): SigRegistry =
  if gRegistry.isNil:
    gRegistry = SigRegistry(
      listeners: initTable[int, seq[SignalListener]](),
      armed: initTable[int, bool](),
      nextId: 1,
    )
    initLock(gRegistry.mu)
  gRegistry

# ------------------------------------------------- platform delivery ---

proc dropOnceListeners(r: SigRegistry, signo: int, ids: openArray[int]) =
  ## Removes `once` subscriptions by id. Idempotent with a concurrent
  ## `unlisten` (a missing id is a no-op). Shared by the posix and Windows
  ## dispatch loops.
  acquire(r.mu)
  if r.listeners.hasKey(signo):
    for id in ids:
      var i = 0
      while i < r.listeners[signo].len:
        if r.listeners[signo][i].id == id:
          r.listeners[signo].delete(i)
          break
        inc i
  release(r.mu)

when defined(posix):
  var SA_RESTART {.importc: "SA_RESTART", header: "<signal.h>".}: cint
  var gPipeRd: cint = -1
  var gPipeWr: cint = -1

  proc signalForwarder(sig: cint) {.noconv.} =
    ## Async-signal-safe: forwards the signal number through the self-pipe.
    if gPipeWr >= 0:
      let b = byte(sig and 0xFF)
      discard write(gPipeWr, addr b, 1)

  proc dispatch(r: SigRegistry, signo: int) =
    var cbs: seq[SignalCallback] = @[]
    var onceIds: seq[int] = @[]
    acquire(r.mu)
    if r.listeners.hasKey(signo):
      # Snapshot while holding the lock; invoke outside. Once-listeners are
      # removed AFTER the callback loop (not before) so that competing
      # callbacks within the same dispatch still observe each other — e.g.
      # crashsafe's terminate check must see a host `once` listener that is
      # about to handle the signal.
      for li in r.listeners[signo]:
        cbs.add(li.cb)
        if li.once:
          onceIds.add(li.id)
    release(r.mu)
    for cb in cbs:
      try:
        cb(cint(signo))
      except CatchableError, Defect:
        discard
    if onceIds.len > 0:
      r.dropOnceListeners(signo, onceIds)

  proc watcherLoop(r: SigRegistry) {.thread.} =
    var buf: array[32, byte]
    while true:
      # Blocking read: the thread sleeps here until a signal byte arrives
      # (zero CPU while idle). Never hot-loop on failure: an O_NONBLOCK fd
      # plus `continue` on EAGAIN pins a core at 100%.
      let n = read(gPipeRd, addr buf[0], buf.len)
      if n < 0:
        # EINTR (blocking read interrupted): retry, the next read blocks again.
        continue
      if n == 0:
        # EOF: every write end closed. The process holds its own write end
        # open for its lifetime so this should not happen; sleep defensively
        # instead of spinning in case it ever does.
        sleep(50)
        continue
      for i in 0 ..< n:
        r.dispatch(int(buf[i]))

  proc ensureWatcher(r: SigRegistry) =
    when not compileOption("threads"):
      raise newException(SignalError,
        "OS signal delivery requires --threads:on")
    else:
      acquire(r.mu)
      let started = r.watcherStarted
      if not started:
        r.watcherStarted = true
      release(r.mu)
      if not started:
        createThread(r.watcher, watcherLoop, r)

  proc armOsSignal(r: SigRegistry, signo: int) =
    ## Installs the forwarder for `signo` (idempotent). Raises `SignalError`
    ## for numbers the OS refuses (e.g. SIGKILL/SIGSTOP).
    if signo < 1 or signo > 64:
      raise newException(SignalError,
        "signal number out of range: " & $signo)
    acquire(r.mu)
    let armed = r.armed.getOrDefault(signo, false)
    release(r.mu)
    if armed:
      return
    r.ensureWatcher()
    # Self-pipe, created once for the process.
    if gPipeWr < 0:
      var fds: array[2, cint]
      if pipe(fds) != 0:
        raise newException(SignalError, "pipe() failed for signal delivery")
      gPipeRd = fds[0]
      gPipeWr = fds[1]
      # The read end stays BLOCKING so the watcher thread sleeps in read()
      # instead of spinning. The write end stays non-blocking: it is used
      # inside the async-signal-safe forwarder, which must never block.
      var fl = fcntl(fds[1], F_GETFL, 0)
      if fl >= 0:
        discard fcntl(fds[1], F_SETFL, fl or O_NONBLOCK)
      # Do not leak the pipe into spawned children.
      discard fcntl(fds[0], F_SETFD, FD_CLOEXEC)
      discard fcntl(fds[1], F_SETFD, FD_CLOEXEC)
      # Both ends stay open for process lifetime.
    var act: Sigaction
    act.sa_handler = signalForwarder
    discard sigemptyset(act.sa_mask)
    act.sa_flags = SA_RESTART
    var old: Sigaction
    if sigaction(cint(signo), act, old) != 0:
      raise newException(SignalError,
        "cannot watch signal " & $signo & " (uncatchable?)")
    acquire(r.mu)
    r.armed[signo] = true
    r.oldHandlers[signo] = old
    release(r.mu)

  proc disarmOsSignal(r: SigRegistry, signo: int) =
    ## Restores the disposition that predated our watch when nothing listens
    ## anymore. Only signals we armed are ever touched.
    acquire(r.mu)
    let armed = r.armed.getOrDefault(signo, false)
    let busy = r.listeners.hasKey(signo) and r.listeners[signo].len > 0
    let hasOld = r.oldHandlers.hasKey(signo)
    var old: Sigaction
    if hasOld:
      old = r.oldHandlers[signo]
    release(r.mu)
    if armed and not busy:
      if hasOld:
        var o = old
        discard sigaction(cint(signo), o, nil)
      acquire(r.mu)
      r.armed.del(signo)
      r.oldHandlers.del(signo)
      release(r.mu)

elif defined(windows):
  proc dispatchWin(signo: int) =
    let r = registry()
    var cbs: seq[SignalCallback] = @[]
    var onceIds: seq[int] = @[]
    acquire(r.mu)
    if r.listeners.hasKey(signo):
      for li in r.listeners[signo]:
        cbs.add(li.cb)
        if li.once:
          onceIds.add(li.id)
    release(r.mu)
    for cb in cbs:
      try:
        cb(cint(signo))
      except CatchableError, Defect:
        discard
    if onceIds.len > 0:
      r.dropOnceListeners(signo, onceIds)

  proc ctrlHook() {.noconv.} =
    dispatchWin(2) # synthetic SIGINT

# ------------------------------------------------------------ API ---

proc listenRawSignal*(signo: int, cb: SignalCallback): ListenerHandle =
  ## Subscribes `cb` to a numeric signal id. Values 1..64 additionally arm
  ## real OS delivery (Linux real-time signals, `SIGUSR1/2`, platform-specific
  ## numbers — "custom" signals); any other value is a pure in-process channel
  ## delivered via `emitSignal`.
  let r = registry()
  acquire(r.mu)
  let id = r.nextId
  inc r.nextId
  r.listeners.mgetOrPut(signo, @[]).add(
    SignalListener(cb: cb, once: false, id: id))
  release(r.mu)
  when defined(posix):
    if signo >= 1 and signo <= 64:
      r.armOsSignal(signo)
  elif defined(windows):
    if signo == 2:
      setControlCHook(ctrlHook)
  ListenerHandle(signo: signo, id: id, alive: true)

proc listenRawSignalOnce*(signo: int, cb: SignalCallback): ListenerHandle =
  ## Like `listenRawSignal` but the callback fires at most once (per
  ## dispatch; a callback that synchronously re-emits its own signal from
  ## inside itself is unsupported and may refire).
  let r = registry()
  acquire(r.mu)
  let id = r.nextId
  inc r.nextId
  r.listeners.mgetOrPut(signo, @[]).add(
    SignalListener(cb: cb, once: true, id: id))
  release(r.mu)
  when defined(posix):
    if signo >= 1 and signo <= 64:
      r.armOsSignal(signo)
  elif defined(windows):
    if signo == 2:
      setControlCHook(ctrlHook)
  ListenerHandle(signo: signo, id: id, alive: true)

proc listenSignal*(s: OsSignal, cb: SignalCallback): ListenerHandle =
  ## Subscribes `cb` to the OS signal `s` (auto-arms delivery).
  listenRawSignal(int(s.signalNumber), cb)

proc listenSignalOnce*(s: OsSignal, cb: SignalCallback): ListenerHandle =
  ## Like `listenSignal` but the callback fires at most once.
  listenRawSignalOnce(int(s.signalNumber), cb)

proc listenCustom*(id: int, cb: SignalCallback): ListenerHandle =
  ## Subscribes `cb` to a pure in-process channel `id` (no OS signal is
  ## armed; deliver with `emitSignal`). Use ids >= 1024 to avoid colliding
  ## with real signal numbers.
  listenRawSignal(id, cb)

proc listenCustomOnce*(id: int, cb: SignalCallback): ListenerHandle =
  ## Like `listenCustom` but the callback fires at most once.
  listenRawSignalOnce(id, cb)

proc unlisten*(h: ListenerHandle) =
  ## Removes the subscription. When the last listener for an armed OS signal
  ## goes away, the previous signal disposition is restored.
  if h.isNil or not h.alive:
    return
  h.alive = false
  let r = registry()
  acquire(r.mu)
  if r.listeners.hasKey(h.signo):
    var i = 0
    while i < r.listeners[h.signo].len:
      if r.listeners[h.signo][i].id == h.id:
        r.listeners[h.signo].delete(i)
        break
      inc i
  release(r.mu)
  when defined(posix):
    if h.signo >= 1 and h.signo <= 64:
      r.disarmOsSignal(h.signo)

proc emitSignal*(signo: int) =
  ## Synchronously dispatches `signo` to its subscribers on the calling
  ## thread (in-process only; does not raise a real OS signal).
  let r = registry()
  when defined(posix):
    r.dispatch(signo)
  elif defined(windows):
    dispatchWin(signo)
  else:
    var cbs: seq[SignalCallback] = @[]
    acquire(r.mu)
    if r.listeners.hasKey(signo):
      var keep: seq[SignalListener] = @[]
      for li in r.listeners[signo]:
        cbs.add(li.cb)
        if not li.once:
          keep.add(li)
      r.listeners[signo] = keep
    release(r.mu)
    for cb in cbs:
      try:
        cb(cint(signo))
      except CatchableError, Defect:
        discard

proc watchOsSignals*(signals: openArray[OsSignal]) =
  ## Arms OS delivery for `signals` without subscribing (the default action
  ## stays suppressed while armed; pair with `listenSignal` to react).
  ## Prefer `listenSignal`, which arms automatically.
  when defined(posix):
    let r = registry()
    for s in signals:
      r.armOsSignal(int(s.signalNumber))
  elif defined(windows):
    for s in signals:
      if int(s.signalNumber) == 2:
        setControlCHook(ctrlHook)

proc watchedSignals*(): seq[int] =
  ## Currently armed OS signal numbers.
  let r = registry()
  acquire(r.mu)
  for k in r.armed.keys:
    result.add(k)
  release(r.mu)

proc countSignalListeners*(signo: int): int =
  ## Number of live subscriptions for `signo` (OS listeners and pure
  ## in-process channels alike). Used by `boogie/crashsafe` to decide whether
  ## its internal termination-signal handler owns the signal outright (no
  ## host-registered listener competes) or must defer to the host.
  let r = registry()
  acquire(r.mu)
  if r.listeners.hasKey(signo):
    result = r.listeners[signo].len
  release(r.mu)

proc countSignalListeners*(s: OsSignal): int =
  ## Enum overload of `countSignalListeners`. Returns 0 for platform-only
  ## signals used on the wrong platform.
  try:
    countSignalListeners(int(s.signalNumber))
  except SignalError:
    0

# ------------------------------------------------------ fatal path ---

when defined(posix):
  var gFatalCb: SignalCallback
  var gFatalOld: array[65, Sigaction]
  var gFatalHasOld: array[65, bool]

  proc fatalForwarder(sig: cint) {.noconv.} =
    ## Handler context: only async-signal-safe operations allowed. Runs the
    ## registered best-effort callback, restores the previous disposition,
    ## then re-raises so the process still dies with the default action
    ## (and a core dump, when enabled).
    if gFatalCb != nil:
      try:
        gFatalCb(sig)
      except CatchableError, Defect:
        discard
    if sig >= 0 and sig <= 64 and gFatalHasOld[sig]:
      var o = gFatalOld[sig]
      discard sigaction(sig, o, nil)
    else:
      var def: Sigaction
      discard sigemptyset(def.sa_mask)
      discard sigaction(sig, def, nil)
    discard kill(getpid(), sig)

  proc installFatalHandler*(signals: openArray[OsSignal],
                            cb: SignalCallback) =
    ## Installs `cb` for fatal signals (SIGSEGV/SIGABRT/...). `cb` runs
    ## INSIDE the signal handler: keep it to async-signal-safe work
    ## (plain `write(2)` syscalls, no allocation, no locks — or `tryAcquire`
    ## at most). After `cb` returns the previous disposition is restored and
    ## the signal is re-raised.
    for s in signals:
      let signo = int(s.signalNumber)
      if signo < 1 or signo > 64:
        raise newException(SignalError,
          "signal number out of range: " & $signo)
      var act: Sigaction
      act.sa_handler = fatalForwarder
      discard sigemptyset(act.sa_mask)
      act.sa_flags = 0 # no SA_RESTART: a fatal handler must not resume
      var old: Sigaction
      if sigaction(cint(signo), act, old) != 0:
        raise newException(SignalError,
          "cannot watch signal " & $signo)
      gFatalHasOld[signo] = true
      gFatalOld[signo] = old
    gFatalCb = cb
