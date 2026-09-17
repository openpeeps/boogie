# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Crash-safety: every registered store's WAL is flushed so committed but
## not-yet-durable writes (async group-commit durability) are not lost on:
## - normal process exit (via an exit hook),
## - termination signals (SIGINT/SIGTERM/SIGHUP/SIGQUIT), delivered through
##   `boogie/signals` to a watcher thread that flushes outside signal context,
## - fatal signals (SIGSEGV/SIGABRT/SIGBUS/SIGILL/SIGFPE), via a best-effort
##   in-handler flush that uses no allocator and never blocks on a busy lock,
##   then re-raises with the default disposition (core dumps still work).
##
## Lifecycle signals (SIGHUP/SIGTERM/SIGQUIT by default, see
## `setTerminateSignals`) are flush-then-terminate: after flushing, the
## process dies with the signal's default disposition — unless the host
## registered its own listener for that signal through `boogie/signals`, in
## which case boogie defers to the host and only flushes. A library must never
## silently convert "terminate" into "keep running": swallowing SIGHUP hangs
## terminal tab-close for shells, and swallowing SIGTERM breaks `kill`,
## `docker stop`, launchd and systemd for every embedder. SIGINT always stays
## flush-and-continue (interactive Ctrl+C must not kill a REPL).
##
## Stores register themselves here at construction via `registerStoreFlush`,
## passing a store identity pointer (the ref cast to pointer) and a `{.gcsafe.}`
## closure that flushes that store. The closure keeps the store alive for the
## process lifetime; `close` removes it.
##
## Usage is automatic: every store constructor calls `registerStoreFlush` and
## `close` calls `unregisterStoreFlush`.

import std/[locks]
import ./signals
export signals

when defined(posix):
  import std/[posix, atomics, exitprocs]

type
  ShutdownHook = object
    id: pointer
      ## store identity (the store ref cast to pointer) for unregistering
    flush: proc() {.gcsafe.}
      ## flushes the store's WAL. The closure captures the store ref, which also
      ## keeps the store alive for the process lifetime so the raw `id` never
      ## dangles. Closures ARE callable from exit procs/signal handlers as long
      ## as they are invoked by index (the `for .. in seq` iterator is not).

  HooksState = ref object
    ## The shutdown-hook registry. Kept in a `ref` object (not a module-level
    ## seq) so the exit hook can capture it and keep it alive: Nim destroys
    ## module-level containers before `addExitProc` callbacks run, which would
    ## otherwise leave the exit flush reading freed memory.
    items: seq[ShutdownHook]
    lock: Lock
    ready: bool
    flushing: bool

var
  hooks: ptr HooksState
    ## Raw heap allocation (never `dealloc`'d) so the exit flush hook can safely
    ## read it after module teardown destroys module-level variables. The state
    ## is process-lifetime anyway, so this is an intentional, tiny leak.
  handlersInstalled = false
  termHandles: seq[ListenerHandle]
    ## Our own internal termination-signal subscriptions (one per signal in
    ## `installCrashHandlers`). Never unlistened; kept so the sole-listener
    ## check can tell our handle apart from host-registered ones.
  termMu: Lock
  termSignals: set[OsSignal] = {SignalHup, SignalTerm, SignalQuit}
    ## Lifecycle signals that kill the process after flushing. Consulted at
    ## dispatch time (not install time) so `setTerminateSignals` applies
    ## regardless of call order. SIGINT is deliberately absent: interactive
    ## interrupts must flush and continue, never kill.

initLock(termMu)

proc flushAllStoresInternal(h: ptr HooksState) =
  if not h.ready or h.flushing:
    return
  h.flushing = true
  acquire(h.lock)
  let n = h.items.len
  release(h.lock)
  # Index-based iteration: the `for .. in seq` iterator is not reliable in the
  # atexit context (it crashes), while direct indexing works.
  var i = 0
  while i < n:
    let hk = h.items[i]
    try:
      hk.flush()
    except CatchableError:
      discard
    inc i
  h.flushing = false

proc flushAllStores*() =
  ## Flushes every registered store's WAL. Called from the exit hook and the
  ## termination-signal callbacks. Best-effort: each store's flush is guarded
  ## against exceptions, and re-entry during a signal is ignored.
  if hooks.isNil:
    return
  flushAllStoresInternal(hooks)

when defined(posix):
  var fatalActive: Atomic[bool]

  proc flushAllStoresNoBlock() =
    ## Best-effort flush for fatal-signal handler context: no allocator, and
    ## the registry lock is taken with `tryAcquire` (never waited on). The
    ## per-store flushes run normally — for plain stores those are just
    ## syscalls; concurrent-mode stores may take their WAL lock, which is the
    ## documented residual risk of flushing while dying. Afterwards the fatal
    ## handler restores the default disposition and re-raises.
    if hooks.isNil or fatalActive.exchange(true):
      return
    if not tryAcquire(hooks.lock):
      return
    try:
      # Index-based iteration (the `for .. in seq` iterator is not reliable
      # in signal/atexit context); the lock is held throughout so a
      # concurrent unregister cannot shift the seq under us.
      var i = 0
      while i < hooks.items.len:
        try:
          hooks.items[i].flush()
        except CatchableError, Defect:
          discard
        inc i
    finally:
      release(hooks.lock)

proc ensureHooks() =
  if hooks.isNil:
    hooks = create(HooksState)
    hooks[] = HooksState(items: @[], ready: false, flushing: false)
    initLock(hooks.lock)
    hooks.ready = true

proc setTerminateSignals*(s: set[OsSignal]) =
  ## Overrides which lifecycle signals kill the process after flushing (see
  ## the module docs). Applies process-wide, effective immediately, and
  ## independent of call order with store opens. Pass `{}` to restore pure
  ## flush-and-continue for every signal (e.g. daemons with their own SIGHUP
  ## reload handling — arm yours after the first store open and call
  ## `flushAllStores()` in it).
  when defined(posix):
    acquire(termMu)
    termSignals = s
    release(termMu)
  else:
    discard s

when defined(posix):
  var termActive: Atomic[bool]

  proc shouldTerminateOn(signo: int): bool {.gcsafe.} =
    ## True when `signo` is a lifecycle signal per the current policy AND no
    ## host-registered listener competes for it. Our own internal handle does
    ## not count: with only boogie listening, the host expressed no intent
    ## and standard Unix semantics (terminate) apply. A host `listenSignal`
    ## for the same signal suppresses termination — the host owns the outcome
    ## and boogie only flushes.
    var wanted = false
    acquire(termMu)
    for s in termSignals:
      try:
        if int(s.signalNumber) == signo:
          wanted = true
          break
      except SignalError:
        discard # platform-only signal in the set; not deliverable here
    release(termMu)
    if not wanted:
      return false
    # Registry access touches GC'd globals; the surrounding proc is gcsafe
    # (watcher thread) and this is the single call site. The read itself is
    # thread-safe: the registry mutex serializes it.
    {.cast(gcsafe).}:
      result = countSignalListeners(signo) <= 1

  proc terminateAfterFlush(signo: int) {.gcsafe.} =
    ## Restores the default disposition for `signo` and re-raises it at ourselves
    ## so the process dies with standard signal semantics (exit-by-signal, core
    ## dump for SIGQUIT). Runs on the watcher thread — never in handler context —
    ## so plain syscalls are safe. Restoring SIG_DFL first is mandatory: without
    ## it the forwarder would re-catch our own re-raise and loop forever.
    if termActive.exchange(true):
      return
    var def: Sigaction
    discard sigemptyset(def.sa_mask)
    def.sa_handler = SIG_DFL
    def.sa_flags = 0
    discard sigaction(cint(signo), def, nil)
    discard kill(getpid(), cint(signo))

proc crashSignalCb(sig: cint) {.gcsafe.} =
  ## Watcher-thread callback for termination signals: flush first, then die
  ## when the lifecycle policy says the signal is ours alone. A top-level
  ## (environment-free) proc converts implicitly to the `SignalCallback`
  ## closure type.
  flushAllStores()
  when defined(posix):
    if shouldTerminateOn(int(sig)):
      terminateAfterFlush(int(sig))

proc installCrashHandlers*() =
  ## Installs the normal-exit flush hook plus OS signal handlers, and is safe
  ## to call repeatedly. Termination signals (INT/TERM/HUP/QUIT) flush via the
  ## watcher thread; HUP/TERM/QUIT then terminate the process unless the host
  ## registered its own listener (see the module docs and
  ## `setTerminateSignals`). Fatal signals (SEGV/ABRT/BUS/ILL/FPE) flush
  ## best-effort in handler context and re-raise. Without `--threads:on` only
  ## the exit hook is installed (signal delivery needs the watcher thread).
  if handlersInstalled:
    return
  handlersInstalled = true
  when defined(posix):
    addExitProc(proc() {.noconv.} = flushAllStores())
    when compileOption("threads"):
      for s in [SignalInt, SignalTerm, SignalHup, SignalQuit]:
        termHandles.add(listenSignal(s, crashSignalCb))
      installFatalHandler([SignalSegv, SignalAbrt, SignalBus, SignalIll,
                           SignalFpe],
        proc(sig: cint) {.closure, gcsafe.} =
          flushAllStoresNoBlock())
  else:
    discard

proc registerStoreFlush*(storeId: pointer, flush: proc() {.gcsafe.}) =
  ## Registers a store's WAL flush for crash/exit durability.
  ensureHooks()
  installCrashHandlers()
  acquire(hooks.lock)
  hooks.items.add(ShutdownHook(id: storeId, flush: flush))
  release(hooks.lock)

proc unregisterStoreFlush*(storeId: pointer) =
  ## Removes a store from the flush registry (e.g. on `close`).
  if hooks.isNil:
    return
  acquire(hooks.lock)
  var i = 0
  while i < hooks.items.len:
    if hooks.items[i].id == storeId:
      hooks.items.delete(i)
    else:
      inc i
  release(hooks.lock)
