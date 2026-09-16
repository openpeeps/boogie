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

proc installCrashHandlers*() =
  ## Installs the normal-exit flush hook plus OS signal handlers, and is safe
  ## to call repeatedly. Termination signals (INT/TERM/HUP/QUIT) flush via the
  ## watcher thread; fatal signals (SEGV/ABRT/BUS/ILL/FPE) flush best-effort
  ## in handler context and re-raise. Without `--threads:on` only the exit
  ## hook is installed (signal delivery needs the watcher thread).
  if handlersInstalled:
    return
  handlersInstalled = true
  when defined(posix):
    addExitProc(proc() {.noconv.} = flushAllStores())
    when compileOption("threads"):
      for s in [SignalInt, SignalTerm, SignalHup, SignalQuit]:
        discard listenSignal(s, proc(sig: cint) {.closure, gcsafe.} =
          flushAllStores())
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
