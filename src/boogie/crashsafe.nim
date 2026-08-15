# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | LGPL-3.0-or-later License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Crash-safety: on fatal POSIX signals (SIGSEGV, SIGABRT, SIGINT, SIGTERM) and
## on normal process exit, every registered store's WAL is flushed so committed
## but not-yet-durable writes (async group-commit durability) are not lost.
##
## Stores register themselves here at construction via `registerStoreFlush`,
## passing a store identity pointer (the ref cast to pointer) and a `{.gcsafe.}`
## closure that flushes that store. The closure keeps the store alive for the
## process lifetime; `close` removes it. The flush is best-effort (guarded
## against exceptions): a SIGSEGV may leave memory in a state where flushing
## cannot fully succeed, and a flush that must acquire a lock can deadlock if the
## signal interrupted a thread holding it. Handlers are installed only on POSIX.
##
## Usage is automatic: every store constructor calls `registerStoreFlush` and
## `close` calls `unregisterStoreFlush`.

import std/[locks]

when defined(posix):
  import std/posix
  import std/exitprocs

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
  ## Flushes every registered store's WAL. Called from signal handlers and the
  ## exit hook. Best-effort: each store's flush is guarded against exceptions,
  ## and re-entry during a signal is ignored.
  if hooks.isNil:
    return
  flushAllStoresInternal(hooks)

proc ensureHooks() =
  if hooks.isNil:
    hooks = create(HooksState)
    hooks[] = HooksState(items: @[], ready: false, flushing: false)
    initLock(hooks.lock)
    hooks.ready = true

proc installCrashHandlers*() =
  ## Installs the POSIX signal handlers and the normal-exit flush hook.
  ## Called automatically on the first store registration; safe to call
  ## repeatedly.
  when defined(posix):
    if handlersInstalled:
      return
    handlersInstalled = true
    addExitProc(proc() {.noconv.} = flushAllStores())

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
