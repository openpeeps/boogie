# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

## Concurrency substrate for Boogie stores, built on a Disruptor-style ring
## buffer (threading-first; ARC required for concurrent stores).
##
## When a store is constructed with `enableConcurrency = true` it owns a
## `ConcurrentState[T]`:
## - a `WriteRing[Queued[T]]`: a pre-allocated, power-of-two ring of task slots
##   with sequence barriers (claim -> write -> ordered publish). Producers
##   claim via an atomic, write the task with Nim move semantics, and publish
##   the cursor contiguously via CAS - so a published sequence is the single
##   authoritative signal and lost wakeups are structurally impossible.
## - one consumer thread that drains the ring in order: it applies each task
##   under the target table's write lock, appends to the store WAL (group
##   commit), and bumps that table's `appliedSeq` for synchronous visibility.
## - a `metaMu` RWLock guarding the table registry and a `walLock` serializing
##   WAL appends.
##
## Every table/collection gets a `TableSlot[T]` (embedded as a field): `mu`
## (RWLock) gives unlimited concurrent readers; the appliedSeq generation
## barrier gives synchronous visibility to writers. The slot's `owner` is an
## opaque pointer to the owning table, used only while `dropped` is false.
##
## Write tasks carry value-type data only (no shared refs); the ring moves them
## with correct refcounting (unlike a channel's raw memcpy).

import std/[locks, atomics, cpuinfo]
import ./wal

# -- RwLock (inlined, avoids the threading package dependency) ---------------

type
  RwLock* = object
    ## Readers-writer lock. Multiple readers can acquire the lock at the same
    ## time, but only one writer can acquire the lock at a time.
    c: Cond
    L: Lock
    activeReaders, waitingWriters: int
    isWriterActive: bool

when defined(nimAllowNonVarDestructor):
  proc `=destroy`*(rw: RwLock) {.nimcall.} =
    let x = addr(rw)
    deinitCond(x.c)
    deinitLock(x.L)
else:
  proc `=destroy`*(rw: var RwLock) {.nimcall.} =
    deinitCond(rw.c)
    deinitLock(rw.L)

proc `=sink`*(dest: var RwLock; source: RwLock) {.error.}
proc `=copy`*(dest: var RwLock; source: RwLock) {.error.}

proc initRwLock*(rw: var RwLock) {.inline.} =
  ## In-place initializer for a zero-initialized `RwLock` field (embeddable).
  initCond(rw.c)
  initLock(rw.L)

proc createRwLock*(): RwLock =
  ## Creates a new `RwLock`.
  result = default(RwLock)
  initCond(result.c)
  initLock(result.L)

template readWith*(a: var RwLock, body: untyped) =
  beginRead(a)
  try:
    body
  finally:
    endRead(a)

template writeWith*(a: var RwLock, body: untyped) =
  beginWrite(a)
  try:
    body
  finally:
    endWrite(a)

proc beginRead*(rw: var RwLock) =
  acquire(rw.L)
  while rw.waitingWriters > 0 or rw.isWriterActive:
    wait(rw.c, rw.L)
  inc rw.activeReaders
  release(rw.L)

proc beginWrite*(rw: var RwLock) =
  acquire(rw.L)
  inc rw.waitingWriters
  while rw.activeReaders > 0 or rw.isWriterActive:
    wait(rw.c, rw.L)
  dec rw.waitingWriters
  rw.isWriterActive = true
  release(rw.L)

proc endRead*(rw: var RwLock) {.inline.} =
  acquire(rw.L)
  dec rw.activeReaders
  broadcast(rw.c)
  release(rw.L)

proc endWrite*(rw: var RwLock) {.inline.} =
  acquire(rw.L)
  rw.isWriterActive = false
  broadcast(rw.c)
  release(rw.L)

type
  Queued*[T] = object
    ## A submitted write op: the target table's slot, its per-table order, and
    ## the op payload.
    slot: ptr TableSlot[T]
    order: uint64
    op: T

  WriteRing*[T] = ref object
    ## Disruptor-style ring buffer with sequence barriers. Producers claim a
    ## slot via `nextClaim`, write the task, and publish the `cursor`
    ## contiguously via CAS (a plain store could regress it). The consumer
    ## reads the cursor and drains in order.
    slots: seq[T]
    mask: int
    available: seq[Atomic[int64]]
    cursor: Atomic[int64]
    nextClaim: Atomic[int64]
    gating: Atomic[int64]
    lock: Lock
    cond: Cond
    stopped: Atomic[bool]

  TableSlot*[T] = ref object
    ## Per-table/collection concurrency state.
    mu*: RwLock
    nextSeq: Atomic[uint64]
    appliedSeq: uint64
    appliedLock: Lock
    appliedCond: Cond
    owner*: pointer
    dropped: bool

  ConcurrentState*[T] = ref object
    ## Store-level concurrency state. `nil` when the store is created without
    ## `enableConcurrency`.
    metaMu: RwLock
    walLock: Lock
    walPending: Atomic[int]
    ring: WriteRing[Queued[T]]
    consumer: Thread[ConcurrentState[T]]
    stopped: Atomic[bool]
    ctx: pointer
    apply: proc(ctx: pointer, slot: TableSlot[T], op: T) {.gcsafe.}

#
# WriteRing
#
proc newWriteRing*[T](capacityPower = 10): WriteRing[T] =
  ## Creates a ring with `2^capacityPower` pre-allocated slots (power of two).
  let cap = 1 shl capacityPower
  result = WriteRing[T](
    slots: newSeq[T](cap),
    available: newSeq[Atomic[int64]](cap),
    mask: cap - 1,
  )
  result.cursor.store(-1, moRelaxed)
  result.nextClaim.store(-1, moRelaxed)
  result.gating.store(-1, moRelaxed)
  result.stopped.store(false, moRelaxed)
  for a in result.available.mitems:
    a.store(-1, moRelaxed)
  initLock(result.lock)
  initCond(result.cond)

proc isAvailable[T](r: WriteRing[T], seq: int64): bool {.inline, gcsafe.} =
  r.available[seq and r.mask].load(moAcquire) == seq

proc setAvailable[T](r: WriteRing[T], seq: int64) {.inline, gcsafe.} =
  r.available[seq and r.mask].store(seq, moRelease)

proc tryPublish[T](r: WriteRing[T], seq: int64) {.gcsafe.} =
  ## Marks `seq` available and advances the cursor contiguously via CAS only.
  ## A plain store here could regress the cursor if another producer published
  ## a higher sequence during the availability scan (this was the cause of both
  ## lost-wakeup hangs and heap corruption in the earlier design).
  r.setAvailable(seq)
  while true:
    var c = r.cursor.load(moAcquire)
    if not r.isAvailable(c + 1):
      break
    var expected = c
    if r.cursor.compareExchange(expected, c + 1):
      continue
    # another producer won the CAS; loop to re-read the cursor

proc publish[T](r: WriteRing[T], seq: int64) {.gcsafe.} =
  r.tryPublish(seq)
  acquire r.lock
  broadcast r.cond
  release r.lock

proc submit*[T](r: WriteRing[T], task: sink T): int64 {.gcsafe.} =
  ## Claims a slot, waits for space (backpressure), writes the task, and
  ## publishes it. Returns the sequence number.
  let seq = r.nextClaim.fetchAdd(1, moAcquireRelease) + 1
  acquire r.lock
  while seq - r.gating.load(moAcquire) >= int64(r.slots.len):
    wait r.cond, r.lock
  release r.lock
  r.slots[seq and r.mask] = task
  r.publish(seq)
  seq

proc consume*[T](r: WriteRing[T], onTask: proc(x: T) {.closure, gcsafe.}) {.gcsafe.} =
  ## Consumer loop: waits for the cursor to advance, drains published slots in
  ## order, and advances the gating sequence (waking producers blocked on a full
  ## ring). Runs until `stop` is called.
  while true:
    let c = r.cursor.load(moAcquire)
    if c > r.gating.load(moRelaxed):
      var ns = r.gating.load(moRelaxed) + 1
      while ns <= c:
        var t = move(r.slots[ns and r.mask])
        onTask(t)
        inc ns
      r.gating.store(ns - 1, moRelease)
      acquire r.lock
      broadcast r.cond
      release r.lock
    else:
      acquire r.lock
      while r.cursor.load(moAcquire) <= r.gating.load(moRelaxed) and
          not r.stopped.load(moRelaxed):
        wait r.cond, r.lock
      release r.lock
      if r.stopped.load(moRelaxed):
        break

proc stop*[T](r: WriteRing[T]) =
  if r.stopped.exchange(true, moAcquireRelease):
    return
  acquire r.lock
  broadcast r.cond
  release r.lock

#
# TableSlot
#
proc newTableSlot*[T](owner: pointer): TableSlot[T] =
  ## Creates an unarmed per-table slot. `owner` is an opaque pointer to the
  ## owning table/collection, dereferenced by the store's `apply` only while the
  ## slot is not dropped.
  result = TableSlot[T](owner: owner)
  initRwLock(result.mu)
  result.nextSeq.store(0, moRelaxed)
  initLock(result.appliedLock)
  initCond(result.appliedCond)

proc submit*[T](cc: ConcurrentState[T], slot: TableSlot[T], op: sink T): uint64 =
  ## Enqueues `op` for the given table and returns its per-table sequence
  ## number. The caller should then `waitApplied` for synchronous visibility.
  let order = slot.nextSeq.fetchAdd(1, moAcquireRelease) + 1
  discard cc.ring.submit(Queued[T](slot: cast[ptr TableSlot[T]](slot),
                                  order: order, op: op))
  order

proc waitApplied*[T](slot: TableSlot[T], order: uint64) =
  ## Blocks until the op with the given per-table sequence has been applied to
  ## the in-memory state (synchronous visibility). Durability (WAL flush) is
  ## async.
  acquire slot.appliedLock
  while slot.appliedSeq < order:
    wait slot.appliedCond, slot.appliedLock
  release slot.appliedLock

#
# ConcurrentState
#
proc consumerLoop[T](cc: ConcurrentState[T]) {.thread.} =
  cc.ring.consume(proc(q: Queued[T]) {.closure, gcsafe.} =
    let slot = cast[TableSlot[T]](q.slot)
    if not slot.dropped:
      beginWrite(slot.mu)
      try:
        cc.apply(cc.ctx, slot, q.op)
      except CatchableError, Defect:
        # Never let an apply failure kill the consumer or wedge a submitter:
        # the appliedSeq barrier still advances below.
        discard
      finally:
        endWrite(slot.mu)
    acquire slot.appliedLock
    if q.order > slot.appliedSeq:
      slot.appliedSeq = q.order
    broadcast slot.appliedCond
    release slot.appliedLock
  )

proc newConcurrentState*[T](apply: proc(ctx: pointer, slot: TableSlot[T], op: T) {.gcsafe.},
                            ctx: pointer): ConcurrentState[T] =
  result = ConcurrentState[T]()
  initRwLock(result.metaMu)
  initLock(result.walLock)
  result.walPending.store(0, moRelaxed)
  result.ctx = ctx
  result.apply = apply
  result.ring = newWriteRing[Queued[T]]()
  result.stopped.store(false, moRelaxed)
  createThread(result.consumer, consumerLoop[T], result)

proc appendWal*[T](cc: ConcurrentState[T], wal: var Wal, entry: WalEntry,
                   flushEvery: int) =
  ## Appends one WAL entry under the store's WAL lock, flushing in a group
  ## commit once `flushEvery` entries are pending (async durability).
  acquire cc.walLock
  try:
    discard wal.append(entry, sync = false)
    let n = cc.walPending.load(moRelaxed) + 1
    if n >= max(1, flushEvery):
      wal.flush()
      cc.walPending.store(0, moRelaxed)
    else:
      cc.walPending.store(n, moRelaxed)
  finally:
    release cc.walLock

proc flushWal*[T](cc: ConcurrentState[T], wal: var Wal, clear = true) =
  acquire cc.walLock
  try:
    if clear:
      wal.flush()
    else:
      wal.flushNoClear()
    cc.walPending.store(0, moRelaxed)
  finally:
    release cc.walLock

proc close*[T](cc: ConcurrentState[T], wal: var Wal) =
  ## Stops the consumer thread and performs a final WAL flush.
  cc.ring.stop()
  cc.consumer.joinThread()
  cc.flushWal(wal)

#
# reader/writer templates over a table slot and the store meta state
#
template withSlotRead*[T](slot: TableSlot[T], body: untyped) =
  beginRead(slot.mu)
  try:
    body
  finally:
    endRead(slot.mu)

template withSlotWrite*[T](slot: TableSlot[T], body: untyped) =
  beginWrite(slot.mu)
  try:
    body
  finally:
    endWrite(slot.mu)

template withMetaRead*[T](cc: ConcurrentState[T], body: untyped) =
  beginRead(cc.metaMu)
  try:
    body
  finally:
    endRead(cc.metaMu)

template withMetaWrite*[T](cc: ConcurrentState[T], body: untyped) =
  beginWrite(cc.metaMu)
  try:
    body
  finally:
    endWrite(cc.metaMu)
