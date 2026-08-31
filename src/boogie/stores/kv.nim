# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[tables, options, os]
import ../fbe_codec
import ../wal
import ../concurrency
import ../crashsafe

## This module implements a simple key-value store with optional
## write-ahead logging (WAL) and disk persistence. It uses an in-memory hash map
## for fast lookups and a sorted index for ordered iteration. The WAL allows for durability
## and crash recovery, while periodic checkpoints can be taken to speed up recovery time.
##
## With `enableConcurrency = true` the store supports unlimited simultaneous
## reader and writer threads: reads are served concurrently under a per-table
## read lock, and writes are serialized through a bounded worker pool with
## synchronous visibility and async (batched) WAL durability.

type
  KvStorageMode* = enum
    ## Defines the storage mode for the key-value store. `ksmInMemory` creates a purely
    ## in-memory store with no persistence, while `ksmDisk` creates a file-based
    ## store that can persist data across restarts
    ksmInMemory, ksmDisk

  KvStoreError* = object of CatchableError
    ## A catchable exception type for errors related to the key-value store operations.

  KvWriteOp = enum
    woPut, woDelete

  KvWriteTask = object
    ## Write task carried across threads (value types only, no shared refs).
    kind: KvWriteOp
    key, value: string

  KvStore* {.acyclic.} = ref object
    ## The main data structure for the key-value store
    dataByKey: Table[string, string]

    storageMode: KvStorageMode
    hasWal: bool
    wal: Wal

    hasDbFile: bool
    dbPath: string

    checkpointLsn: uint64
    pendingOps: uint32
    checkpointEveryOps: uint32

    walFlushEveryOps: uint32
    pendingWalOps: uint32

    lazyReads: bool
      ## In lazy-read mode values are not retained in memory: `valueOffsets`
      ## maps each key to its record's byte offset in the WAL, and `get` reads
      ## the value back from disk on demand. WAL-only durability (no snapshots),
      ## trading read latency for a small memory footprint on large datasets.
    valueOffsets: Table[string, int64]

    cc: ConcurrentState[KvWriteTask]
      ## Store-level concurrency state; nil unless `enableConcurrency = true`.
    slot: TableSlot[KvWriteTask]
      ## The store's single table slot; nil unless `enableConcurrency = true`.

const
  KvTableName = "__kv__"

proc recoverFromWal*(s: KvStore)

proc writeTextAtomic(path, content: string) =
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc putNoWal(s: KvStore, key, value: string) =
  if key.len == 0:
    raise newException(KvStoreError, "key cannot be empty")
  s.dataByKey[key] = if s.lazyReads: "" else: value

proc deleteNoWal(s: KvStore, key: string): bool =
  if not s.dataByKey.hasKey(key):
    return false
  s.dataByKey.del(key)
  true

proc buildSnapshot(s: KvStore): KvSnapshotOnDisk =
  result.version = 1'u32
  result.checkpointLsn = s.checkpointLsn
  for k, v in s.dataByKey.pairs:
    result.entries.add((k, v))

proc loadSnapshotIntoStore(s: KvStore, snap: KvSnapshotOnDisk) =
  if snap.version != 1'u32:
    raise newException(KvStoreError, "unsupported .db snapshot version")
  s.dataByKey = initTable[string, string]()
  s.checkpointLsn = snap.checkpointLsn
  for (k, v) in snap.entries:
    s.dataByKey[k] = v

proc saveSnapshotIfEnabled(s: KvStore) =
  if not s.hasDbFile:
    return
  let blob = encodeKvSnapshotToString(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: KvStore) =
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = decodeKvSnapshotFromString(blob)
  s.loadSnapshotIntoStore(snap)

proc flushWalIfNeeded(s: KvStore, force = false) =
  if not s.hasWal:
    return
  if force:
    s.wal.flush()
    s.pendingWalOps = 0'u32
    return
  if s.walFlushEveryOps == 0'u32:
    return
  if s.pendingWalOps >= s.walFlushEveryOps:
    s.wal.flush()
    s.pendingWalOps = 0'u32

proc appendWalIfEnabled(s: KvStore, op: WalOp, key, payload: string): uint64 =
  # Appends an operation to the WAL if it is enabled
  if not s.hasWal:
    return 0'u64
  if s.lazyReads:
    # sync append so the record's file offset is known immediately, and record
    # it for offset-based reads
    let off = s.wal.logPos()
    let lsn = s.wal.append(
      WalEntry(op: op, table: KvTableName, pk: key, payload: payload),
      sync = true
    )
    if op == woInsertRow:
      s.valueOffsets[key] = off
    return lsn
  let lsn = s.wal.append(
    WalEntry(op: op, table: KvTableName, pk: key, payload: payload),
    sync = false
  )
  inc s.pendingWalOps
  s.flushWalIfNeeded(force = false)
  lsn # return the LSN of the appended WAL entry for checkpointing purposes

proc markCommitted(s: KvStore, lsn: uint64) =
  # Update the checkpoint LSN to reflect the latest committed operation
  if lsn > s.checkpointLsn:
    s.checkpointLsn = lsn

  if s.hasDbFile and s.checkpointEveryOps > 0'u32:
    inc s.pendingOps
    if s.pendingOps >= s.checkpointEveryOps:
      s.saveSnapshotIfEnabled()
      s.pendingOps = 0'u32

proc applyWalEntry(s: KvStore, e: WalEntry) =
  # Applies a single WAL entry to the in-memory store. This is used during
  # recovery to replay operations that have not yet been checkpointed.
  if e.table != KvTableName:
    return
  case e.op
  of woInsertRow:
    s.putNoWal(e.pk, e.payload)
  of woDeleteRow:
    discard s.deleteNoWal(e.pk)
  else:
    raise newException(KvStoreError, "WAL replay: unsupported op for kvstore: " & $e.op)

#
# Public API
#
proc newKvStore*(path: string, mode: KvStorageMode = ksmDisk, enableWal: bool = true,
        checkpointEveryOps: uint32 = 0'u32, walFlushEveryOps: uint32 = 1000'u32,
        lazyReads = false, enableConcurrency: static bool = false): KvStore =
  ## Creates a new key-value store. If `mode` is `ksmDisk`, a file-based
  ## store is created at the given `path`. If `enableWal` is true, write-ahead
  ## logging is enabled for durability. The `checkpointEveryOps` parameter controls
  ## how often a checkpoint (snapshot) is taken after a certain number of operations,
  ## while `walFlushEveryOps` controls how often the WAL is flushed to disk.
  ##
  ## With `lazyReads = true` the store keeps only a key -> offset index in memory
  ## (WAL-only durability, no snapshots) and reads values back from the WAL on
  ## demand. This trades read latency for a small memory footprint on large datasets.
  ##
  ## With `enableConcurrency = true` the store supports unlimited simultaneous
  ## reader and writer threads: reads are served concurrently under a read lock,
  ## and writes are serialized through a bounded worker pool (synchronous
  ## visibility, async batched WAL durability). Concurrent mode is WAL-only
  ## (no snapshots).
  var
    dbPath: string
    hasDb: bool
    hasWal: bool
    walObj: Wal

  case mode
  of ksmInMemory:
    discard
  of ksmDisk:
    if path.len == 0:
      raise newException(KvStoreError, "path cannot be empty in disk mode")
    hasDb = not lazyReads
    dbPath = path.changeFileExt(".db")
    if enableWal:
      hasWal = true
      walObj = openWal(path)

  when enableConcurrency:
    static: doAssert compileOption("threads"), "concurrency requires --threads:on"
    hasDb = false

  result = KvStore(
    dataByKey: initTable[string, string](),
    storageMode: mode,
    hasWal: hasWal,
    wal: walObj,
    hasDbFile: hasDb,
    dbPath: dbPath,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps,
    lazyReads: lazyReads,
    valueOffsets: initTable[string, int64]()
  )

  when enableConcurrency:
    result.slot = newTableSlot[KvWriteTask](cast[pointer](result))
    result.cc = newConcurrentState[KvWriteTask](
      proc(ctx: pointer, slot: TableSlot[KvWriteTask], op: KvWriteTask) {.gcsafe.} =
        let store = cast[KvStore](ctx)
        case op.kind
        of woPut:
          store.putNoWal(op.key, op.value)
          if store.hasWal:
            store.cc.appendWal(store.wal,
              WalEntry(op: woInsertRow, table: KvTableName, pk: op.key, payload: op.value),
              int(store.walFlushEveryOps))
        of woDelete:
          discard store.deleteNoWal(op.key)
          if store.hasWal:
            store.cc.appendWal(store.wal,
              WalEntry(op: woDeleteRow, table: KvTableName, pk: op.key, payload: ""),
              int(store.walFlushEveryOps))
      ,
      cast[pointer](result)
    )

  recoverFromWal(result)

  let s = result
  registerStoreFlush(cast[pointer](s), proc() {.gcsafe.} =
    if s.hasWal:
      if s.cc != nil:
        s.cc.flushWal(s.wal, clear = false)
      else:
        s.wal.flushNoClear()
  )

proc newInMemoryKvStore*(): KvStore =
  ## Creates a new in-memory key-value store with no persistence or WAL.
  ## This is useful for testing or scenarios where durability is not required.
  newKvStore("", ksmInMemory, false)

proc checkpoint*(s: KvStore) =
  ## Forces a checkpoint (snapshot) to be taken immediately. This can be used to
  ## ensure that all operations up to the current LSN are persisted to disk,
  ## which can speed up recovery time in case of a crash. If WAL is enabled,
  ## the WAL is flushed before taking the snapshot to ensure durability.
  if s.cc != nil:
    s.cc.flushWal(s.wal)
    return
  if not s.hasDbFile:
    return
  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32

proc close*(s: KvStore) =
  ## Stops the write workers (if concurrent) and flushes the WAL.
  unregisterStoreFlush(cast[pointer](s))
  if s.cc != nil:
    s.cc.close(s.wal)
  else:
    s.flushWalIfNeeded(force = true)

proc put*(s: KvStore, key, value: string) =
  ## Inserts or updates the value for the given key. If WAL is enabled,
  ## the operation is first appended to the WAL before being applied to
  ## the in-memory store. The checkpoint LSN is updated accordingly.
  if s.cc != nil:
    let mySeq = s.cc.submit(s.slot, KvWriteTask(kind: woPut, key: key, value: value))
    s.slot.waitApplied(mySeq)
    return
  let lsn = s.appendWalIfEnabled(woInsertRow, key, value)
  s.putNoWal(key, value)
  s.markCommitted(lsn)

proc get*(s: KvStore, key: string): Option[string] =
  ## Retrieves the value for the given key, if it exists.
  ## Returns `none` if the key is not found.
  if s.cc != nil:
    var res: Option[string]
    withSlotRead(s.slot):
      if s.lazyReads:
        if s.valueOffsets.hasKey(key):
          res = some(s.wal.readEntryAt(s.valueOffsets[key]).payload)
        else:
          res = none(string)
      else:
        if s.dataByKey.hasKey(key):
          res = some(s.dataByKey[key])
        else:
          res = none(string)
    return res
  if s.lazyReads:
    if s.valueOffsets.hasKey(key):
      some(s.wal.readEntryAt(s.valueOffsets[key]).payload)
    else:
      none(string)
  else:
    if s.dataByKey.hasKey(key):
      some(s.dataByKey[key])
    else:
      none(string)

proc hasKey*(s: KvStore, key: string): bool =
  ## Checks if the given key exists in the store.
  if s.cc != nil:
    var res = false
    withSlotRead(s.slot):
      res = s.dataByKey.hasKey(key)
    return res
  s.dataByKey.hasKey(key)

proc delete*(s: KvStore, key: string): bool {.discardable.} =
  ## Deletes the given key from the store. If WAL is enabled, the delete
  ## operation is first appended to the WAL before being applied to the in-memory store.
  ## 
  ## Returns true if the key was found and deleted, false if the key was not found.
  if s.cc != nil:
    let mySeq = s.cc.submit(s.slot, KvWriteTask(kind: woDelete, key: key))
    s.slot.waitApplied(mySeq)
    return true
  let lsn = s.appendWalIfEnabled(woDeleteRow, key, "")
  let removed = s.deleteNoWal(key)
  if s.lazyReads:
    s.valueOffsets.del(key)
  s.markCommitted(lsn)
  removed

proc len*(s: KvStore): int =
  ## Returns the number of key-value pairs currently stored.
  if s.cc != nil:
    var res = 0
    withSlotRead(s.slot):
      res = s.dataByKey.len
    return res
  s.dataByKey.len

proc isEmpty*(s: KvStore): bool =
  ## Checks if the store is empty (contains no key-value pairs).
  if s.cc != nil:
    var res = true
    withSlotRead(s.slot):
      res = s.dataByKey.len == 0
    return res
  s.dataByKey.len == 0

proc retainedValueBytes*(s: KvStore): int64 =
  ## Total bytes of value payloads currently held in memory. In lazy-read mode
  ## this is always 0 — values live in the WAL and are read back on demand.
  if s.lazyReads:
    return 0'i64
  for _, v in s.dataByKey:
    result += int64(v.len)

iterator pairsUnordered*(s: KvStore): (string, string) =
  ## Iterates over all key-value pairs in the store in no particular order.
  if s.cc != nil:
    beginRead(s.slot.mu)
    try:
      if s.lazyReads:
        for k in s.dataByKey.keys:
          yield (k, s.wal.readEntryAt(s.valueOffsets[k]).payload)
      else:
        for k, v in s.dataByKey.pairs:
          yield (k, v)
    finally:
      endRead(s.slot.mu)
  elif s.lazyReads:
    for k in s.dataByKey.keys:
      yield (k, s.wal.readEntryAt(s.valueOffsets[k]).payload)
  else:
    for k, v in s.dataByKey.pairs:
      yield (k, v)

proc recoverFromWal*(s: KvStore) =
  ## Recovers the in-memory state of the store by replaying any WAL entries
  ## that have not yet been checkpointed. This is called during initialization to
  ## ensure that the store reflects all committed operations even after a crash.
  ## 
  ## The checkpoint LSN is updated to reflect the latest applied WAL entry
  s.dataByKey = initTable[string, string]()
  s.checkpointLsn = 0'u64
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

  if s.lazyReads:
    # WAL-only recovery: build the key set + key -> offset index without
    # retaining values in memory.
    s.valueOffsets = initTable[string, int64]()
    if s.hasWal:
      for (off, e) in s.wal.entriesWithOffsets:
        if e.lsn <= s.checkpointLsn:
          continue
        case e.op
        of woInsertRow:
          s.valueOffsets[e.pk] = off
          s.dataByKey[e.pk] = ""
        of woDeleteRow:
          s.valueOffsets.del(e.pk)
          s.dataByKey.del(e.pk)
        else:
          raise newException(KvStoreError, "WAL replay: unsupported op for kvstore: " & $e.op)
        s.checkpointLsn = e.lsn
    s.wal.openLog()
    s.flushWalIfNeeded(force = true)
    s.pendingOps = 0'u32
    s.pendingWalOps = 0'u32
    return

  s.loadSnapshotIfPresent()

  if s.hasWal:
    for e in s.wal.entries:
      if e.lsn <= s.checkpointLsn:
        continue
      s.applyWalEntry(e)
      s.checkpointLsn = e.lsn

  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32