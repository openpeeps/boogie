# Boogie KV Store - WAL-based key-value storage using sorta BTree tables
#
# (c) 2026 George Lemon | LGPLv3 License

import std/[tables, options, strformat, os]
import pkg/[flatty, sorta]
import ../wal

## This module implements a simple key-value store with optional
## write-ahead logging (WAL) and disk persistence. It uses an in-memory hash map
## for fast lookups and a sorted index for ordered iteration. The WAL allows for durability
## and crash recovery, while periodic checkpoints can be taken to speed up recovery time.

type
  KvStorageMode* = enum
    ## Defines the storage mode for the key-value store. `ksmInMemory` creates a purely
    ## in-memory store with no persistence, while `ksmDisk` creates a file-based
    ## store that can persist data across restarts
    ksmInMemory, ksmDisk

  KvStoreError* = object of CatchableError
    ## A catchable exception type for errors related to the key-value store operations.

  KeyRecord = object
    k: string

  KvStore* {.acyclic.} = ref object
    ## The main data structure for the key-value store
    dataByKey: OrderedTable[string, string]

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

type
  KvSnapshotOnDisk = tuple
    version: uint32
    checkpointLsn: uint64
    entries: seq[(string, string)]

const
  KvTableName = "__kv__"

proc recoverFromWal*(s: KvStore)

proc cmp(a, b: KeyRecord): int = cmp(a.k, b.k)
proc extract(r: KeyRecord): string = r.k
proc `==`(a, b: KeyRecord): bool = a.k == b.k

proc writeTextAtomic(path, content: string) =
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc putNoWal(s: KvStore, key, value: string) =
  if key.len == 0:
    raise newException(KvStoreError, "key cannot be empty")
  s.dataByKey[key] = value

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
  s.dataByKey = initOrderedTable[string, string](snap.entries.len)
  s.checkpointLsn = snap.checkpointLsn
  for (k, v) in snap.entries:
    s.dataByKey[k] = v

proc saveSnapshotIfEnabled(s: KvStore) =
  if not s.hasDbFile:
    return
  let blob = toFlatty(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: KvStore) =
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = fromFlatty(blob, KvSnapshotOnDisk)
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
        checkpointEveryOps: uint32 = 0'u32, walFlushEveryOps: uint32 = 1000'u32): KvStore =
  ## Creates a new key-value store. If `mode` is `ksmDisk`, a file-based
  ## store is created at the given `path`. If `enableWal` is true, write-ahead
  ## logging is enabled for durability. The `checkpointEveryOps` parameter controls
  ## how often a checkpoint (snapshot) is taken after a certain number of operations,
  ## while `walFlushEveryOps` controls how often the WAL is flushed to disk.
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
    hasDb = true
    dbPath = path.changeFileExt(".db")
    if enableWal:
      hasWal = true
      walObj = openWal(path)

  result = KvStore(
    dataByKey: initOrderedTable[string, string](),
    storageMode: mode,
    hasWal: hasWal,
    wal: walObj,
    hasDbFile: hasDb,
    dbPath: dbPath,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps
  )

  recoverFromWal(result)

proc newInMemoryKvStore*(): KvStore =
  ## Creates a new in-memory key-value store with no persistence or WAL.
  ## This is useful for testing or scenarios where durability is not required.
  newKvStore("", ksmInMemory, false)

proc checkpoint*(s: KvStore) =
  ## Forces a checkpoint (snapshot) to be taken immediately. This can be used to
  ## ensure that all operations up to the current LSN are persisted to disk,
  ## which can speed up recovery time in case of a crash. If WAL is enabled,
  ## the WAL is flushed before taking the snapshot to ensure durability.
  if not s.hasDbFile:
    return
  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32

proc put*(s: KvStore, key, value: string) =
  ## Inserts or updates the value for the given key. If WAL is enabled,
  ## the operation is first appended to the WAL before being applied to
  ## the in-memory store. The checkpoint LSN is updated accordingly.
  let lsn = s.appendWalIfEnabled(woInsertRow, key, value)
  s.putNoWal(key, value)
  s.markCommitted(lsn)

proc get*(s: KvStore, key: string): Option[string] =
  ## Retrieves the value for the given key, if it exists.
  ## Returns `none` if the key is not found.
  if s.dataByKey.hasKey(key):
    some(s.dataByKey[key])
  else:
    none(string)

proc hasKey*(s: KvStore, key: string): bool =
  ## Checks if the given key exists in the store.
  s.dataByKey.hasKey(key)

proc delete*(s: KvStore, key: string): bool {.discardable.} =
  ## Deletes the given key from the store. If WAL is enabled, the delete
  ## operation is first appended to the WAL before being applied to the in-memory store.
  ## 
  ## Returns true if the key was found and deleted, false if the key was not found.
  let lsn = s.appendWalIfEnabled(woDeleteRow, key, "")
  let removed = s.deleteNoWal(key)
  s.markCommitted(lsn)
  removed

proc len*(s: KvStore): int =
  ## Returns the number of key-value pairs currently stored.
  s.dataByKey.len

proc isEmpty*(s: KvStore): bool =
  ## Checks if the store is empty (contains no key-value pairs).
  s.dataByKey.len == 0

iterator pairsUnordered*(s: KvStore): (string, string) =
  ## Iterates over all key-value pairs in the store in no particular order.
  for k, v in s.dataByKey.pairs:
    yield (k, v)

proc recoverFromWal*(s: KvStore) =
  ## Recovers the in-memory state of the store by replaying any WAL entries
  ## that have not yet been checkpointed. This is called during initialization to
  ## ensure that the store reflects all committed operations even after a crash.
  ## 
  ## The checkpoint LSN is updated to reflect the latest applied WAL entry
  s.dataByKey = initOrderedTable[string, string]()
  s.checkpointLsn = 0'u64
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32

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