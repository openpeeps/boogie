# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[os, times]

## This module implements a simple write-ahead log (WAL) for durability in the Boogie database.
## The WAL is designed to be simple and efficient, supporting group commits and crash recovery.
## 
## The WAL file format is binary and consists of a magic signature, followed by a sequence of log
## entries. Each entry includes a log sequence number (LSN), a timestamp, an operation type, the
## affected table and primary key, and a payload (e.g. the row data for inserts/updates).
##
## The main types are:
## - `WalOp`: an enum of the supported operations (create table, drop table, insert row, delete row, update row)
## - `WalEntry`: a record representing a single log entry
## - `Wal`: the main WAL object that manages the log file and pending entries for group commits
## The WAL supports the following operations:
## - `openWal(path)`: opens or creates a WAL file at the given path
## - `append(wal, entry, sync)`: appends an entry to the WAL, optionally flushing to disk immediately
## - `flush(wal)`: flushes any pending entries to disk
## - `replay(wal, onEntry)`: replays the WAL entries by calling the provided callback for each entry
## - `reset(wal)`: resets the WAL by clearing pending entries and writing a new header
## - `entries(wal)`: an iterator over all entries in the WAL file

type
  WalOp* = enum
    ## Supported WAL operations. This is used to identify the type of change being logged.
    woCreateTable,
    woDropTable,
    woInsertRow,
    woDeleteRow,
    woUpdateRow

  WalEntry* = object
    ## A single WAL entry representing a change to the database
    lsn*: uint64
      ## Log Sequence Number, a monotonically increasing identifier for each log entry
    tsUnix*: int64
      ## Timestamp of the log entry in Unix time (seconds since epoch)
    op*: WalOp
      ## The type of operation (e.g. insert, update, delete)
    table*: string
      ## The name of the affected table
    pk*: string
      ## The primary key of the affected row (if applicable)
    payload*: string
      ## The payload of the log entry, typically a JSON string representing the row data for inserts/updates

  Wal* = object
    ## The main WAL object that manages the log file and pending entries for group commits
    path*: string
      ## The file path to the WAL file on disk
    nextLsn*: uint64
      ## The next LSN to be assigned to a new log entry. This is computed based
      ## on the existing WAL file on open.
    pendingEntries*: seq[WalEntry]  # buffered for group commit
      ## A sequence of pending WAL entries that have been appended but not yet flushed to disk.
      ## This allows for efficient group commits, where multiple operations can be flushed together
      ## to reduce disk I/O. The `append` procedure adds entries to this buffer, and the `flush`
      ## procedure writes them to disk and clears the buffer.
      ## 
      ## The `walFlushEveryOps` configuration in the Store determines how many pending entries can
      ## accumulate before an automatic flush is triggered. This helps to balance durability with performance.
      ## 
      ## Note that if the application crashes before pending entries are flushed, those entries will be lost.
      ## However, once entries are flushed to disk, they are durable and will be replayed on recovery.

  WalError* = object of CatchableError

let WalMagic* = "BOGWAL2\0"
  ## magic string to identify our WAL files and version,
  ## also serves as a simple integrity check on open

const MaxFieldBytes = 64 * 1024 * 1024'u32 # 64MB safety cap per string field

proc writeExact(f: File, p: pointer, n: int) =
  if n <= 0: return
  let wrote = f.writeBuffer(p, n)
  if wrote != n:
    raise newException(WalError, "WAL write failed")

proc readExact(f: File, p: pointer, n: int): bool =
  if n <= 0: return true
  f.readBuffer(p, n) == n

proc writeU8(f: File, v: uint8) =
  var b = v
  writeExact(f, addr b, 1)

proc writeU32Le(f: File, v: uint32) =
  var b: array[4, uint8]
  b[0] = uint8(v and 0xFF'u32)
  b[1] = uint8((v shr 8) and 0xFF'u32)
  b[2] = uint8((v shr 16) and 0xFF'u32)
  b[3] = uint8((v shr 24) and 0xFF'u32)
  writeExact(f, addr b[0], 4)

proc writeU64Le(f: File, v: uint64) =
  var b: array[8, uint8]
  for i in 0..7:
    b[i] = uint8((v shr (i * 8)) and 0xFF'u64)
  writeExact(f, addr b[0], 8)

proc readU8(f: File, outv: var uint8): bool =
  var b: uint8
  if not readExact(f, addr b, 1): return false
  outv = b
  result = true

proc readU32Le(f: File, outv: var uint32): bool =
  var b: array[4, uint8]
  if not readExact(f, addr b[0], 4): return false
  outv = uint32(b[0]) or (uint32(b[1]) shl 8) or (uint32(b[2]) shl 16) or (uint32(b[3]) shl 24)
  result = true

proc readU64Le(f: File, outv: var uint64): bool =
  var b: array[8, uint8]
  if not readExact(f, addr b[0], 8): return false
  outv = 0'u64
  for i in 0..7:
    outv = outv or (uint64(b[i]) shl (i * 8))
  result = true

proc writeStringBin(f: File, s: string) =
  if s.len > int(high(uint32)):
    raise newException(WalError, "WAL string too large")
  writeU32Le(f, uint32(s.len))
  if s.len > 0:
    writeExact(f, unsafeAddr s[0], s.len)

proc readStringBin(f: File, s: var string): bool =
  var n: uint32
  if not readU32Le(f, n): return
  if n > MaxFieldBytes: return
  if n == 0'u32:
    s = ""
    return true
  s = newString(int(n))
  readExact(f, addr s[0], int(n))

proc writeHeader(path: string) =
  let f = open(path, fmWrite)
  defer: f.close()
  writeExact(f, unsafeAddr WalMagic[0], WalMagic.len)
  f.flushFile()

proc ensureWalFile(path: string) =
  if not fileExists(path):
    writeHeader(path)
    return
  if getFileSize(path) == 0:
    writeHeader(path)

proc readHeader(f: File): bool =
  var hdr = newString(WalMagic.len)
  if not readExact(f, addr hdr[0], WalMagic.len): return false
  hdr == WalMagic

proc writeEntry(f: File, e: WalEntry) =
  writeU64Le(f, e.lsn)
  writeU64Le(f, cast[uint64](e.tsUnix))
  writeU8(f, uint8(ord(e.op)))
  writeStringBin(f, e.table)
  writeStringBin(f, e.pk)
  writeStringBin(f, e.payload)

proc readEntry(f: File, e: var WalEntry): bool =
  var lsnU, tsU: uint64
  var opRaw: uint8
  if not readU64Le(f, lsnU): return false
  if not readU64Le(f, tsU): return false
  if not readU8(f, opRaw): return false
  if int(opRaw) < ord(low(WalOp)) or int(opRaw) > ord(high(WalOp)): return false

  var table, pk, payload: string
  if not readStringBin(f, table): return false
  if not readStringBin(f, pk): return false
  if not readStringBin(f, payload): return false

  e.lsn = lsnU
  e.tsUnix = cast[int64](tsU)
  e.op = WalOp(opRaw)
  e.table = table
  e.pk = pk
  e.payload = payload
  result = true

proc computeNextLsn(path: string): uint64 =
  if not fileExists(path):
    return 1'u64

  let f = open(path, fmRead)
  defer: f.close()
  if not readHeader(f):
    raise newException(WalError, "invalid WAL header (expected binary WAL v2)")

  var maxLsn = 0'u64
  var e: WalEntry
  while readEntry(f, e):
    if e.lsn > maxLsn:
      maxLsn = e.lsn
  maxLsn + 1'u64

proc openWal*(path: string): Wal =
  ## Opens or creates a WAL file at the given path. If the file already exists,
  ## it reads the header and computes the next LSN based on the existing entries.
  ## 
  ## If the file does not exist or is empty, it initializes a new WAL file with
  ## the correct header and starts with LSN 1.
  let walPath = path.changeFileExt(".wal")
  ensureWalFile(walPath)
  Wal(path: walPath, nextLsn: computeNextLsn(walPath), pendingEntries: @[])

proc flush*(w: var Wal) =
  ## Flushes any pending entries to disk. This is typically called
  ## after a group of operations have been appended
  if w.pendingEntries.len == 0:
    return

  let f = open(w.path, fmAppend)
  defer: f.close()

  for e in w.pendingEntries:
    writeEntry(f, e)
  f.flushFile()
  w.pendingEntries.setLen(0)

proc append*(w: var Wal, entry: WalEntry, sync: bool = true): uint64 =
  ## Appends a new entry to the WAL. The entry is assigned the next
  ## LSN and timestamp, and added to the pending entries buffer.
  var e = entry
  e.lsn = w.nextLsn
  e.tsUnix = getTime().toUnix()

  w.pendingEntries.add(e)
  if sync:
    w.flush()

  inc w.nextLsn
  e.lsn

proc replay*(w: Wal, onEntry: proc(e: WalEntry)) =
  ## Replays the WAL by reading all entries from the WAL file
  ## and calling the provided callback for each entry.
  if not fileExists(w.path): return

  let f = open(w.path, fmRead)
  defer: f.close()
  if not readHeader(f):
    raise newException(WalError, "invalid WAL header (expected binary WAL v2)")

  var e: WalEntry
  while readEntry(f, e):
    onEntry(e)

proc reset*(w: var Wal) =
  ## Resets the WAL by clearing pending entries and writing a new header. This is typically called
  ## after a checkpoint to start a new WAL segment. Note that this will discard any pending
  ## entries that have not been flushed, so it should be used with caution
  w.pendingEntries.setLen(0)
  writeHeader(w.path)
  w.nextLsn = 1'u64

iterator entries*(w: Wal): WalEntry =
  ## An iterator over all entries in the WAL file. This can
  ## be used for replaying the WAL during recovery.
  if likely(fileExists(w.path)):

    let f = open(w.path, fmRead)
    defer: f.close()

    if not readHeader(f):
      raise newException(WalError,
        "invalid WAL header (expected binary WAL v2)")
    var e: WalEntry
    while readEntry(f, e):
      yield e
