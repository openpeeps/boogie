# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | LGPL-3.0-or-later License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[os, times]

## This module implements a simple write-ahead log (WAL) for durability in the Boogie database.
## The WAL is designed to be simple and efficient, supporting group commits and crash recovery.
## 
## The WAL file format is binary and consists of a magic signature, followed by a sequence of
## framed log records. Each record is wrapped as `@ <u32 len> <u32 crc32> <body>`, where the
## body is the entry fields (LSN, timestamp, operation, table, primary key, payload). A successful
## flush ends with a `! <u64 lastLsn>` footer, giving O(1) recovery of the next LSN on open and a
## well-defined boundary after every group commit.
##
## The log file handle is kept open across flushes (lazily opened on first use) to avoid the
## open/close syscall cost per flush, and a whole batch is written as one buffered write.
##
## The main types are:
## - `WalOp`: an enum of the supported operations (create table, drop table, insert row, delete row, update row)
## - `WalEntry`: a record representing a single log entry
## - `Wal`: the main WAL object that manages the log file and pending entries for group commits
## The WAL supports the following operations:
## - `openWal(path)`: opens or creates a WAL file at the given path
## - `append(wal, entry, sync)`: appends an entry to the WAL, optionally flushing to disk immediately
## - `flush(wal)`: flushes any pending entries to disk as one batch
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
      ## Timestamp of the log entry in Unix time (seconds since epoch). Stamped once per
      ## flush batch so group commits do not pay a clock read per append.
    op*: WalOp
      ## The type of operation (e.g. insert, update, delete)
    table*: string
      ## The name of the affected table
    pk*: string
      ## The primary key of the affected row (if applicable)
    payload*: string
      ## The payload of the log entry, typically a JSON string representing the row data for inserts/updates

  LogFile = ref object
    ## Heap box for the persistent file handle. Storing the handle behind a ref makes
    ## copies of `Wal` (which happen during store construction) share one safe handle.
    handle: File

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
    log: LogFile
      ## Persistent open file handle, lazily opened on the first flush

  WalError* = object of CatchableError

let WalMagic* = "BOGWAL3\0"
  ## magic string to identify our WAL files and version,
  ## also serves as a simple integrity check on open

const
  MaxFieldBytes = 64 * 1024 * 1024'u32 # 64MB safety cap per string field
  RecordTag = 0x40'u8  # '@'
  FooterTag = 0x21'u8  # '!'

#
# crc32 (slicing-by-8, from the classic public-domain tables)
#
proc buildCrc32Tables(): array[8, array[256, uint32]] =
  for i in 0 ..< result[0].len:
    var crc = i.uint32
    for _ in 0 ..< 8:
      if (crc and 1'u32) != 0'u32:
        crc = (crc shr 1) xor 0xEDB88320'u32
      else:
        crc = crc shr 1
    result[0][i] = crc
  for table in 1 ..< result.len:
    for i in 0 ..< result[table].len:
      let previous = result[table - 1][i]
      result[table][i] =
        (previous shr 8) xor result[0][int(previous and 0xFF'u32)]

const Crc32Tables = buildCrc32Tables()

proc crc32(data: string): uint32 =
  var crc = 0xFFFFFFFF'u32
  var i = 0
  while i + 8 <= data.len:
    let firstWord =
      uint32(ord(data[i])) or
      (uint32(ord(data[i + 1])) shl 8) or
      (uint32(ord(data[i + 2])) shl 16) or
      (uint32(ord(data[i + 3])) shl 24)
    let first = crc xor firstWord
    let second =
      uint32(ord(data[i + 4])) or
      (uint32(ord(data[i + 5])) shl 8) or
      (uint32(ord(data[i + 6])) shl 16) or
      (uint32(ord(data[i + 7])) shl 24)
    crc = Crc32Tables[7][int(first and 0xFF'u32)] xor
          Crc32Tables[6][int((first shr 8) and 0xFF'u32)] xor
          Crc32Tables[5][int((first shr 16) and 0xFF'u32)] xor
          Crc32Tables[4][int(first shr 24)] xor
          Crc32Tables[3][int(second and 0xFF'u32)] xor
          Crc32Tables[2][int((second shr 8) and 0xFF'u32)] xor
          Crc32Tables[1][int((second shr 16) and 0xFF'u32)] xor
          Crc32Tables[0][int(second shr 24)]
    inc i, 8
  while i < data.len:
    let idx = int((crc xor uint32(ord(data[i]))) and 0xFF'u32)
    crc = (crc shr 8) xor Crc32Tables[0][idx]
    inc i
  result = not crc

static:
  doAssert crc32("123456789") == 0xCBF43926'u32

#
# buffer encoding (one contiguous string per entry body)
#
proc putU8(s: var string, v: uint8) =
  s.add(char(v))

proc putU32(s: var string, v: uint32) =
  for i in 0..3:
    s.add(char((v shr (i * 8)) and 0xFF'u32))

proc putU64(s: var string, v: uint64) =
  for i in 0..7:
    s.add(char((v shr (i * 8)) and 0xFF'u64))

proc putStr(s: var string, x: string) =
  if x.len > int(high(uint32)):
    raise newException(WalError, "WAL string too large")
  putU32(s, uint32(x.len))
  s.add(x)

proc readU8At(s: string, off: var int): uint8 =
  result = uint8(ord(s[off]))
  inc off

proc readU32At(s: string, off: var int): uint32 =
  result = uint32(ord(s[off])) or
    (uint32(ord(s[off + 1])) shl 8) or
    (uint32(ord(s[off + 2])) shl 16) or
    (uint32(ord(s[off + 3])) shl 24)
  off += 4

proc readU64At(s: string, off: var int): uint64 =
  for i in 0..7:
    result = result or (uint64(ord(s[off + i])) shl (i * 8))
  off += 8

proc readStrAt(s: string, off: var int): string =
  let n = int(readU32At(s, off))
  if n > int(MaxFieldBytes):
    raise newException(WalError, "WAL string too large")
  result = s[off ..< off + n]
  off += n

proc encodeEntry(e: WalEntry): string =
  putU64(result, e.lsn)
  putU64(result, cast[uint64](e.tsUnix))
  putU8(result, uint8(ord(e.op)))
  putStr(result, e.table)
  putStr(result, e.pk)
  putStr(result, e.payload)

proc decodeEntry(body: string, off: var int): WalEntry =
  result.lsn = readU64At(body, off)
  result.tsUnix = cast[int64](readU64At(body, off))
  let opRaw = readU8At(body, off)
  if int(opRaw) < ord(low(WalOp)) or int(opRaw) > ord(high(WalOp)):
    raise newException(WalError, "invalid WAL op")
  result.op = WalOp(opRaw)
  result.table = readStrAt(body, off)
  result.pk = readStrAt(body, off)
  result.payload = readStrAt(body, off)

#
# low-level file helpers
#
proc writeExact(f: File, p: pointer, n: int) =
  if n <= 0: return
  let wrote = f.writeBuffer(p, n)
  if wrote != n:
    raise newException(WalError, "WAL write failed")

proc readExact(f: File, p: pointer, n: int): bool =
  if n <= 0: return true
  f.readBuffer(p, n) == n

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

#
# record framing + iteration
#
proc putRecordHeader(s: var string, body: string) =
  s.add(char(RecordTag))
  putU32(s, uint32(body.len))
  putU32(s, crc32(body))

proc putFooter(s: var string, lastLsn: uint64) =
  s.add(char(FooterTag))
  putU64(s, lastLsn)

proc readFrame(f: File, entry: var WalEntry): bool =
  ## Reads the next entry frame, skipping any flush-batch footers in between.
  ## Returns false only on EOF or corruption (a footer is not the end of the file).
  while true:
    var tag: uint8
    if not readExact(f, addr tag, 1): return false
    if tag == FooterTag:
      var lsn: uint64
      if not readU64Le(f, lsn): return false
      continue
    if tag != RecordTag:
      return false
    var len, crc: uint32
    if not readU32Le(f, len): return false
    if not readU32Le(f, crc): return false
    if len > MaxFieldBytes: return false
    var body = newString(int(len))
    if not readExact(f, addr body[0], int(len)): return false
    if crc32(body) != crc:
      raise newException(WalError, "WAL record crc mismatch")
    var off = 0
    entry = decodeEntry(body, off)
    return true

proc computeNextLsn(path: string): uint64 =
  if not fileExists(path):
    return 1'u64

  let f = open(path, fmRead)
  defer: f.close()
  if not readHeader(f):
    raise newException(WalError, "invalid WAL header (expected binary WAL v3)")

  # O(1) fast path: a clean flush leaves a fixed-size `! <u64 lastLsn>` footer
  # as the last 9 bytes of the file.
  let size = getFileSize(f)
  if size >= 9:
    f.setFilePos(size - 9)
    var tag: uint8
    if readExact(f, addr tag, 1) and tag == FooterTag:
      var lsn: uint64
      if readU64Le(f, lsn):
        return lsn + 1'u64

  # fallback: crash/truncation left no usable footer; scan all valid records
  f.setFilePos(WalMagic.len)
  var maxLsn = 0'u64
  var e: WalEntry
  while readFrame(f, e):
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
  Wal(path: walPath, nextLsn: computeNextLsn(walPath))

proc ensureLog(w: var Wal): File =
  ## Lazily opens the persistent log handle on the first flush.
  if w.log == nil:
    w.log = new(LogFile)
    w.log.handle = open(w.path, fmAppend)
  w.log.handle

proc openLog*(w: var Wal) =
  ## Eagerly opens the persistent log handle, positioned at the current end of
  ## the file. Used by lazy-read stores that need `logPos` before the first append.
  discard w.ensureLog()

proc logPos*(w: Wal): int64 =
  ## File offset where the next appended record will be written, or -1 if the
  ## persistent log handle is not yet open. With `sync` appends this is the
  ## record's stable offset.
  if w.log == nil: -1'i64
  else: w.log.handle.getFilePos()

proc flush*(w: var Wal) =
  ## Flushes any pending entries to disk as a single batch, then writes the
  ## batch footer. The log file handle stays open across calls.
  if w.pendingEntries.len == 0:
    return

  let f = w.ensureLog()
  var buf = newStringOfCap(16 * 1024)
  var maxLsn = 0'u64
  let ts = getTime().toUnix()
  for e in w.pendingEntries.mitems:
    e.tsUnix = ts
    if e.lsn > maxLsn:
      maxLsn = e.lsn
    let body = encodeEntry(e)
    buf.putRecordHeader(body)
    buf.add(body)
  buf.putFooter(maxLsn)

  f.write(buf)
  f.flushFile()
  w.pendingEntries.setLen(0)

proc append*(w: var Wal, entry: WalEntry, sync: bool = true): uint64 =
  ## Appends a new entry to the WAL. The entry is assigned the next
  ## LSN and added to the pending entries buffer. Timestamps are stamped
  ## once per flush batch instead of per append.
  var e = entry
  e.lsn = w.nextLsn

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
    raise newException(WalError, "invalid WAL header (expected binary WAL v3)")

  var e: WalEntry
  while readFrame(f, e):
    onEntry(e)

proc readEntryAt*(w: Wal, offset: int64): WalEntry =
  ## Random-access read of a single entry by its record's file offset.
  ## The offset is the record start (as returned by `logPos`); the CRC frame
  ## makes the read self-contained.
  let f = open(w.path, fmRead)
  defer: f.close()
  if not readHeader(f):
    raise newException(WalError, "invalid WAL header (expected binary WAL v3)")
  f.setFilePos(offset)
  if not readFrame(f, result):
    raise newException(WalError, "WAL record not found at offset " & $offset)

proc reset*(w: var Wal) =
  ## Resets the WAL by clearing pending entries and writing a new header. This is typically called
  ## after a checkpoint to start a new WAL segment. Note that this will discard any pending
  ## entries that have not been flushed, so it should be used with caution
  w.pendingEntries.setLen(0)
  if w.log != nil:
    w.log.handle.close()
    w.log = nil
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
        "invalid WAL header (expected binary WAL v3)")
    var e: WalEntry
    while readFrame(f, e):
      yield e

iterator entriesWithOffsets*(w: Wal): (int64, WalEntry) =
  ## An iterator over all entries in the WAL file together with the file offset
  ## of each entry's record. Used by lazy-read stores to build a key -> offset
  ## index during recovery.
  if likely(fileExists(w.path)):

    let f = open(w.path, fmRead)
    defer: f.close()

    if not readHeader(f):
      raise newException(WalError,
        "invalid WAL header (expected binary WAL v3)")
    var e: WalEntry
    while true:
      let off = f.getFilePos()
      if not readFrame(f, e):
        break
      yield (off, e)
