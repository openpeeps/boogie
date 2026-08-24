# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[tables, options, os, times, strutils]

import ../wal
import ../crashsafe
export wal

## This module implements an append-only log store optimized for sequential access.
## Data is organized into named streams. Every appended record is assigned a dense,
## monotonically increasing sequence number and a Unix timestamp, and records are
## immutable once written. This makes the store a natural foundation for higher level
## abstractions such as command history (do/undo stacks, via `backward`/`last`) or
## time-series databases (via `rangeByTime`).
##
## Unlike the other Boogie stores there is no snapshot file: the WAL itself is the
## primary data. Only a sparse in-memory index is retained (one file offset per
## sequence number plus a timestamp-sorted view), so the memory footprint stays flat
## regardless of history size. Records are read back through a persistent file handle,
## and a small LRU cache absorbs repeated access to recent records.
##
## Durability follows the house policy: appends are visible immediately, WAL writes
## are batched into group commits (`walFlushEveryOps`, or `sync = true` to force),
## and a best-effort flush runs on exit/fatal signals. Because the log file is
## append-only, recovery is a single index-rebuilding scan; there are no checkpoints.
##
## The main types are:
## - `LogRecord`: an immutable record (sequence number, timestamp, opaque payload)
## - `LogStore`: the main store object managing one or more named streams
##
## Note: single-threaded like the other non-concurrent stores. Sequence numbers are
## dense per stream, which the offset index and `rangeScan` rely on.

type
  LogRecord* = object
    ## A single immutable record within a stream
    seqNum*: uint64
      ## 1-based position of the record within its stream
    tsUnix*: int64
      ## Unix timestamp in seconds, stamped at append time unless overridden
    payload*: string
      ## Opaque record body. Higher level layers may encode JSON, BSON or any
      ## other format on top

  LogStoreError* = object of CatchableError
    ## Raised on invalid arguments, duplicate streams or layout corruption

  StreamIndex = object
    ## Sparse in-memory index over one named stream. Sequence numbers are dense
    ## per stream, so both views below can be plain arrays indexed by position.
    offsets: seq[int64]
      ## Disk mode only: byte offset of each record inside the WAL file,
      ## indexed by (seqNum - 1). Empty for in-memory stores.
    tsIndex: seq[(int64, uint64)]
      ## (tsUnix, seqNum) pairs kept sorted ascending by timestamp, enabling
      ## binary-searched window scans even with out-of-order timestamps

  CacheKey = tuple[stream: string, seqNum: uint64]

  CacheNode {.acyclic.} = ref object
    ## Doubly-linked list node for the LRU cache. `acyclic` breaks the
    ## prev/next ref cycles under ARC/ORC; lifetimes are managed explicitly.
    key: CacheKey
    rec: LogRecord
    prev, next: CacheNode

  LruCache = ref object
    ## Tiny LRU cache over decoded records, keyed by (stream, seqNum).
    ## Absorbs repeated reads around a traversal cursor (undo stacks) without
    ## keeping whole streams resident. Nil when caching is disabled.
    cap: int
    map: Table[CacheKey, CacheNode]
    head, tail: CacheNode
      ## head is the most recently used node, tail the least recently used

  LogStore* = ref object
    ## An append-only log store holding one or more named streams
    name*: string
      ## Logical name of the store, used as the base name for its files
    wal: Wal
      ## Disk mode only: the append-only log that IS the primary data
    reader: LogReader
      ## Persistent read handle over the WAL, avoids open/close per record
    streams: Table[string, StreamIndex]
    memRecords: Table[string, seq[LogRecord]]
      ## In-memory mode only: full records resident in RAM
    hasDisk: bool
    walFlushEveryOps: uint32
    pendingWalOps: uint32
    cursor: int64
      ## File offset where the next appended record will land. Tracked
      ## arithmetically because the underlying file position only advances on
      ## flush; each verified flush absorbs its footer into the cursor.
    durableUpTo: int64
      ## File offset of the durable prefix of the log. Records at or beyond
      ## this point exist only in the group-commit buffer and are served from
      ## there until the next flush lands them on disk.
    cache: LruCache

const FooterSize = 9'i64
  ## Size of the `<tag> <u64 lastLsn>` footer every WAL flush appends

proc fail(msg: string) {.noreturn.} =
  raise newException(LogStoreError, msg)

#
# payload framing: `<u64 LE tsUnix> <user payload>`, so precise per-record
# timestamps survive independent of the WAL's flush-batch stamping
#
proc framePayload(tsUnix: int64, payload: string): string =
  let t = cast[uint64](tsUnix)
  result = newStringOfCap(8 + payload.len)
  for i in 0 ..< 8:
    result.add(char((t shr (i * 8)) and 0xFF'u64))
  result.add(payload)

proc unframePayload(s: string): (int64, string) =
  if s.len < 8:
    fail("corrupt log record: payload shorter than timestamp prefix")
  var t = 0'u64
  for i in 0 ..< 8:
    t = t or (uint64(ord(s[i])) shl (i * 8))
  (cast[int64](t), s[8 .. ^1])

#
# WAL framing arithmetic: exact on-disk size of one framed record, mirroring
# `encodeEntry` plus the 9-byte record header (tag + u32 len + u32 crc32).
# Lets appends assign their future offset immediately even while the record
# still sits in the group-commit buffer; every flush verifies the running
# cursor against the real file position and fails loudly on any drift.
#
proc framedRecordSize(table, pk, payload: string): int64 =
  let bodyLen = 8 + 8 + 1 +
    (4 + table.len) + (4 + pk.len) + (4 + payload.len)
  int64(9 + bodyLen)

#
# LRU cache
#
proc lruUnlink(c: LruCache, n: CacheNode) =
  if n.prev != nil: n.prev.next = n.next
  else: c.head = n.next
  if n.next != nil: n.next.prev = n.prev
  else: c.tail = n.prev
  n.prev = nil
  n.next = nil

proc lruPushFront(c: LruCache, n: CacheNode) =
  n.prev = nil
  n.next = c.head
  if c.head != nil: c.head.prev = n
  c.head = n
  if c.tail == nil: c.tail = n

proc newLruCache(capacity: int): LruCache =
  if capacity <= 0:
    return nil
  LruCache(cap: capacity, map: initTable[CacheKey, CacheNode]())

proc lruGet(c: LruCache, key: CacheKey): Option[LogRecord] =
  if c != nil and c.map.hasKey(key):
    let n = c.map[key]
    c.lruUnlink(n)
    c.lruPushFront(n)
    result = some(n.rec)
  else:
    result = none(LogRecord)

proc lruPut(c: LruCache, key: CacheKey, rec: LogRecord) =
  if c == nil:
    return
  if c.map.hasKey(key):
    let n = c.map[key]
    n.rec = rec
    c.lruUnlink(n)
    c.lruPushFront(n)
    return
  let n = CacheNode(key: key, rec: rec)
  c.map[key] = n
  c.lruPushFront(n)
  if c.map.len > c.cap:
    let old = c.tail
    c.lruUnlink(old)
    c.map.del(old.key)

#
# internal helpers
#
proc streamLen(s: LogStore, stream: string): int =
  if not s.hasDisk:
    if s.memRecords.hasKey(stream):
      s.memRecords[stream].len
    else:
      0
  elif s.streams.hasKey(stream):
    s.streams[stream].offsets.len
  else:
    0

proc indexTs(s: LogStore, stream: string, seqNum: uint64, tsUnix: int64) =
  ## Registers a record's timestamp in the stream's sorted ts view. Monotonic
  ## stamps (the common case) take the O(1) append fast path; out-of-order
  ## stamps pay a binary search plus insert. Mutates through `addr` because
  ## copying the `StreamIndex` value out of the table would detach the update.
  let slot = addr s.streams.mgetOrPut(stream, StreamIndex())
  if slot[].tsIndex.len == 0 or slot[].tsIndex[^1][0] <= tsUnix:
    slot[].tsIndex.add((tsUnix, seqNum))
    return
  var lo = 0
  var hi = slot[].tsIndex.len
  while lo < hi:
    let mid = (lo + hi) div 2
    if slot[].tsIndex[mid][0] < tsUnix: lo = mid + 1
    else: hi = mid
  slot[].tsIndex.insert((tsUnix, seqNum), lo)

proc pendingRecord(s: LogStore, stream: string, seqNum: uint64): LogRecord =
  ## Resolves a not-yet-flushed record from the group-commit buffer
  let wantPk = $seqNum
  for e in s.wal.pendingEntries:
    if e.table == stream and e.pk == wantPk:
      let (ts, payload) = unframePayload(e.payload)
      return LogRecord(seqNum: seqNum, tsUnix: ts, payload: payload)
  fail("pending record missing from group-commit buffer: " &
       stream & "/" & $seqNum)

proc fetchRecord(s: LogStore, stream: string, seqNum: uint64): LogRecord =
  ## Returns the record at `seqNum`. Assumes the caller has bounds-checked.
  if not s.hasDisk:
    return s.memRecords[stream][int(seqNum) - 1]
  let key = (stream, seqNum)
  let cached = s.cache.lruGet(key)
  if cached.isSome:
    return cached.get
  result =
    if s.streams[stream].offsets[int(seqNum) - 1] < s.durableUpTo:
      let e = s.reader.readAt(s.streams[stream].offsets[int(seqNum) - 1])
      let (ts, payload) = unframePayload(e.payload)
      LogRecord(seqNum: seqNum, tsUnix: ts, payload: payload)
    else:
      s.pendingRecord(stream, seqNum)
  s.cache.lruPut(key, result)

proc flushWalIfNeeded(s: LogStore, force = false) =
  ## Flushes pending group commits and verifies that the arithmetically
  ## tracked append cursor matches the real end of the WAL file (plus the
  ## footer the flush appends). Any drift means the size arithmetic no longer
  ## matches the WAL framing, which is raised loudly rather than silently
  ## serving wrong offsets later. The footer becomes part of the file layout,
  ## so the cursor absorbs it and the durable frontier moves to the new end.
  if not s.hasDisk or s.pendingWalOps == 0'u32:
    return
  if not force and (s.walFlushEveryOps == 0'u32 or
                    s.pendingWalOps < s.walFlushEveryOps):
    return
  let expectedEnd = s.cursor + FooterSize
  s.wal.flush()
  if s.wal.logPos() != expectedEnd:
    fail("log store cursor drift: WAL layout does not match size arithmetic")
  s.cursor = expectedEnd
  s.durableUpTo = expectedEnd
  s.pendingWalOps = 0'u32

proc recoverFromWal*(s: LogStore) =
  ## Rebuilds all stream indexes by scanning the WAL once. Sequence numbers
  ## must be contiguous per stream; gaps indicate truncation or foreign data.
  s.streams = initTable[string, StreamIndex]()
  s.pendingWalOps = 0'u32
  for (off, e) in s.wal.entriesWithOffsets():
    if e.op != woInsertRow:
      fail("unexpected op in log store WAL: " & $e.op)
    var seqNum = 0'u64
    try:
      seqNum = parseBiggestUInt(e.pk)
    except ValueError:
      fail("corrupt log record key: " & e.pk)
    let (ts, _) = unframePayload(e.payload)
    let slot = addr s.streams.mgetOrPut(e.table, StreamIndex())
    if seqNum != uint64(slot[].offsets.len + 1):
      fail("non-contiguous sequence number " & $seqNum & " in stream " & e.table)
    slot[].offsets.add(off)
    s.indexTs(e.table, seqNum, ts)
  if s.hasDisk:
    # everything recovered from disk is durable; appends continue at EOF
    s.cursor = s.wal.logPos()
    s.durableUpTo = s.cursor

#
# construction / lifecycle
#
proc openLogStore*(path: string, name = "logs",
                   walFlushEveryOps: uint32 = 1000'u32,
                   cacheCapacity = 1024): LogStore =
  ## Opens a disk-backed log store at `path`, creating the directory if needed.
  ## If the store was opened before, all stream indexes are rebuilt from the
  ## existing log and subsequent appends continue after the last sequence
  ## number.
  ##
  ## `walFlushEveryOps` controls how many appends may accumulate before a
  ## group commit; `cacheCapacity` bounds the LRU record cache (0 disables it).
  if path.len == 0:
    fail("path cannot be empty")
  if not dirExists(path):
    createDir(path)

  let base = path / name
  result = LogStore(
    name: name,
    wal: openWal(base),
    hasDisk: true,
    walFlushEveryOps: walFlushEveryOps,
    pendingWalOps: 0'u32,
    cache: newLruCache(cacheCapacity)
  )
  openLog(result.wal)
  result.reader = openLogReader(result.wal)
  result.recoverFromWal()

  let store = result
  registerStoreFlush(cast[pointer](store), proc() {.gcsafe.} =
    store.wal.flushNoClear()
  )

proc newInMemoryLogStore*(): LogStore =
  ## Creates a purely in-memory log store with no persistence. All records
  ## stay resident in RAM; no WAL, cache or recovery is involved.
  LogStore(
    name: "",
    hasDisk: false,
    walFlushEveryOps: 0'u32,
    memRecords: initTable[string, seq[LogRecord]](),
    streams: initTable[string, StreamIndex]()
  )

proc checkpoint*(s: LogStore) =
  ## Flushes all pending appends to disk. With no snapshot file, checkpoints
  ## only bound how much of the log remains vulnerable to a crash.
  s.flushWalIfNeeded(force = true)

proc close*(s: LogStore) =
  ## Flushes pending appends, releases file handles and removes the store
  ## from the crash-flush registry.
  unregisterStoreFlush(cast[pointer](s))
  if s.hasDisk:
    try:
      s.flushWalIfNeeded(force = true)
    finally:
      s.reader.close()

#
# stream management
#
proc createStream*(s: LogStore, stream: string) =
  ## Explicitly creates an empty stream. Appends auto-create streams, so this
  ## is only needed to pin down existence up front. Raises if it already exists.
  if stream.len == 0:
    fail("stream name cannot be empty")
  if s.streams.hasKey(stream) or s.memRecords.hasKey(stream):
    fail("stream already exists: " & stream)
  s.streams[stream] = StreamIndex()
  if not s.hasDisk:
    s.memRecords[stream] = @[]

proc hasStream*(s: LogStore, stream: string): bool =
  s.streams.hasKey(stream)

iterator streams*(s: LogStore): string =
  ## Iterates over the names of all streams known to the store
  for streamName in s.streams.keys:
    yield streamName

#
# writes
#
proc append*(s: LogStore, stream: string, payload: string,
             tsUnix = 0'i64, sync = true): uint64 {.discardable.} =
  ## Appends a record to `stream` and returns its assigned sequence number.
  ## Streams are created on first use. Pass `tsUnix = 0` (the default) to
  ## stamp the current Unix time, or supply your own for out-of-band ordering.
  ##
  ## With `sync = true` the WAL is flushed before returning; otherwise the
  ## write joins the current group commit batch (still immediately visible
  ## to reads). Records are immutable once written.
  if stream.len == 0:
    fail("stream name cannot be empty")
  let ts = if tsUnix == 0'i64: getTime().toUnix() else: tsUnix
  let seqNum = uint64(s.streamLen(stream) + 1)

  if not s.hasDisk:
    s.memRecords.mgetOrPut(stream, @[]).add(
      LogRecord(seqNum: seqNum, tsUnix: ts, payload: payload))
    s.indexTs(stream, seqNum, ts)
    return seqNum

  let framed = framePayload(ts, payload)
  let off = s.cursor
  discard s.wal.append(
    WalEntry(op: woInsertRow, table: stream, pk: $seqNum, payload: framed),
    sync = false
  )
  s.streams.mgetOrPut(stream, StreamIndex()).offsets.add(off)
  s.cursor += framedRecordSize(stream, $seqNum, framed)
  s.indexTs(stream, seqNum, ts)
  inc s.pendingWalOps
  if sync:
    s.flushWalIfNeeded(force = true)
  else:
    s.flushWalIfNeeded(force = false)
  seqNum

#
# reads
#
proc len*(s: LogStore, stream: string): int =
  ## Returns the number of records in `stream` (0 for unknown streams)
  s.streamLen(stream)

proc tailSeq*(s: LogStore, stream: string): uint64 =
  ## Returns the newest sequence number in `stream`, or 0 when empty/unknown
  uint64(s.streamLen(stream))

proc firstSeq*(s: LogStore, stream: string): uint64 =
  ## Returns the oldest sequence number in `stream` (always 1), or 0 when
  ## empty/unknown
  if s.streamLen(stream) > 0: 1'u64 else: 0'u64

proc get*(s: LogStore, stream: string, seqNum: uint64): Option[LogRecord] =
  ## Random access by sequence number. Returns `none` when the stream is
  ## unknown or `seqNum` is out of bounds.
  if seqNum == 0'u64 or seqNum > uint64(s.streamLen(stream)):
    return none(LogRecord)
  some(s.fetchRecord(stream, seqNum))

proc last*(s: LogStore, stream: string, n: int): seq[LogRecord] =
  ## Returns up to `n` newest records, newest first. The building block for
  ## cursor-based undo: peek the most recent actions without mutating them.
  if n <= 0:
    return
  let total = s.streamLen(stream)
  let cnt = min(n, total)
  result = newSeqOfCap[LogRecord](cnt)
  for i in countdown(total, total - cnt + 1):
    result.add(s.fetchRecord(stream, uint64(i)))

iterator forward*(s: LogStore, stream: string): LogRecord =
  ## Sequential scan from oldest to newest. Unknown streams yield nothing.
  for i in 1 .. s.streamLen(stream):
    yield s.fetchRecord(stream, uint64(i))

iterator backward*(s: LogStore, stream: string): LogRecord =
  ## Sequential scan from newest to oldest. Unknown streams yield nothing.
  for i in countdown(s.streamLen(stream), 1):
    yield s.fetchRecord(stream, uint64(i))

iterator rangeScan*(s: LogStore, stream: string, fromSeq, toSeq: uint64): LogRecord =
  ## Scan over `[fromSeq, toSeq]` inclusive, oldest to newest. Bounds outside
  ## the stream are clamped; inverted ranges yield nothing.
  let n = s.streamLen(stream)
  if fromSeq <= toSeq and n > 0:
    let lo = max(fromSeq, 1'u64)
    let hi = min(toSeq, uint64(n))
    var i = lo
    while i <= hi:
      yield s.fetchRecord(stream, i)
      inc i

iterator rangeByTime*(s: LogStore, stream: string, fromTs, toTs: int64): LogRecord =
  ## Scan over a timestamp window `[fromTs, toTs]` inclusive, ordered by
  ## timestamp ascending (which equals sequence order for monotonic stamps).
  ## Binary-searches the stream's sorted timestamp view.
  if s.streams.hasKey(stream):
    let tsIdx = s.streams[stream].tsIndex
    var lo = 0
    var hi = tsIdx.len
    while lo < hi:
      let mid = (lo + hi) div 2
      if tsIdx[mid][0] < fromTs: lo = mid + 1
      else: hi = mid
    var i = lo
    while i < tsIdx.len and tsIdx[i][0] <= toTs:
      yield s.fetchRecord(stream, tsIdx[i][1])
      inc i
