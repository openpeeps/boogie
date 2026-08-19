# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[os, json, strutils, times]

## This module implements a simple Write-Ahead Log (WAL) mechanism for durability and crash recovery
## WAL entries are stored as JSON lines in a .wal file. Each entry includes an LSN (log sequence number),
## timestamp, operation type, table name, primary key, and a payload (e.g. row data or schema).
##
## WAL is an append-only structure stored on disk. This means new changes are simply added to the end of
## the log, and existing entries are never altered or removed.

type
  WalOp* = enum
    ## Defines the type of operation being logged. This can
    ## be extended with more operations as needed
    woCreateTable,
    woDropTable,
    woInsertRow,
    woDeleteRow,
    woUpdateRow

  WalEntry* = object
    ## This is the main structure representing a single WAL entry
    lsn*: uint64
      ## Log Sequence Number, a unique identifier for each log entry
      ## that increases with each new entry
    tsUnix*: int64
      ## Timestamp of the log entry in Unix time (seconds since epoch)
    op*: WalOp
      ## The type of operation (e.g. create table, insert row, etc)
    table*: string
      ## The name of the table affected by this operation
    pk*: string
      ## The primary key of the affected row (if applicable, empty string otherwise)
    payload*: string # JSON/text payload (schema, row data, etc.)

  Wal* = object
    ## The main structure representing the WAL. It holds the path to the
    ## WAL file and the next LSN to be assigned for new entries
    path*: string
      ## The file path of a `.wal` file where log entries are stored
    nextLsn*: uint64
      ## The next Log Sequence Number (LSN) to be assigned to a new log entry.
      ## This is initialized based on the existing entries in the WAL file
      ## to ensure LSNs are increasing correctly
    pendingLines*: seq[string]
      ## A buffer for pending log lines that have not yet been flushed to disk.
      ## This allows for batching multiple log entries together before writing
      ## to the WAL file, which can improve performance by reducing the number
      ## of disk writes.

  WalError* = object of CatchableError

proc opToStr(op: WalOp): string =
  $op

proc strToOp(s: string): WalOp =
  try:
    parseEnum[WalOp](s)
  except ValueError:
    raise newException(WalError, "invalid WAL op: " & s)

proc toJsonNode(e: WalEntry): JsonNode =
  %*{
    "v": 1,
    "lsn": e.lsn,
    "ts": e.tsUnix,
    "op": opToStr(e.op),
    "table": e.table,
    "pk": e.pk,
    "payload": e.payload
  }

proc fromJsonNode(n: JsonNode): WalEntry =
  if n.kind != JObject:
    raise newException(WalError, "invalid WAL record: not an object")

  result.lsn = n["lsn"].getInt.uint64
  result.tsUnix = n["ts"].getInt.int64
  result.op = strToOp(n["op"].getStr())
  result.table = n["table"].getStr()
  result.pk = n["pk"].getStr()
  result.payload = n["payload"].getStr()

proc ensureWalFile(path: string) =
  if not fileExists(path):
    # create empty file
    let f = open(path, fmWrite)
    f.close()

proc computeNextLsn(path: string): uint64 =
  if not fileExists(path):
    return 1'u64

  var maxLsn = 0'u64
  let f = open(path, fmRead)
  defer: f.close()

  for line in f.lines:
    if line.strip.len == 0:
      continue
    try:
      let e = fromJsonNode(parseJson(line))
      if e.lsn > maxLsn:
        maxLsn = e.lsn
    except:
      # stop at first malformed tail record
      break

  maxLsn + 1'u64

proc openWal*(path: string): Wal =
  ## Opens a WAL file and initializes the Wal struct with the next LSN to use
  let path = path.changeFileExt(".wal")
  ensureWalFile(path)
  Wal(path: path, nextLsn: computeNextLsn(path))

proc flush*(w: var Wal) =
  ## Flush buffered WAL lines to disk.
  if w.pendingLines.len == 0:
    return

  let f = open(w.path, fmAppend)
  defer: f.close()

  for line in w.pendingLines:
    f.writeLine(line)
  f.flushFile()

  w.pendingLines.setLen(0)

proc append*(w: var Wal, entry: WalEntry, sync: bool = true): uint64 =
  ## Appends a new entry to the WAL and returns its LSN.
  ## If `sync` is true, the entry is flushed to disk immediately. Otherwise, it is buffered
  var e = entry
  e.lsn = w.nextLsn
  e.tsUnix = getTime().toUnix()

  w.pendingLines.add($toJsonNode(e))
  if sync:
    w.flush()

  inc w.nextLsn
  e.lsn

proc replay*(w: Wal, onEntry: proc(e: WalEntry)) =
  ## Replays WAL entries in order, calling `onEntry` for
  ## each valid entry. Stops at first malformed record
  if not fileExists(w.path):
    return

  let f = open(w.path, fmRead)
  defer: f.close()

  for line in f.lines:
    if line.strip.len == 0:
      continue
    try:
      let e = fromJsonNode(parseJson(line))
      onEntry(e)
    except:
      # stop at first malformed tail record
      break

proc reset*(w: var Wal) =
  ## Resets the WAL by truncating the file and resetting LSN counter
  w.pendingLines.setLen(0)
  let f = open(w.path, fmWrite) # truncates
  f.close()
  w.nextLsn = 1'u64

iterator entries*(w: Wal): WalEntry =
  ## Iterates over all WAL entries in order. Note: this is
  ## not efficient for large WAL files, use replay() instead
  if fileExists(w.path):
    let f = open(w.path, fmRead)
    defer: f.close()

    for line in f.lines:
      if line.strip.len == 0:
        continue
      try:
        yield fromJsonNode(parseJson(line))
      except:
        # stop at first malformed tail record
        break
