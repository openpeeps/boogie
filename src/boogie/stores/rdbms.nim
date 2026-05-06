# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[tables, options, strformat,
            json, strutils, os, sets]

import pkg/[flatty, sorta]
import ../wal

## This module implements a simple WAL-based embedded database for Nim.
## It provides a Store type that manages multiple tables, each with a defined schema and supports 
## basic operations like creating tables, inserting rows, deleting rows, and querying rows.
## 
## The Store can be configured to use either in-memory storage (for testing or ephemeral data)
## or disk-based storage with optional write-ahead logging (WAL) for durability.
## 
## Boogie is using BTrees and HashTables for efficient data storage and retrieval.
## The WAL is designed to support efficient group commits and to allow for recovery
## of the store state in case of crashes. 

type
  StorageMode* = enum
    ## Storage mode determines how data is stored and accessed:
    ##
    ## - `smInMemory` keeps everything in RAM (fast but non-persistent)
    ## - `smDisk` mode writes data to disk (persistent but slower)
    smInMemory, smDisk

  DataType* = enum
    dtNull, dtInt, dtFloat, dtBool, dtText, dtJson

  ColumnDef* = object
    name*: string
      ## Name of the column
    kind*: DataType
      ## Data type of the column (int, float, bool, text, json)
    nullable*: bool
      ## Whether the column can contain null values. If false, a value must be provided for this column on insert.

  Value* = object
    ## Value representing a cell in a table row
    case kind*: DataType
    of dtInt:
      i*: int64
    of dtFloat:
      f*: float64
    of dtBool:
      b*: bool
    of dtText:
      s*: string
    of dtJson:
      j*: string
        ## For JSON, we store the raw JSON string. This allows us to
        ## preserve formatting and avoid double-encoding issues.
    of dtNull:
      discard

  RowData* = OrderedTable[string, Value]
    ## A mapping of column names to cell values representing a single row in a table

  RowRecord = object
    # Internal repr of a row, stored in the order index
    pk: string
    cols: RowData

  RowIndex* = SortedTable[RowRecord, string]
    ## An ordered index of rows in a table, keyed by primary key value. This allows for efficient
    ## ordered iteration and range queries.

  PrimaryKeyMode* = enum
    pkmManual, pkmSerial

  DbTable* {.acyclic.} = ref object
    ## Represents a table in the store, including its schema and data.
    name*: string
      ## Name of the table
    primaryKey*: string
      # Name of the primary key column
    case pkType: PrimaryKeyMode
    of pkmSerial:
      pkSequence: uint64
        # Sequence number for auto-incrementing primary keys. This is only used if pkType is pkmSerial.
    else: discard
    columns*: seq[ColumnDef]
      ## Schema definition of the table columns
    columnsByName: OrderedTable[string, ColumnDef]
      # Helper for looking up column definitions by name
    rows: RowIndex
      # ordered index of rows for efficient iteration and range queries
    rowsByPk: OrderedTable[string, RowRecord]
      # hash index of rows by primary key for fast lookups and mutations
    orderIndexDirty: bool
      # flag indicating whether the order index needs to be rebuilt.
      # This is set to true on mutations and cleared when the index is rebuilt.
    indexedCols: HashSet[string]
      # set of column names that have equality indexes built
    eqIndex: Table[string, Table[string, HashSet[string]]]
      # equality indexes for columns. Maps column name to a mapping of cell value keys to sets of primary keys.

  Store* {.acyclic.} = ref object
    ## Represents the main store, containing multiple tables and managing persistence and WAL.
    tables: tables.Table[string, DbTable]
      # mapping of table names to DbTable objects representing the tables in the store
    storageMode: StorageMode
      # the storage mode of the store (in-memory or disk)
    hasWal: bool
      # indicates whether WAL is enabled for this store
    wal: Wal
      # the WAL object for managing write-ahead logging (only used if hasWal is true)
    hasDbFile: bool
      # indicates whether this store uses a .db file for snapshots (only relevant in disk mode)
    dbPath: string
      # the file path for the .db snapshot file (only relevant if hasDbFile is true)
    checkpointLsn: uint64
      # the LSN up to which the store has been checkpointed to disk.
      # This is used to determine which WAL entries can be safely
      # discarded after a checkpoint
    pendingOps: uint32
      # number of mutation operations since the last checkpoint. This is used to trigger
      # automatic checkpoints after a certain number of operations, as configured by
      # `checkpointEveryOps`
    checkpointEveryOps: uint32
      # the number of mutation operations after which an automatic checkpoint
      # should be triggered. This is only relevant if hasDbFile is true
    walFlushEveryOps: uint32
      # the number of mutation operations after which the WAL should be flushed to disk.
      # This is only relevant if hasWal is true
    pendingWalOps: uint32
      # number of mutation operations since the last WAL flush. This is used to trigger
      # automatic WAL flushes after a certain number of operations, as configured by
      # `walFlushEveryOps`

  StoreError* = object of CatchableError

# fwd declarations
proc recoverFromWal*(s: Store)

proc cmp(a, b: RowRecord): int = cmp(a.pk, b.pk)
proc extract(r: RowRecord): string = r.pk

proc `==`*(a, b: RowRecord): bool =
  ## Equality comp for RowRecord. Used by the SortedTable to determine
  ## if two records are the same. We consider two records equal if
  ## their primary keys are equal, since the primary key is the unique
  ## identifier for a row.
  ## 
  ## This allows us to update a row by deleting the old record and
  ## inserting a new one with the same primary key.
  a.pk == b.pk

proc `==`*(a, b: Value): bool =
  ## Equality comparison for Value. This is used for
  ## validating updates and for equality indexes.
  if a.kind != b.kind: return
  case a.kind
  of dtNull:
    true
  of dtInt:
    a.i == b.i
  of dtFloat:
    a.f == b.f
  of dtBool:
    a.b == b.b
  of dtText:
    a.s == b.s
  of dtJson:
    a.j == b.j

proc writeTextAtomic(path, content: string) =
  # Atomically write text content to a file by writing to a temp file and renaming it
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc newStore*(path: string, mode: StorageMode = smDisk,
    enableWal: bool = true, checkpointEveryOps: uint32 = 0'u32,
    walFlushEveryOps: uint32 = 1000'u32
  ): Store =
  ## Create a new Store instance. Use `smInMemory` for an in-memory
  ## store (no persistence) or `smDisk` for a disk-backed store.
  ## 
  ## walFlushEveryOps:
  ##   - 1    => flush every op (old behavior / strongest durability)
  ##   - 1000 => group commit (faster inserts)
  ##   - 0    => flush only on checkpoint/close/recovery-end
  var
    dbPath: string
    hasDb: bool
    hasWal: bool
    walObj: Wal
  
  case mode
  of smInMemory:
    discard
  of smDisk:
    if path.len == 0:
      raise newException(StoreError, "path cannot be empty in disk mode")
    hasDb = true
    dbPath = path.changeFileExt(".db")
    if enableWal:
      hasWal = true
      walObj = openWal(path)

  result = Store(
    storageMode: mode,
    hasWal: hasWal,
    wal: walObj,
    hasDbFile: hasDb,
    dbPath: dbPath,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps,
  )
  # If WAL is enabled, we need to recover the store state by replaying the WAL entries.
  recoverFromWal(result)

proc newInMemoryStore*(): Store =
  ## Create a new in-memory store (no persistence, no WAL)
  newStore("", smInMemory, false)

proc newTable*(name: string, primaryKey: string, columns: openArray[ColumnDef],
              primaryKeyMode: PrimaryKeyMode = pkmSerial): DbTable =
  ## Create a new DbTable with the specified name, primary key, columns, and primary key mode.
  if name.len == 0:
    raise newException(StoreError, "table name cannot be empty")
  if primaryKey.len == 0:
    raise newException(StoreError, "primary key name cannot be empty")

  var seen: seq[string]
  for c in columns:
    if c.name.len == 0:
      raise newException(StoreError, "column name cannot be empty")
    if seen.contains(c.name):
      raise newException(StoreError, fmt"duplicate column: {c.name}")
    seen.add(c.name)

  let colsSeq = @columns
  var colsByName = initOrderedTable[string, ColumnDef](colsSeq.len)
  for c in colsSeq:
    colsByName[c.name] = c

  if primaryKeyMode == pkmSerial:
    let pkCol = block:
      var found = false
      var col: ColumnDef
      for c in columns:
        if c.name == primaryKey:
          found = true
          col = c
          break
      if not found:
        raise newException(StoreError, "serial primary key column not found in table schema")
      col
    if pkCol.kind != dtInt:
      raise newException(StoreError, "serial primary key column must be dtInt")
    if pkCol.nullable:
      raise newException(StoreError, "serial primary key column cannot be nullable")

  if primaryKeyMode == pkmSerial:
    DbTable(
      name: name,
      primaryKey: primaryKey,
      pkType: pkmSerial,
      pkSequence: 0'u64,
      columns: colsSeq,
      columnsByName: colsByName,
      rows: initSortedTable[RowRecord, string](),
    )
  else:
    DbTable(
      name: name,
      primaryKey: primaryKey,
      pkType: pkmManual,
      columns: colsSeq,
      columnsByName: colsByName,
      rows: initSortedTable[RowRecord, string](),
    )

proc newColumn*(name: string, kind: DataType, nullable: bool): ColumnDef =
  ## Create a new ColumnDef with the specified name, data type, and nullability.
  ColumnDef(name: name, kind: kind, nullable: nullable)

#
# Value constructors and helpers
#
proc newNullValue*(): Value = Value(kind: dtNull)
proc newIntValue*(v: int64): Value = Value(kind: dtInt, i: v)
proc newFloatValue*(v: float64): Value = Value(kind: dtFloat, f: v)
proc newBoolValue*(v: bool): Value = Value(kind: dtBool, b: v)
proc newTextValue*(v: string): Value = Value(kind: dtText, s: v)
proc newJSONValue*(v: JsonNode): Value = Value(kind: dtJson, j: $(v))

proc `$`*(v: Value): string =
  ## Stringified representation of a Value
  case v.kind
  of dtNull: "null"
  of dtInt: $v.i
  of dtFloat: $v.f
  of dtBool: $v.b
  of dtText: v.s
  of dtJson: v.j

proc row*(pairs: openArray[(string, Value)]): RowData =
  ## Helper to create RowData from an open array of (column, value) pairs.
  for (k, v) in pairs:
    result[k] = v

proc hasTable*(s: Store, name: string): bool =
  ## Check if the store has a table with the given name.
  s.tables.hasKey(name)

proc findColumn(t: DbTable, colName: string): Option[ColumnDef] =
  # Returns the ColumnDef for the given column name, or none if not found
  if t.columnsByName.hasKey(colName):
    some(t.columnsByName[colName])
  else:
    none(ColumnDef)

proc cellIndexKey(v: Value): string =
  # generate a string key for a cell value for use in equality indexes
  case v.kind
  of dtNull: "n:"
  of dtInt: "i:" & $v.i
  of dtFloat: "f:" & $v.f
  of dtBool: "b:" & (if v.b: "1" else: "0")
  of dtText: "t:" & v.s
  of dtJson: "j:" & v.j

proc addToEqIndexes(t: DbTable, pk: string, data: RowData) =
  # Update equality indexes for the given row data. This should be
  # called on insert and update. 
  for col in t.indexedCols.items:
    if data.hasKey(col):
      let k = cellIndexKey(data[col])
      var colMap = t.eqIndex.mgetOrPut(col, initTable[string, HashSet[string]]())
      var pkSet = colMap.mgetOrPut(k, initHashSet[string]())
      pkSet.incl(pk)

proc removeFromEqIndexes(t: DbTable, pk: string, data: RowData) =
  for col in t.indexedCols.items:
    if data.hasKey(col) and t.eqIndex.hasKey(col):
      let k = cellIndexKey(data[col])

      if t.eqIndex[col].hasKey(k):
        t.eqIndex[col][k].excl(pk)
        if t.eqIndex[col][k].len == 0:
          t.eqIndex[col].del(k)

      if t.eqIndex[col].len == 0:
        t.eqIndex.del(col)

proc createIndex*(t: DbTable, column: string) =
  ## Create an equality index on the specified column
  if t.findColumn(column).isNone:
    raise newException(StoreError, fmt"unknown column '{column}' in table '{t.name}'")
  if t.indexedCols.contains(column):
    return

  var colMap = initTable[string, HashSet[string]]()
  for pk, rec in t.rowsByPk.pairs:
    if rec.cols.hasKey(column):
      let k = cellIndexKey(rec.cols[column])
      var pkSet = colMap.mgetOrPut(k, initHashSet[string]())
      pkSet.incl(pk)

  t.eqIndex[column] = colMap
  t.indexedCols.incl(column)

proc effectivePkForInsert(t: DbTable, pk: string): string =
  case t.pkType
  of pkmManual:
    result = pk
  of pkmSerial:
    if pk.len > 0:
      # allow explicit PK override
      result = pk
    else:
      inc t.pkSequence
      result = $t.pkSequence

proc normalizedRowWithPk(t: DbTable, data: RowData, pk: string): RowData =
  ## Returns a copy of `data` with the primary key column set to `pk`.
  result = data
  let pkCol = t.primaryKey
  let pkDef = t.findColumn(pkCol)
  if pkDef.isNone:
    raise newException(StoreError, "primary key column not found in table schema")
  case pkDef.get.kind
  of dtInt:
    # Try to parse pk as int
    if not result.hasKey(pkCol):
      result[pkCol] = newIntValue(pk.parseBiggestInt.int64)
  of dtText:
    if not result.hasKey(pkCol):
      result[pkCol] = newTextValue(pk)
  else:
    # Add more types as needed
    if not result.hasKey(pkCol):
      result[pkCol] = newTextValue(pk)

proc matchesType(v: Value, c: ColumnDef): bool =
  case v.kind
  of dtNull: c.nullable
  of dtInt: c.kind == dtInt
  of dtFloat: c.kind == dtFloat
  of dtBool: c.kind == dtBool
  of dtText: c.kind == dtText
  of dtJson: c.kind == dtJson

proc validateRow(t: DbTable, data: RowData) =
  # Unknown columns
  for colName, val in data.pairs:
    let c = t.findColumn(colName)
    if c.isNone:
      raise newException(StoreError, fmt"unknown column '{colName}' in table '{t.name}'")
    if not matchesType(val, c.get):
      raise newException(
        StoreError,
        fmt"type mismatch for column '{colName}' in table '{t.name}'"
      )

  # Missing required columns
  for c in t.columns:
    if (not c.nullable) and (not data.hasKey(c.name)):
      raise newException(
        StoreError,
        fmt"missing non-null column '{c.name}' in table '{t.name}'"
      )

#
# JSON serialization helpers (for WAL payloads)
#
proc cellToJson(v: Value): JsonNode =
  case v.kind
  of dtNull: %*{"k": "null"}
  of dtInt: %*{"k": "int", "v": v.i}
  of dtFloat: %*{"k": "float", "v": v.f}
  of dtBool: %*{"k": "bool", "v": v.b}
  of dtText: %*{"k": "text", "v": v.s}
  of dtJson: %*{"k": "json", "v": v.j}

proc cellFromJson(n: JsonNode): Value =
  let k = n["k"].getStr()
  case k
  of "null": newNullValue()
  of "int": newIntValue(n["v"].getBiggestInt.int64)
  of "float": newFloatValue(n["v"].getFloat)
  of "bool": newBoolValue(n["v"].getBool)
  of "text": newTextValue(n["v"].getStr)
  of "json":
    # stored as raw JSON string
    Value(kind: dtJson, j: n["v"].getStr)
  else:
    raise newException(StoreError, "invalid cell kind in WAL payload: " & k)

proc rowToJson(data: RowData): JsonNode =
  result = newJObject()
  for k, v in data.pairs:
    result[k] = cellToJson(v)

proc rowFromJson(n: JsonNode): RowData =
  result = initOrderedTable[string, Value]()
  for k, v in n.pairs:
    result[k] = cellFromJson(v)

proc schemaToPayload(t: DbTable): string =
  var cols = newJArray()
  for c in t.columns:
    cols.add(%*{"name": c.name, "kind": $c.kind, "nullable": c.nullable})
  var payload = %*{
    "primaryKeyMode": $t.pkType,
    "primaryKey": t.primaryKey,
    "columns": cols
  }
  if t.pkType == pkmSerial:
    payload["pkSequence"] = %t.pkSequence
  result = $payload

proc tableFromPayload(tableName, payload: string): DbTable =
  let n = parseJson(payload)
  var cols: seq[ColumnDef] = @[]
  for c in n["columns"].items:
    cols.add(ColumnDef(
      name: c["name"].getStr(),
      kind: parseEnum[DataType](c["kind"].getStr()),
      nullable: c["nullable"].getBool()
    ))
  let pkm =
    if n.hasKey("primaryKeyMode"):
      parseEnum[PrimaryKeyMode](n["primaryKeyMode"].getStr())
    else: pkmManual
  var t = newTable(tableName, n["primaryKey"].getStr(), cols, pkm)
  if pkm == pkmSerial and n.hasKey("pkSequence"):
    t.pkSequence = n["pkSequence"].getInt.uint64
  t

proc rowToPayload(data: RowData): string =
  # Convert RowData to a JSON string for use as a WAL payload.
  # This will be parsed back by `rowFromPayload`.
  $(rowToJson(data))

proc rowFromPayload(payload: string): RowData =
  # Parse a JSON string payload back into RowData. This should be the inverse of `rowToPayload`.
  rowFromJson(parseJson(payload))

#
# internal mutators. no WAL write
#
proc createTableNoWal(s: Store, t: DbTable) =
  if s.tables.hasKey(t.name):
    raise newException(StoreError, fmt"table already exists: {t.name}")
  s.tables[t.name] = t

proc dropTableNoWal(s: Store, name: string) =
  # Drop the specified table from the store without writing to WAL. This is
  # used by the store-level `dropTable` proc which handles WAL logging and commit.
  if not s.tables.hasKey(name):
    raise newException(StoreError, fmt"table not found: {name}")
  s.tables.del(name)

proc ensureOrderIndex(t: DbTable) =
  # Ensure the order index is built and up to date. This should
  # be called before any operation that requires ordered access to rows.
  if not t.orderIndexDirty:
    return
  t.rows = initSortedTable[RowRecord, string]()
  for _, rec in t.rowsByPk.pairs:
    t.rows[rec] = rec.pk
  t.orderIndexDirty = false

proc insertRowNoWal(t: DbTable, pk: string, data: RowData): string =
  # Insert a row into the table without writing to WAL. This is used
  # by the store-level `insertRow` proc which handles WAL logging and commit.
  let generated = (t.pkType == pkmSerial and pk.len == 0)
  let effectivePk = t.effectivePkForInsert(pk)
  if effectivePk.len == 0:
    raise newException(StoreError, "primary key value cannot be empty")

  if t.rowsByPk.hasKey(effectivePk):
    raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")

  let normalized = t.normalizedRowWithPk(data, effectivePk)
  validateRow(t, normalized)

  let rec = RowRecord(pk: effectivePk, cols: normalized)
  t.rowsByPk[effectivePk] = rec
  t.addToEqIndexes(effectivePk, normalized) # NEW
  t.orderIndexDirty = true

  # avoid parseInt on hot path for auto-serial inserts
  if t.pkType == pkmSerial and not generated:
    let n = parseBiggestUInt(effectivePk)
    if n.uint64 > t.pkSequence:
      t.pkSequence = n.uint64

  result = effectivePk

proc deleteRowNoWal(t: DbTable, pk: string): bool =
  # Delete a row from the table without writing to WAL. This is used
  # by the store-level `deleteRow` proc which handles WAL logging and commit
  if not t.rowsByPk.hasKey(pk):
    return false
  let rec = t.rowsByPk[pk]
  t.removeFromEqIndexes(pk, rec.cols)       # NEW
  t.rowsByPk.del(pk)
  t.orderIndexDirty = true
  true

#
# Snapshot API
#
type
  SnapshotOnDisk = tuple
    version: uint32
    checkpointLsn: uint64
    tables: seq[tuple[
      name: string,
      primaryKey: string,
      pkType: PrimaryKeyMode,
      pkSequence: uint64,
      columns: seq[ColumnDef],
      rows: seq[(string, RowData)]
    ]]

proc buildSnapshot(s: Store): SnapshotOnDisk =
  # Build a snapshot of the current store state for persistence.
  # This captures the full state of the store.
  result.version = 1'u32
  result.checkpointLsn = s.checkpointLsn
  for _, t in s.tables.pairs:
    var rows: seq[(string, RowData)] = @[]

    # IMPORTANT: snapshot from rowsByPk (source of truth during writes),
    # not from t.rows (which may be stale/empty while orderIndexDirty=true).
    for pk, rec in t.rowsByPk.pairs:
      rows.add((pk, rec.cols))

    result.tables.add((
      name: t.name,
      primaryKey: t.primaryKey,
      pkType: t.pkType,
      pkSequence: (if t.pkType == pkmSerial: t.pkSequence else: 0'u64),
      columns: t.columns,
      rows: rows
    ))

proc loadSnapshotIntoStore(s: Store, snap: SnapshotOnDisk) =
  # Load the given snapshot into the store state. This will
  # replace any existing state in the store.
  if snap.version != 1'u32:
    raise newException(StoreError, "unsupported .db snapshot version")
  s.tables = initTable[string, DbTable]()
  s.checkpointLsn = snap.checkpointLsn
  for td in snap.tables:
    var t = newTable(td.name, td.primaryKey, td.columns, td.pkType)
    if td.pkType == pkmSerial:
      t.pkSequence = td.pkSequence
    for (pk, data) in td.rows:
      discard t.insertRowNoWal(pk, data)
    s.tables[t.name] = t

proc saveSnapshotIfEnabled(s: Store) =
  # When enabled, saves a snapshot of the current store state to disk.
  if not s.hasDbFile:
    return
  let blob = toFlatty(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: Store) =
  # Load a snapshot from disk if it exists. This should be called during store
  # initialization before applying WAL entries.
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = fromFlatty(blob, SnapshotOnDisk)
  s.loadSnapshotIntoStore(snap)

proc flushWalIfNeeded(s: Store, force = false) =
  # Flush WAL to disk based on group-commit policy.
  if not s.hasWal:
    return
  if force:
    s.wal.flush()
    s.pendingWalOps = 0'u32
    return

  # 0 means "never auto flush" (only flush on checkpoint/shutdown)
  if s.walFlushEveryOps == 0'u32: return

  if s.pendingWalOps >= s.walFlushEveryOps:
    s.wal.flush()
    s.pendingWalOps = 0'u32

proc appendWalIfEnabled(s: Store, op: WalOp, table, pk, payload: string): uint64 =
  # Append a WAL entry if WAL is enabled. Returns the LSN of the appended entry, or 0 if no WAL.
  if not s.hasWal: return

  # Group-commit path - append without immediate sync
  let lsn = s.wal.append(
    WalEntry(op: op, table: table, pk: pk, payload: payload),
    sync = false
  )

  inc s.pendingWalOps
  s.flushWalIfNeeded(force = false)
  lsn

proc markCommitted(s: Store, lsn: uint64) =
  # Update checkpointLsn to the given LSN. This should be called after applying the changes
  # associated with the WAL entry with the given LSN. This allows the checkpointing
  if lsn > s.checkpointLsn:
    s.checkpointLsn = lsn
  
  # Decouple commit from checkpoint:
  # snapshot only after > checkpointEveryOps mutations.
  if s.hasDbFile and s.checkpointEveryOps > 0'u32:
    inc s.pendingOps
    if s.pendingOps >= s.checkpointEveryOps:
      s.saveSnapshotIfEnabled()
      s.pendingOps = 0'u32

proc checkpoint*(s: Store) =
  ## Force a snapshot checkpoint now.
  if not s.hasDbFile: return
  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32

#
# Public API (WAL + apply)
#
proc getTable*(s: Store, name: string): Option[DbTable] =
  ## Get a table by name. Returns none if not found.
  if s.tables.hasKey(name):
    some(s.tables[name]) else: none(DbTable)

proc createTable*(s: Store, t: DbTable) =
  ## Create a new table in the store. This will write to the WAL and commit the transaction.
  let lsn = s.appendWalIfEnabled(woCreateTable, t.name, "", schemaToPayload(t))
  s.createTableNoWal(t)
  s.markCommitted(lsn)

proc dropTable*(s: Store, name: string) =
  ## Drop a table from the store. This will write to the WAL and commit the transaction.
  let lsn = s.appendWalIfEnabled(woDropTable, name, "", "")
  s.dropTableNoWal(name)
  s.markCommitted(lsn)

proc isEmpty*(t: DbTable): bool =
  ## Check if the table is empty (has no rows).
  t.rowsByPk.len == 0

proc insertRow*(t: DbTable, pk: string, data: RowData) =
  # direct table mutation: no WAL here (store-level proc logs)
  discard t.insertRowNoWal(pk, data)

proc deleteRow*(t: DbTable, pk: string): bool =
  # direct table mutation: no WAL here (store-level proc logs)
  t.deleteRowNoWal(pk)

proc getRow*(t: DbTable, pk: string): Option[RowData] =
  ## Fetch a single row by primary key. This will be fast if the table has
  ## an order index, but will fall back to a hash lookup if not
  if t.rowsByPk.hasKey(pk):
    some(t.rowsByPk[pk].cols)
  else:
    none(RowData)

iterator allRows*(t: DbTable): (string, RowData) =
  ## Iterate over all rows in the table in primary key order. This will be fast
  ## if the order index is clean, but may be slower if the index needs to be rebuilt.
  t.ensureOrderIndex()
  for rec in t.rows.keys:
    yield (rec.pk, rec.cols)

iterator allRowsByPk*(t: DbTable): (string, RowData) =
  ## Iterate over all rows in the table using the hash index (unsorted).
  for pk, rec in t.rowsByPk.pairs:
    yield (pk, rec.cols)

proc insertRow*(s: Store, tableName: string, pk: string, data: RowData) =
  ## Insert a row into the specified table with the given primary key and data. This will
  ## write to the WAL and commit the transaction. The primary key can be empty for tables with
  ## serial PK mode, in which case it will be auto-generated
  if unlikely(not s.tables.hasKey(tableName)):
    raise newException(StoreError, fmt"table not found: {tableName}")
  var t = s.tables[tableName]
  let effectivePk = t.effectivePkForInsert(pk)
  let lsn = s.appendWalIfEnabled(woInsertRow, tableName, effectivePk, rowToPayload(data))
  discard t.insertRowNoWal(effectivePk, data)
  s.markCommitted(lsn)

proc insertRow*(t: DbTable, data: RowData): string =
  ## direct table mutation: no WAL here (store-level proc logs)
  if t.pkType != pkmSerial:
    raise newException(StoreError, "insertRow(data) requires a serial primary key table")
  result = t.insertRowNoWal("", data)

proc insertRow*(s: Store, tableName: string, data: RowData): string {.discardable.}=
  if unlikely(not s.tables.hasKey(tableName)):
    raise newException(StoreError, fmt"table not found: {tableName}")

  var t = s.tables[tableName]
  if t.pkType != pkmSerial:
    raise newException(StoreError, "insertRow(tableName, data) requires a serial primary key table")

  let effectivePk = t.effectivePkForInsert("")
  let lsn = s.appendWalIfEnabled(woInsertRow, tableName, effectivePk, rowToPayload(data))
  discard t.insertRowNoWal(effectivePk, data)
  s.tables[tableName] = t
  s.markCommitted(lsn)
  result = effectivePk

proc deleteRow*(s: Store, tableName: string, pk: string): bool =
  ## Delete a row by primary key. Returns true if a row was deleted, false if not found
  if unlikely(not s.tables.hasKey(tableName)):
    return false
  let lsn = s.appendWalIfEnabled(woDeleteRow, tableName, pk, "")
  var t = s.tables[tableName]
  let removed = t.deleteRowNoWal(pk)
  s.tables[tableName] = t
  s.markCommitted(lsn)
  removed

proc getRow*(s: Store, tableName: string, pk: string): Option[RowData] =
  ## Fetch a single row by primary key. This will be fast if the table has
  ## an order index, but will fall back to a hash lookup if not.
  if unlikely(not s.tables.hasKey(tableName)):
    return none(RowData)
  s.tables[tableName].getRow(pk)

#
# SQL Query-like API
#
proc where*(t: DbTable, column: string, value: Value): seq[(string, RowData)] =
  ## Return all rows where the given column matches the specified value. This will be
  ## fast if the column is indexed, but will fall back to a full scan if not.
  if t.indexedCols.contains(column) and t.eqIndex.hasKey(column):
    let k = cellIndexKey(value)
    let colMap = t.eqIndex[column]
    if colMap.hasKey(k):
      for pk in colMap[k].items:
        if t.rowsByPk.hasKey(pk):
          result.add((pk, t.rowsByPk[pk].cols))
    return

  # fallback unsorted hash scan (faster than allRows ordered walk/rebuild)
  for pk, rec in t.rowsByPk.pairs:
    if rec.cols.hasKey(column) and rec.cols[column] == value:
      result.add((pk, rec.cols))

#
# WAL application and recovery
#
proc applyWalEntry(s: Store, e: WalEntry) =
  # Apply a single WAL entry to the store state. This is used during recovery to replay
  # operations from the WAL after loading a snapshot.
  case e.op
  of woCreateTable:
    s.createTableNoWal(tableFromPayload(e.table, e.payload))
  of woDropTable:
    s.dropTableNoWal(e.table)
  of woInsertRow:
    if unlikely(not s.tables.hasKey(e.table)):
      raise newException(StoreError, "WAL replay: table not found: " & e.table)
    var t = s.tables[e.table]
    discard t.insertRowNoWal(e.pk, rowFromPayload(e.payload))
    s.tables[e.table] = t
  of woDeleteRow:
    if likely(s.tables.hasKey(e.table)):
      var t = s.tables[e.table]
      discard t.deleteRowNoWal(e.pk)
      s.tables[e.table] = t
  of woUpdateRow:
    raise newException(StoreError, "WAL replay: woUpdateRow not implemented")

proc recoverFromWal*(s: Store) =
  ## Load snapshot if present, then apply WAL entries to bring state up to date.
  s.tables = initTable[string, DbTable]()
  s.checkpointLsn = 0'u64
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32
  # First load the snapshot (if it exists) to get the base state.
  # Then apply WAL entries that are newer than the checkpointLsn.
  s.loadSnapshotIfPresent()

  if s.hasWal:
    for e in s.wal.entries:
      if e.lsn <= s.checkpointLsn:
        continue
      s.applyWalEntry(e)
      s.checkpointLsn = e.lsn
 
  # After recovery, flush WAL and checkpoint to ensure
  # a clean state on disk with no pending WAL entries.
  s.flushWalIfNeeded(force = true)

  # Only checkpoint if we loaded a snapshot or applied WAL entries.
  # If neither happened, we can skip the snapshot write and just reset pendingOps.
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32
  s.pendingWalOps = 0'u32