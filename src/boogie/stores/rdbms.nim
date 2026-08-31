# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[tables, options, strformat,
            json, strutils, os, sets]

import pkg/sorta
import ../wal
import ../concurrency
import ../crashsafe
import ../fbe_codec
import ../rdbms_types
export rdbms_types

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

  RowRecord = object
    # Internal repr of a row, stored in the order index
    pk: string
    cols: RowData

  RowIndex* = SortedTable[RowRecord, string]
    ## An ordered index of rows in a table, keyed by primary key value. This allows for efficient
    ## ordered iteration and range queries.

  DbTable* = ref object
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
    foreignKeys*: seq[ForeignKeyDef]
      # foreign key constraints owned by this table.
    slot: TableSlot[RdbWriteTask]
      ## Per-table concurrency slot; nil unless the store is concurrent.

  Store* = ref object
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
    cc: ConcurrentState[RdbWriteTask]
      ## Store-level concurrency state; nil unless `enableConcurrency = true`.

  StoreError* = object of CatchableError

  RdbWriteOp = enum
    roInsert, roDelete

  RdbWriteTask = object
    ## Write task carried across threads (value types only, no shared refs).
    kind: RdbWriteOp
    pk: string
    data: RowData

# fwd declarations
proc where*(t: DbTable, column: string, value: Value): seq[(string, RowData)]
proc insertRowNoWal(t: DbTable, pk: string, data: var RowData): string
proc deleteRowNoWal(t: DbTable, pk: string): bool
proc rowToPayload(data: RowData): string

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

proc writeTextAtomic(path, content: string) =
  # Atomically write text content to a file by writing to a temp file and renaming it
  let tmp = path & ".tmp"
  writeFile(tmp, content)
  if fileExists(path):
    removeFile(path)
  moveFile(tmp, path)

proc loadSnapshotIfPresent(s: Store)
proc recoverFromWal*(s: Store)

proc newStore*(path: string, mode: StorageMode = smDisk,
    enableWal: bool = true, checkpointEveryOps: uint32 = 0'u32,
    walFlushEveryOps: uint32 = 1000'u32,
    enableConcurrency: static bool = false
  ): Store =
  ## Create a new Store instance. Use `smInMemory` for an in-memory
  ## store (no persistence) or `smDisk` for a disk-backed store.
  ## 
  ## walFlushEveryOps:
  ##   - 1    => flush every op (old behavior / strongest durability)
  ##   - 1000 => group commit (faster inserts)
  ##   - 0    => flush only on checkpoint/close/recovery-end
  ##
  ## With `enableConcurrency = true` reads are concurrent per table and writes
  ## are serialized per table through a bounded worker pool (WAL-only durability).
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

  when enableConcurrency:
    static: doAssert compileOption("threads"), "concurrency requires --threads:on"
    hasDb = false

  result = Store(
    storageMode: mode,
    hasWal: hasWal,
    wal: walObj,
    hasDbFile: hasDb,
    dbPath: dbPath,
    checkpointEveryOps: checkpointEveryOps,
    walFlushEveryOps: walFlushEveryOps,
  )

  when enableConcurrency:
    result.cc = newConcurrentState[RdbWriteTask](
      proc(ctx: pointer, slot: TableSlot[RdbWriteTask], op: RdbWriteTask) {.gcsafe.} =
        let s = cast[Store](ctx)
        let t = cast[DbTable](slot.owner)
        case op.kind
        of roInsert:
          var d = op.data
          discard t.insertRowNoWal(op.pk, d)
          if s.hasWal:
            s.cc.appendWal(s.wal,
              WalEntry(op: woInsertRow, table: t.name, pk: op.pk, payload: rowToPayload(op.data)),
              int(s.walFlushEveryOps))
        of roDelete:
          discard t.deleteRowNoWal(op.pk)
          if s.hasWal:
            s.cc.appendWal(s.wal,
              WalEntry(op: woDeleteRow, table: t.name, pk: op.pk, payload: ""),
              int(s.walFlushEveryOps))
      ,
      cast[pointer](result)
    )

  # If WAL is enabled, we need to recover the store state by replaying the WAL entries.
  recoverFromWal(result)

  let s = result
  registerStoreFlush(cast[pointer](s), proc() {.gcsafe.} =
    if s.hasWal:
      if s.cc != nil:
        s.cc.flushWal(s.wal, clear = false)
      else:
        s.wal.flushNoClear()
  )

proc newInMemoryStore*(): Store =
  ## Create a new in-memory store (no persistence, no WAL)
  newStore("", smInMemory, false)

proc newTable*(name: string, primaryKey: string, columns: openArray[ColumnDef],
              primaryKeyMode: PrimaryKeyMode = pkmSerial,
              foreignKeys: openArray[ForeignKeyDef] = []): DbTable =
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
      rowsByPk: initOrderedTable[string, RowRecord](),
      indexedCols: initHashSet[string](),
      eqIndex: initTable[string, Table[string, HashSet[string]]](),
      foreignKeys: @foreignKeys,
    )
  else:
    DbTable(
      name: name,
      primaryKey: primaryKey,
      pkType: pkmManual,
      columns: colsSeq,
      columnsByName: colsByName,
      rows: initSortedTable[RowRecord, string](),
      rowsByPk: initOrderedTable[string, RowRecord](),
      indexedCols: initHashSet[string](),
      eqIndex: initTable[string, Table[string, HashSet[string]]](),
      foreignKeys: @foreignKeys,
    )

proc newForeignKey*(name, column, refTable, refColumn: string,
                    onDelete: ForeignKeyAction = fkaRestrict): ForeignKeyDef =
  ## Create a new foreign key definition.
  if name.len == 0:
    raise newException(StoreError, "foreign key name cannot be empty")
  if column.len == 0:
    raise newException(StoreError, "foreign key column cannot be empty")
  if refTable.len == 0:
    raise newException(StoreError, "foreign key referenced table cannot be empty")
  if refColumn.len == 0:
    raise newException(StoreError, "foreign key referenced column cannot be empty")
  ForeignKeyDef(
    name: name,
    column: column,
    refTable: refTable,
    refColumn: refColumn,
    onDelete: onDelete
  )

proc newColumn*(name: string, kind: DataType, nullable: bool): ColumnDef =
  ## Create a new ColumnDef with the specified name, data type, and nullability.
  ColumnDef(name: name, kind: kind, nullable: nullable)

proc row*(pairs: openArray[(string, Value)]): RowData =
  ## Helper to create RowData from an open array of (column, value) pairs.
  for (k, v) in pairs:
    result[k] = v

proc hasTable*(s: Store, name: string): bool =
  ## Check if the store has a table with the given name.
  if s.cc != nil:
    var res = false
    withMetaRead(s.cc):
      res = s.tables.hasKey(name)
    return res
  s.tables.hasKey(name)

proc findColumn*(t: DbTable, colName: string): Option[ColumnDef] =
  # Returns the ColumnDef for the given column name, or none if not found
  if t.columnsByName.hasKey(colName):
    some(t.columnsByName[colName])
  else:
    none(ColumnDef)

proc cellIndexKey(v: Value): string =
  # generate a string key for a cell value for use in equality indexes
  case v.kind
  of dtNull: "n:"
  of dtInt: "i:" & $v.intVal
  of dtFloat: "f:" & $v.floatVal
  of dtBool: "b:" & (if v.boolVal: "1" else: "0")
  of dtText: "t:" & v.strVal
  of dtJson: "j:" & v.jsonVal

proc addToEqIndexes(t: DbTable, pk: string, data: RowData) =
  # Update equality indexes for the given row data.
  for col in t.indexedCols.items:
    if not data.hasKey(col):
      continue
    let k = cellIndexKey(data[col])

    if not t.eqIndex.hasKey(col):
      t.eqIndex[col] = initTable[string, HashSet[string]]()
    if not t.eqIndex[col].hasKey(k):
      t.eqIndex[col][k] = initHashSet[string]()

    t.eqIndex[col][k].incl(pk)

proc removeFromEqIndexes(t: DbTable, pk: string, data: RowData) =
  for col in t.indexedCols.items:
    if not data.hasKey(col):
      continue
    if not t.eqIndex.hasKey(col):
      continue

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
      colMap.mgetOrPut(k, initHashSet[string]()).incl(pk)

  t.eqIndex[column] = colMap
  t.indexedCols.incl(column)

proc jsonToStoreValue*(j: JsonNode): Value {.gcsafe.}
  ## Converts a JSON literal into a store value. Used for column defaults.

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

proc normalizedRowWithPk(t: DbTable, data: var RowData, pk: string) =
  ## Sets the primary key column on `data` to `pk` in place and applies any
  ## declared column defaults for values that were not provided.
  let pkCol = t.primaryKey
  let pkDef = t.findColumn(pkCol)
  if pkDef.isNone:
    raise newException(StoreError, "primary key column not found in table schema")
  case pkDef.get.kind
  of dtInt:
    # Try to parse pk as int
    if not data.hasKey(pkCol):
      data[pkCol] = newIntValue(pk.parseBiggestInt.int64)
  of dtText:
    if not data.hasKey(pkCol):
      data[pkCol] = newTextValue(pk)
  else:
    # Add more types as needed
    if not data.hasKey(pkCol):
      data[pkCol] = newTextValue(pk)
  for c in t.columns:
    if c.defaultValue.len > 0 and not data.hasKey(c.name) and c.name != pkCol:
      data[c.name] = jsonToStoreValue(parseJson(c.defaultValue))

proc jsonToStoreValue*(j: JsonNode): Value {.gcsafe.} =
  ## Converts a JSON literal into a store value. Used for column defaults.
  case j.kind
  of JNull: newNullValue()
  of JInt: newIntValue(j.getInt.int64)
  of JFloat: newFloatValue(j.getFloat)
  of JBool: newBoolValue(j.getBool)
  of JString: newTextValue(j.getStr())
  of JArray, JObject: Value(kind: dtJson, jsonVal: $j)

proc matchesType(v: Value, c: ColumnDef): bool =
  case v.kind
  of dtNull: c.nullable
  of dtInt: c.kind == dtInt
  of dtFloat: c.kind == dtFloat
  of dtBool: c.kind == dtBool
  of dtText: c.kind == dtText
  of dtJson: c.kind == dtJson

proc validateRow(t: DbTable, data: var RowData) =
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
# Schema serialization helpers (for WAL payloads)
#
proc schemaToPayload(t: DbTable): string =
  var cols = newJArray()
  for c in t.columns:
    cols.add(%*{"name": c.name, "kind": $c.kind, "nullable": c.nullable,
                "default": c.defaultValue})
  var fks = newJArray()
  for fk in t.foreignKeys:
    fks.add(%*{
      "name": fk.name,
      "column": fk.column,
      "refTable": fk.refTable,
      "refColumn": fk.refColumn,
      "onDelete": $fk.onDelete
    })
  var payload = %*{
    "primaryKeyMode": $t.pkType,
    "primaryKey": t.primaryKey,
    "columns": cols,
    "foreignKeys": fks
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
      nullable: c["nullable"].getBool(),
      defaultValue: if c.hasKey("default"): c["default"].getStr() else: ""
    ))
  var foreignKeys: seq[ForeignKeyDef] = @[]
  if n.hasKey("foreignKeys"):
    for fk in n["foreignKeys"].items:
      foreignKeys.add(ForeignKeyDef(
        name: fk["name"].getStr(),
        column: fk["column"].getStr(),
        refTable: fk["refTable"].getStr(),
        refColumn: fk["refColumn"].getStr(),
        onDelete: parseEnum[ForeignKeyAction](fk["onDelete"].getStr())
      ))
  let pkm =
    if n.hasKey("primaryKeyMode"):
      parseEnum[PrimaryKeyMode](n["primaryKeyMode"].getStr())
    else: pkmManual
  var t = newTable(tableName, n["primaryKey"].getStr(), cols, pkm, foreignKeys)
  if pkm == pkmSerial and n.hasKey("pkSequence"):
    t.pkSequence = n["pkSequence"].getInt.uint64
  t

proc rowToPayload(data: RowData): string =
  encodeRowPayload(data)

proc rowFromPayload(payload: string): RowData =
  decodeRowPayload(payload)

proc pkStringFromValue(v: Value): string =
  case v.kind
  of dtNull:
    ""
  of dtInt:
    $v.intVal
  of dtFloat:
    $v.floatVal
  of dtBool:
    if v.boolVal: "true" else: "false"
  of dtText:
    v.strVal
  of dtJson:
    v.jsonVal

proc validateForeignKeysOnInsert(s: Store, t: DbTable, data: var RowData) =
  for fk in t.foreignKeys:
    if not data.hasKey(fk.column):
      continue

    let v = data[fk.column]
    if v.kind == dtNull:
      continue

    if unlikely(not s.tables.hasKey(fk.refTable)):
      raise newException(StoreError, fmt"foreign key '{fk.name}' references missing table '{fk.refTable}'")

    let parent = s.tables[fk.refTable]
    let parentPk = pkStringFromValue(v)
    if unlikely(parentPk.len == 0):
      raise newException(StoreError, fmt"foreign key '{fk.name}' has invalid referenced key value")

    if not parent.rowsByPk.hasKey(parentPk):
      raise newException(
        StoreError,
        fmt"foreign key violation '{fk.name}' on table '{t.name}' column '{fk.column}'"
      )

proc validateNoRestrictChildRows(s: Store, parentTable: string, parentPk: string) =
  let parent = s.tables[parentTable]

  for childName, child in s.tables.pairs:
    if childName == parentTable:
      continue
    for fk in child.foreignKeys:
      if fk.refTable != parentTable:
        continue
      if fk.onDelete != fkaRestrict:
        continue

      let childCol = child.findColumn(fk.column)
      if childCol.isNone:
        continue

      let parentPkCol = parent.findColumn(parent.primaryKey)
      if parentPkCol.isNone:
        continue

      var probe: Value
      case childCol.get.kind
      of dtInt:
        probe = newIntValue(parentPk.parseBiggestInt.int64)
      of dtText:
        probe = newTextValue(parentPk)
      else:
        probe = newTextValue(parentPk)

      # correctness-first: do not depend on eq index for FK integrity checks
      for _, rec in child.rowsByPk.pairs:
        if rec.cols.hasKey(fk.column) and rec.cols[fk.column] == probe:
          raise newException(
            StoreError,
            fmt"cannot delete '{parentTable}:{parentPk}', referenced by foreign key '{fk.name}' in table '{childName}'"
          )

proc validateTableForeignKeys(s: Store, t: DbTable) =
  var seen = initHashSet[string]()
  for fk in t.foreignKeys:
    if fk.name.len == 0:
      raise newException(StoreError, "foreign key name cannot be empty")
    if fk.name in seen:
      raise newException(StoreError, fmt"duplicate foreign key '{fk.name}' in table '{t.name}'")
    seen.incl(fk.name)

    let childCol = t.findColumn(fk.column)
    if childCol.isNone:
      raise newException(StoreError, fmt"foreign key '{fk.name}' column '{fk.column}' not found in table '{t.name}'")

    if unlikely(not s.tables.hasKey(fk.refTable)):
      raise newException(StoreError, fmt"foreign key '{fk.name}' references unknown table '{fk.refTable}'")
    let parent = s.tables[fk.refTable]

    let parentCol = parent.findColumn(fk.refColumn)
    if parentCol.isNone:
      raise newException(
        StoreError,
        fmt"foreign key '{fk.name}' references unknown column '{fk.refColumn}' in table '{fk.refTable}'"
      )

    if fk.refColumn != parent.primaryKey:
      raise newException(
        StoreError,
        fmt"foreign key '{fk.name}' must reference primary key column '{parent.primaryKey}' on table '{fk.refTable}'"
      )

    if childCol.get.kind != parentCol.get.kind:
      raise newException(
        StoreError,
        fmt"foreign key '{fk.name}' type mismatch between '{t.name}.{fk.column}' and '{fk.refTable}.{fk.refColumn}'"
      )

proc ensureForeignKeyIndexes(t: DbTable) =
  for fk in t.foreignKeys:
    t.createIndex(fk.column)

#
# internal mutators. no WAL write
#
proc createTableNoWal(s: Store, t: DbTable) =
  if s.tables.hasKey(t.name):
    raise newException(StoreError, fmt"table already exists: {t.name}")
  s.validateTableForeignKeys(t)
  t.ensureForeignKeyIndexes()
  if s.cc != nil:
    t.slot = newTableSlot[RdbWriteTask](cast[pointer](t))
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

proc insertRowNoWal(t: DbTable, pk: string, data: var RowData): string =
  # Insert a row into the table without writing to WAL. This is used
  # by the store-level `insertRow` proc which handles WAL logging and commit.
  let generated = (t.pkType == pkmSerial and pk.len == 0)
  let effectivePk = t.effectivePkForInsert(pk)
  if effectivePk.len == 0:
    raise newException(StoreError, "primary key value cannot be empty")

  if t.rowsByPk.hasKey(effectivePk):
    raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")

  t.normalizedRowWithPk(data, effectivePk)
  validateRow(t, data)

  let rec = RowRecord(pk: effectivePk, cols: data)
  t.rowsByPk[effectivePk] = rec
  t.addToEqIndexes(effectivePk, data) # NEW
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

proc updateRowNoWal(t: DbTable, pk: string, data: var RowData): bool =
  # Replace an existing row in place without writing to WAL. This is used
  # by the store-level `updateRow` proc which handles WAL logging and commit.
  # The row is deleted and re-inserted under the same primary key, which keeps
  # the equality indexes and the order index consistent.
  if not t.rowsByPk.hasKey(pk):
    return false
  discard t.deleteRowNoWal(pk)
  discard t.insertRowNoWal(pk, data)
  true

#
# Snapshot API
#
type
  SnapshotOnDisk = RdbmsSnapshotOnDisk

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
      foreignKeys: t.foreignKeys,
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
    var t = newTable(td.name, td.primaryKey, td.columns, td.pkType, td.foreignKeys)
    t.ensureForeignKeyIndexes()
    if s.cc != nil:
      t.slot = newTableSlot[RdbWriteTask](cast[pointer](t))
    if td.pkType == pkmSerial:
      t.pkSequence = td.pkSequence
    for i in 0 ..< td.rows.len:
      var data = td.rows[i][1]
      discard t.insertRowNoWal(td.rows[i][0], data)
    s.tables[t.name] = t

proc saveSnapshotIfEnabled(s: Store) =
  # When enabled, saves a snapshot of the current store state to disk.
  if not s.hasDbFile:
    return
  let blob = encodeRdbmsSnapshotToString(buildSnapshot(s))
  writeTextAtomic(s.dbPath, blob)

proc loadSnapshotIfPresent(s: Store) =
  if (not s.hasDbFile) or (not fileExists(s.dbPath)):
    return
  let blob = readFile(s.dbPath)
  if blob.len == 0:
    return
  let snap = decodeRdbmsSnapshotFromString(blob)
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
  if s.cc != nil:
    s.cc.flushWal(s.wal)
    return
  if not s.hasDbFile: return
  s.flushWalIfNeeded(force = true)
  s.saveSnapshotIfEnabled()
  s.pendingOps = 0'u32

proc close*(s: Store) =
  ## Stops the write workers (if concurrent) and flushes the WAL.
  unregisterStoreFlush(cast[pointer](s))
  if s.cc != nil:
    s.cc.close(s.wal)
  else:
    s.flushWalIfNeeded(force = true)

#
# Public API (WAL + apply)
#
proc getTable*(s: Store, name: string): Option[DbTable] =
  ## Get a table by name. Returns none if not found.
  if s.cc != nil:
    var res: Option[DbTable]
    withMetaRead(s.cc):
      if s.tables.hasKey(name):
        res = some(s.tables[name]) else: res = none(DbTable)
    return res
  if s.tables.hasKey(name):
    some(s.tables[name]) else: none(DbTable)

proc createTable*(s: Store, t: DbTable) =
  ## Create a new table in the store. This will write to the WAL and commit the transaction.
  if s.cc != nil:
    t.slot = newTableSlot[RdbWriteTask](cast[pointer](t))
    withMetaWrite(s.cc):
      s.createTableNoWal(t)
    if s.hasWal:
      s.cc.appendWal(s.wal,
        WalEntry(op: woCreateTable, table: t.name, pk: "", payload: schemaToPayload(t)),
        int(s.walFlushEveryOps))
    return
  s.validateTableForeignKeys(t)
  t.ensureForeignKeyIndexes()
  let lsn = s.appendWalIfEnabled(woCreateTable, t.name, "", schemaToPayload(t))
  s.createTableNoWal(t)
  s.markCommitted(lsn)

proc createTableIfNotExist*(s: Store, t: DbTable) =
  ## Create a new table in the store only if it does not already exist.
  if not s.hasTable(t.name):
    s.createTable(t)

proc dropTable*(s: Store, name: string) =
  ## Drop a table from the store. This will write to the WAL and commit the transaction.
  if s.cc != nil:
    withMetaWrite(s.cc):
      for childName, child in s.tables.pairs:
        if childName == name:
          continue
        for fk in child.foreignKeys:
          if fk.refTable == name:
            raise newException(
              StoreError,
              fmt"cannot drop table '{name}', referenced by foreign key '{fk.name}' in table '{childName}'"
            )
      s.dropTableNoWal(name)
    if s.hasWal:
      s.cc.appendWal(s.wal,
        WalEntry(op: woDropTable, table: name, pk: "", payload: ""),
        int(s.walFlushEveryOps))
    return
  for childName, child in s.tables.pairs:
    if childName == name:
      continue
    for fk in child.foreignKeys:
      if fk.refTable == name:
        raise newException(
          StoreError,
          fmt"cannot drop table '{name}', referenced by foreign key '{fk.name}' in table '{childName}'"
        )
  let lsn = s.appendWalIfEnabled(woDropTable, name, "", "")
  s.dropTableNoWal(name)
  s.markCommitted(lsn)

proc isEmpty*(t: DbTable): bool =
  ## Check if the table is empty (has no rows).
  t.rowsByPk.len == 0

proc pkMode*(t: DbTable): PrimaryKeyMode =
  ## Returns the primary key mode of the table (manual or serial/auto-increment)
  t.pkType

proc isConcurrent*(s: Store): bool =
  ## True when the store was opened with `enableConcurrency = true`
  s.cc != nil

proc insertRow*(t: DbTable, pk: string, data: RowData) =
  # direct table mutation: no WAL here (store-level proc logs)
  var d = data
  discard t.insertRowNoWal(pk, d)

proc deleteRow*(t: DbTable, pk: string): bool {.discardable.} =
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
  if t.slot != nil:
    beginRead(t.slot.mu)
    try:
      t.ensureOrderIndex()
      for rec in t.rows.keys:
        yield (rec.pk, rec.cols)
    finally:
      endRead(t.slot.mu)
  else:
    t.ensureOrderIndex()
    for rec in t.rows.keys:
      yield (rec.pk, rec.cols)

iterator allRowsByPk*(t: DbTable): (string, RowData) =
  ## Iterate over all rows in the table using the hash index (unsorted).
  if t.slot != nil:
    beginRead(t.slot.mu)
    try:
      for pk, rec in t.rowsByPk.pairs:
        yield (pk, rec.cols)
    finally:
      endRead(t.slot.mu)
  else:
    for pk, rec in t.rowsByPk.pairs:
      yield (pk, rec.cols)

proc insertRow*(s: Store, tableName: string, pk: string, data: RowData) =
  ## Insert a row into the specified table with the given primary key and data. This will
  ## write to the WAL and commit the transaction. The primary key can be empty for tables with
  ## serial PK mode, in which case it will be auto-generated
  if s.cc != nil:
    var slot: ptr TableSlot[RdbWriteTask]
    withMetaRead(s.cc):
      if unlikely(not s.tables.hasKey(tableName)):
        raise newException(StoreError, fmt"table not found: {tableName}")
      slot = cast[ptr TableSlot[RdbWriteTask]](s.tables[tableName].slot)
    var effectivePk: string
    withSlotWrite(cast[TableSlot[RdbWriteTask]](slot)):
      let t = cast[DbTable](cast[TableSlot[RdbWriteTask]](slot).owner)
      effectivePk = t.effectivePkForInsert(pk)
      if t.rowsByPk.hasKey(effectivePk):
        raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")
      var d = data
      t.normalizedRowWithPk(d, effectivePk)
      validateRow(t, d)
      s.validateForeignKeysOnInsert(t, d)
    let mySeq = s.cc.submit(cast[TableSlot[RdbWriteTask]](slot), RdbWriteTask(kind: roInsert, pk: effectivePk, data: data))
    cast[TableSlot[RdbWriteTask]](slot).waitApplied(mySeq)
    return
  if unlikely(not s.tables.hasKey(tableName)):
    raise newException(StoreError, fmt"table not found: {tableName}")
  var t = s.tables[tableName]
  let effectivePk = t.effectivePkForInsert(pk)
  if t.rowsByPk.hasKey(effectivePk):
    raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")

  var d = data
  t.normalizedRowWithPk(d, effectivePk)
  validateRow(t, d)
  s.validateForeignKeysOnInsert(t, d)

  let lsn = s.appendWalIfEnabled(woInsertRow, tableName, effectivePk, rowToPayload(d))
  discard t.insertRowNoWal(effectivePk, d)
  s.markCommitted(lsn)

proc insertRow*(t: DbTable, data: RowData): string =
  ## direct table mutation: no WAL here (store-level proc logs)
  if t.pkType != pkmSerial:
    raise newException(StoreError, "insertRow(data) requires a serial primary key table")
  var d = data
  result = t.insertRowNoWal("", d)

proc insertRow*(s: Store, tableName: string, data: RowData): string {.discardable.}=
  if s.cc != nil:
    var slot: ptr TableSlot[RdbWriteTask]
    withMetaRead(s.cc):
      if unlikely(not s.tables.hasKey(tableName)):
        raise newException(StoreError, fmt"table not found: {tableName}")
      slot = cast[ptr TableSlot[RdbWriteTask]](s.tables[tableName].slot)
      if cast[DbTable](cast[TableSlot[RdbWriteTask]](slot).owner).pkType != pkmSerial:
        raise newException(StoreError, "insertRow(tableName, data) requires a serial primary key table")
    var effectivePk: string
    withSlotWrite(cast[TableSlot[RdbWriteTask]](slot)):
      let t = cast[DbTable](cast[TableSlot[RdbWriteTask]](slot).owner)
      effectivePk = t.effectivePkForInsert("")
      if t.rowsByPk.hasKey(effectivePk):
        raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")
      var d = data
      t.normalizedRowWithPk(d, effectivePk)
      validateRow(t, d)
      s.validateForeignKeysOnInsert(t, d)
    let mySeq = s.cc.submit(cast[TableSlot[RdbWriteTask]](slot), RdbWriteTask(kind: roInsert, pk: effectivePk, data: data))
    cast[TableSlot[RdbWriteTask]](slot).waitApplied(mySeq)
    return effectivePk
  if unlikely(not s.tables.hasKey(tableName)):
    raise newException(StoreError, fmt"table not found: {tableName}")

  var t = s.tables[tableName]
  if t.pkType != pkmSerial:
    raise newException(StoreError, "insertRow(tableName, data) requires a serial primary key table")

  let effectivePk = t.effectivePkForInsert("")
  if t.rowsByPk.hasKey(effectivePk):
    raise newException(StoreError, fmt"duplicate primary key '{effectivePk}' in table '{t.name}'")

  var d = data
  t.normalizedRowWithPk(d, effectivePk)
  validateRow(t, d)
  s.validateForeignKeysOnInsert(t, d)

  let lsn = s.appendWalIfEnabled(woInsertRow, tableName, effectivePk, rowToPayload(d))
  discard t.insertRowNoWal(effectivePk, d)
  s.tables[tableName] = t
  s.markCommitted(lsn)
  result = effectivePk

proc deleteRow*(s: Store, tableName: string, pk: string): bool {.discardable.} =
  ## Delete a row by primary key. Returns true if a row was deleted, false if not found
  if s.cc != nil:
    var slot: ptr TableSlot[RdbWriteTask]
    withMetaRead(s.cc):
      if unlikely(not s.tables.hasKey(tableName)):
        return false
      slot = cast[ptr TableSlot[RdbWriteTask]](s.tables[tableName].slot)
    let mySeq = s.cc.submit(cast[TableSlot[RdbWriteTask]](slot), RdbWriteTask(kind: roDelete, pk: pk))
    cast[TableSlot[RdbWriteTask]](slot).waitApplied(mySeq)
    return true
  if unlikely(not s.tables.hasKey(tableName)):
    return false
  var t = s.tables[tableName]
  if not t.rowsByPk.hasKey(pk):
    return false

  s.validateNoRestrictChildRows(tableName, pk)

  let lsn = s.appendWalIfEnabled(woDeleteRow, tableName, pk, "")
  let removed = t.deleteRowNoWal(pk)
  s.tables[tableName] = t
  s.markCommitted(lsn)
  removed

proc updateRow*(s: Store, tableName: string, pk: string, data: RowData) =
  ## Replace an existing row identified by `pk` with `data`. The payload is the
  ## complete new row; callers doing partial updates are expected to fetch,
  ## merge and pass back the full row. Concurrency slots are not supported yet.
  if s.cc != nil:
    raise newException(StoreError, "updateRow is not supported in concurrent mode yet")
  if unlikely(not s.tables.hasKey(tableName)):
    raise newException(StoreError, fmt"table not found: {tableName}")
  var t = s.tables[tableName]
  if not t.rowsByPk.hasKey(pk):
    raise newException(StoreError, fmt"row not found: '{pk}' in table '{tableName}'")

  var d = data
  t.normalizedRowWithPk(d, pk)
  validateRow(t, d)
  s.validateForeignKeysOnInsert(t, d)

  let lsn = s.appendWalIfEnabled(woUpdateRow, tableName, pk, rowToPayload(d))
  discard t.updateRowNoWal(pk, d)
  s.tables[tableName] = t
  s.markCommitted(lsn)

proc getRow*(s: Store, tableName: string, pk: string): Option[RowData] =
  ## Fetch a single row by primary key. This will be fast if the table has
  ## an order index, but will fall back to a hash lookup if not.
  if s.cc != nil:
    var slot: ptr TableSlot[RdbWriteTask]
    withMetaRead(s.cc):
      if unlikely(not s.tables.hasKey(tableName)):
        return none(RowData)
      slot = cast[ptr TableSlot[RdbWriteTask]](s.tables[tableName].slot)
    var res: Option[RowData]
    withSlotRead(cast[TableSlot[RdbWriteTask]](slot)):
      res = cast[DbTable](cast[TableSlot[RdbWriteTask]](slot).owner).getRow(pk)
    return res
  if unlikely(not s.tables.hasKey(tableName)):
    return none(RowData)
  s.tables[tableName].getRow(pk)

#
# SQL Query-like API
#
proc whereScan(t: DbTable, column: string, value: Value): seq[(string, RowData)] =
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

proc where*(t: DbTable, column: string, value: Value): seq[(string, RowData)] =
  ## Return all rows where the given column matches the specified value. This will be
  ## fast if the column is indexed, but will fall back to a full scan if not.
  if t.slot != nil:
    var res: seq[(string, RowData)]
    withSlotRead(t.slot):
      res = whereScan(t, column, value)
    return res
  whereScan(t, column, value)

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
    var row = rowFromPayload(e.payload)
    discard t.insertRowNoWal(e.pk, row)
    s.tables[e.table] = t
  of woDeleteRow:
    if likely(s.tables.hasKey(e.table)):
      var t = s.tables[e.table]
      discard t.deleteRowNoWal(e.pk)
      s.tables[e.table] = t
  of woUpdateRow:
    if unlikely(not s.tables.hasKey(e.table)):
      raise newException(StoreError, "WAL replay: table not found: " & e.table)
    var t = s.tables[e.table]
    var row = rowFromPayload(e.payload)
    discard t.updateRowNoWal(e.pk, row)
    s.tables[e.table] = t

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