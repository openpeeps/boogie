# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie


# Description: Store data by columns for analytics workloads.
# How: Store each column as a separate array/file. Optimize for column scans and compression.
# Example: Parquet, ClickHouse, https://github.com/kelindar/column
# key features: bitmap indexing, columnar compression, vectorized execution

import std/[tables, options, json, strformat, sets, strutils, os, sequtils]
import ../wal

type
  ColumnType* = enum
    ## A simple set of column types.
    ## 
    ## Adding more complex types (e.g. nested structures, arrays) is reserved for future work,
    ## but the current design should be flexible enough to accommodate them with
    ## some adjustments to the normalization and storage logic.
    ctInt64, ctFloat64, ctBool, ctString, ctJson

  CompressionCodec* = enum
    ccNone, ccRle, ccDictionary # reserved for future compression engines

  ColumnSchema* = object
    ## Schema for a single column, including its name, type, nullability,
    ## and compression codec. In a production system, you would likely want
    ## to include additional metadata such as encoding parameters, statistics
    ## for query optimization, and support for more complex types.
    name*: string
    kind*: ColumnType
    nullable*: bool
    codec*: CompressionCodec

  TableSchema* = object
    name*: string
    primaryKey*: string
    columns*: seq[ColumnSchema]
    rowCount*: int64

  FilterOp* = enum
    foEq, foNe, foGt, foGte, foLt, foLte, foIn

  Filter* = object
    column*: string
    op*: FilterOp
    value*: JsonNode
    values*: seq[JsonNode]

  AggKind* = enum
    akCount, akSum, akMin, akMax, akAvg

  AggregateSpec* = object
    column*: string
    kind*: AggKind
    alias*: string

  JoinType* = enum
    jtInner, jtLeft, jtRight, jtFull, jtCross # reserved for future SQL joins

  ColumnarError* = object of CatchableError

  ColumnarTable = ref object
    schema: TableSchema
    byName: Table[string, ColumnSchema]
    pkValues: HashSet[string]

  ColumnarStore* = object
    rootDir*: string
    wal*: Wal
    tables*: Table[string, ColumnarTable]
    walFlushEveryOps*: int
    pendingOps*: int

proc tablesRoot(s: ColumnarStore): string = s.rootDir / "tables"
proc tableDir(s: ColumnarStore, table: string): string = s.tablesRoot / table
proc tableMetaPath(s: ColumnarStore, table: string): string = s.tableDir(table) / "table.json"
proc columnDir(s: ColumnarStore, table: string): string = s.tableDir(table) / "columns"
proc columnPath(s: ColumnarStore, table, col: string): string = s.columnDir(table) / (col & ".col")

proc ensureDir(path: string) =
  if not dirExists(path):
    createDir(path)

proc kindToStr(k: ColumnType): string =
  case k
  of ctInt64: "int64"
  of ctFloat64: "float64"
  of ctBool: "bool"
  of ctString: "string"
  of ctJson: "json"

proc strToKind(s: string): ColumnType =
  case s.toLowerAscii()
  of "int64": ctInt64
  of "float64": ctFloat64
  of "bool": ctBool
  of "string": ctString
  of "json": ctJson
  else:
    raise newException(ColumnarError, fmt"Unknown column kind: {s}")

proc codecToStr(c: CompressionCodec): string =
  case c
  of ccNone: "none"
  of ccRle: "rle"
  of ccDictionary: "dictionary"

proc strToCodec(s: string): CompressionCodec =
  case s.toLowerAscii()
  of "none": ccNone
  of "rle": ccRle
  of "dictionary": ccDictionary
  else: ccNone

proc schemaToJson(schema: TableSchema): JsonNode =
  result = %*{
    "name": schema.name,
    "primaryKey": schema.primaryKey,
    "rowCount": schema.rowCount,
    "columns": newJArray()
  }
  for c in schema.columns:
    result["columns"].add(%*{
      "name": c.name,
      "kind": kindToStr(c.kind),
      "nullable": c.nullable,
      "codec": codecToStr(c.codec)
    })

proc schemaFromJson(n: JsonNode): TableSchema =
  if n.kind != JObject:
    raise newException(ColumnarError, "Invalid schema json")
  result.name = n["name"].getStr()
  result.primaryKey = if n.hasKey("primaryKey"): n["primaryKey"].getStr() else: ""
  result.rowCount = if n.hasKey("rowCount"): n["rowCount"].getInt().int64 else: 0'i64
  result.columns = @[]
  for cn in n["columns"].items:
    result.columns.add(ColumnSchema(
      name: cn["name"].getStr(),
      kind: strToKind(cn["kind"].getStr()),
      nullable: if cn.hasKey("nullable"): cn["nullable"].getBool() else: false,
      codec: if cn.hasKey("codec"): strToCodec(cn["codec"].getStr()) else: ccNone
    ))

proc saveSchema(s: ColumnarStore, schema: TableSchema) =
  ensureDir(s.tableDir(schema.name))
  ensureDir(s.columnDir(schema.name))
  writeFile(s.tableMetaPath(schema.name), $(schemaToJson(schema)))

proc loadSchema(path: string): TableSchema =
  schemaFromJson(parseJson(readFile(path)))

proc makeTable(schema: TableSchema): ColumnarTable =
  result = ColumnarTable(
    schema: schema,
    byName: initTable[string, ColumnSchema](),
    pkValues: initHashSet[string]()
  )
  for c in schema.columns:
    result.byName[c.name] = c

proc normalizeValue(kind: ColumnType, nullable: bool, v: JsonNode): JsonNode =
  if v.kind == JNull:
    if not nullable:
      raise newException(ColumnarError, "NULL on non-nullable column")
    return v

  case kind
  of ctInt64:
    if v.kind == JInt: return newJInt(v.getInt())
    if v.kind == JFloat: return newJInt(v.getFloat().int)
    raise newException(ColumnarError, "Expected int64")
  of ctFloat64:
    if v.kind == JFloat: return newJFloat(v.getFloat())
    if v.kind == JInt: return newJFloat(float(v.getInt()))
    raise newException(ColumnarError, "Expected float64")
  of ctBool:
    if v.kind == JBool: return newJBool(v.getBool())
    raise newException(ColumnarError, "Expected bool")
  of ctString:
    if v.kind == JString: return newJString(v.getStr())
    raise newException(ColumnarError, "Expected string")
  of ctJson:
    return v

proc loadColumnValues(path: string): seq[JsonNode] =
  result = @[]
  if not fileExists(path): return
  for line in lines(path):
    if line.len == 0: continue
    result.add(parseJson(line))

proc appendColumnValues(path: string, values: seq[JsonNode]) =
  # Note: This simple append logic assumes that each value can be serialized
  # to a single line of JSON text. In a production system, you would want to
  # include more robust handling of escaping and newlines within values,
  # as well as consider more efficient binary formats for large datasets.
  let f = open(path, fmAppend)
  defer: f.close()
  for v in values:
    f.writeLine($v)

proc cmpJson(a, b: JsonNode): int =
  if a.kind in {JInt, JFloat} and b.kind in {JInt, JFloat}:
    let x = (if a.kind == JInt: float(a.getInt()) else: a.getFloat())
    let y = (if b.kind == JInt: float(b.getInt()) else: b.getFloat())
    if x < y: return -1
    if x > y: return 1
    return 0
  if a.kind == JString and b.kind == JString:
    return cmp(a.getStr(), b.getStr())
  if a.kind == JBool and b.kind == JBool:
    let x = if a.getBool(): 1 else: 0
    let y = if b.getBool(): 1 else: 0
    return cmp(x, y)
  return cmp($a, $b)

proc passesFilter(v: JsonNode, flt: Filter): bool =
  case flt.op
  of foEq: cmpJson(v, flt.value) == 0
  of foNe: cmpJson(v, flt.value) != 0
  of foGt: cmpJson(v, flt.value) > 0
  of foGte: cmpJson(v, flt.value) >= 0
  of foLt: cmpJson(v, flt.value) < 0
  of foLte: cmpJson(v, flt.value) <= 0
  of foIn:
    for vv in flt.values:
      if cmpJson(v, vv) == 0: return true
    false

proc maybeFlushWal(s: var ColumnarStore) =
  inc s.pendingOps
  if s.pendingOps >= max(1, s.walFlushEveryOps):
    s.wal.flush()
    s.pendingOps = 0

proc checkpoint*(s: var ColumnarStore) =
  s.wal.flush()
  s.wal.reset()
  s.pendingOps = 0

proc createTableInternal(s: var ColumnarStore, schema: TableSchema, emitWal: bool, sync: bool) =
  if s.tables.hasKey(schema.name):
    raise newException(ColumnarError, fmt"Table already exists: {schema.name}")

  if schema.columns.len == 0:
    raise newException(ColumnarError, "Table must have at least one column")

  var seen = initHashSet[string]()
  for c in schema.columns:
    if c.name.len == 0:
      raise newException(ColumnarError, "Column name cannot be empty")
    if c.name in seen:
      raise newException(ColumnarError, fmt"Duplicate column name: {c.name}")
    seen.incl(c.name)

  if schema.primaryKey.len > 0 and schema.primaryKey notin seen:
    raise newException(ColumnarError, "Primary key must be one of table columns")

  if emitWal:
    discard s.wal.append(WalEntry(
      op: woCreateTable,
      table: schema.name,
      pk: "",
      payload: $(schemaToJson(schema))
    ), sync = sync)

  ensureDir(s.rootDir)
  ensureDir(s.tablesRoot)
  ensureDir(s.tableDir(schema.name))
  ensureDir(s.columnDir(schema.name))

  for c in schema.columns:
    let p = s.columnPath(schema.name, c.name)
    if not fileExists(p):
      writeFile(p, "")

  s.saveSchema(schema)
  s.tables[schema.name] = makeTable(schema)

  if emitWal:
    if sync: s.checkpoint()
    else: s.maybeFlushWal()

proc dropTableInternal(s: var ColumnarStore, table: string, emitWal: bool, sync: bool) =
  if not s.tables.hasKey(table):
    raise newException(ColumnarError, fmt"Table not found: {table}")

  if emitWal:
    discard s.wal.append(WalEntry(
      op: woDropTable,
      table: table,
      pk: "",
      payload: "{}"
    ), sync = sync)

  removeDir(s.tableDir(table))
  s.tables.del(table)

  if emitWal:
    if sync: s.checkpoint()
    else: s.maybeFlushWal()

proc insertBatchInternal(s: var ColumnarStore, table: string, rows: seq[JsonNode], emitWal: bool, sync: bool) =
  if rows.len == 0: return
  if not s.tables.hasKey(table):
    raise newException(ColumnarError, fmt"Table not found: {table}")

  var t = s.tables[table]
  var buffer = initTable[string, seq[JsonNode]]()
  for c in t.schema.columns:
    buffer[c.name] = @[]

  var newPk = initHashSet[string]()
  for r in rows:
    if r.kind != JObject:
      raise newException(ColumnarError, "Each row must be a JSON object")

    for c in t.schema.columns:
      let raw = if r.hasKey(c.name): r[c.name] else: newJNull()
      let v = normalizeValue(c.kind, c.nullable, raw)
      buffer[c.name].add(v)

      if c.name == t.schema.primaryKey:
        let key = $v
        if key in t.pkValues or key in newPk:
          raise newException(ColumnarError, fmt"Duplicate primary key: {key}")
        newPk.incl(key)

  if emitWal:
    discard s.wal.append(WalEntry(
      op: woInsertRow,
      table: table,
      pk: "",
      payload: $(%*{"rows": rows})
    ), sync = sync)

  for c in t.schema.columns:
    appendColumnValues(s.columnPath(table, c.name), buffer[c.name])

  for k in newPk:
    t.pkValues.incl(k)

  t.schema.rowCount += rows.len.int64
  s.saveSchema(t.schema)
  s.tables[table] = t

  if emitWal:
    if sync: s.checkpoint()
    else: s.maybeFlushWal()


proc recoverFromWal(s: var ColumnarStore) =
  for e in s.wal.entries():
    case e.op
    of woCreateTable:
      let schema = schemaFromJson(parseJson(e.payload))
      if not s.tables.hasKey(schema.name):
        s.createTableInternal(schema, emitWal = false, sync = true)
    of woDropTable:
      if s.tables.hasKey(e.table):
        s.dropTableInternal(e.table, emitWal = false, sync = true)
    of woInsertRow:
      if s.tables.hasKey(e.table):
        let n = parseJson(e.payload)
        let rows = n["rows"].getElems()
        s.insertBatchInternal(e.table, rows, emitWal = false, sync = true)
    of woDeleteRow, woUpdateRow:
      discard

  # clear replayed log to prevent reapplication on next open.
  s.checkpoint()

proc openColumnarStore*(rootDir: string, walPath: string = "", walFlushEveryOps: int = 100): ColumnarStore =
  ## Open or create a columnar store at the given root directory. If walPath is
  ## provided, use that for the WAL file; otherwise, use a default path under rootDir.
  ## The walFlushEveryOps parameter controls how many operations can be buffered in
  ## memory before an automatic flush to disk is triggered.
  result.rootDir = rootDir
  result.tables = initTable[string, ColumnarTable]()
  result.walFlushEveryOps = max(1, walFlushEveryOps)
  result.pendingOps = 0

  ensureDir(rootDir)
  ensureDir(result.tablesRoot)

  let wpath = if walPath.len > 0: walPath else: rootDir / "boogie"
  result.wal = openWal(wpath)

  for kind, p in walkDir(result.tablesRoot):
    if kind == pcDir:
      let meta = p / "table.json"
      if fileExists(meta):
        let schema = loadSchema(meta)
        var t = makeTable(schema)
        if schema.primaryKey.len > 0:
          let pkPath = p / "columns" / (schema.primaryKey & ".col")
          for v in loadColumnValues(pkPath):
            t.pkValues.incl($v)
        result.tables[schema.name] = t

  result.recoverFromWal()

proc createTable*(s: var ColumnarStore, schema: TableSchema, sync: bool = true) =
  s.createTableInternal(schema, emitWal = true, sync = sync)

proc dropTable*(s: var ColumnarStore, table: string, sync: bool = true) =
  s.dropTableInternal(table, emitWal = true, sync = sync)

proc insertBatch*(s: var ColumnarStore, table: string, rows: seq[JsonNode], sync: bool = true) =
  s.insertBatchInternal(table, rows, emitWal = true, sync = sync)

proc scan*(s: ColumnarStore, table: string,
            projectedColumns: seq[string],
            filters: seq[Filter] = @[], limit: int = 0): seq[JsonNode] =
  ## Scan the specified table, applying optional filters and projecting only the requested columns.
  if not s.tables.hasKey(table):
    raise newException(ColumnarError, fmt"Table not found: {table}")

  let t = s.tables[table]
  if projectedColumns.len == 0:
    raise newException(ColumnarError, "projectedColumns cannot be empty")

  var needed = initHashSet[string]()
  for c in projectedColumns: needed.incl(c)
  for f in filters: needed.incl(f.column)

  for c in needed:
    if not t.byName.hasKey(c):
      raise newException(ColumnarError, fmt"Unknown column: {c}")

  var vectors = initTable[string, seq[JsonNode]]()
  for c in needed:
    vectors[c] = loadColumnValues(s.columnPath(table, c))

  let n = int(t.schema.rowCount)
  result = @[]
  for i in 0..<n:
    var ok = true
    for flt in filters:
      if i >= vectors[flt.column].len or not passesFilter(vectors[flt.column][i], flt):
        ok = false
        break
    if not ok: continue

    var row = newJObject()
    for c in projectedColumns:
      row[c] = if i < vectors[c].len: vectors[c][i] else: newJNull()
    result.add(row)

    if limit > 0 and result.len >= limit:
      break

proc aggregate*(s: ColumnarStore, table: string, specs: seq[AggregateSpec], filters: seq[Filter] = @[]): JsonNode =
  if not s.tables.hasKey(table):
    raise newException(ColumnarError, fmt"Table not found: {table}")
  if specs.len == 0:
    raise newException(ColumnarError, "At least one aggregate spec is required")

  let t = s.tables[table]
  var needed = initHashSet[string]()
  for f in filters: needed.incl(f.column)
  for sp in specs:
    if sp.kind != akCount:
      needed.incl(sp.column)

  for c in needed:
    if not t.byName.hasKey(c):
      raise newException(ColumnarError, fmt"Unknown column: {c}")

  var vectors = initTable[string, seq[JsonNode]]()
  for c in needed:
    vectors[c] = loadColumnValues(s.columnPath(table, c))

  var matched: seq[int] = @[]
  let n = int(t.schema.rowCount)
  for i in 0..<n:
    var ok = true
    for flt in filters:
      if i >= vectors[flt.column].len or not passesFilter(vectors[flt.column][i], flt):
        ok = false
        break
    if ok: matched.add(i)

  result = newJObject()
  for sp in specs:
    let key = if sp.alias.len > 0: sp.alias else: sp.kind.repr & "_" & sp.column
    case sp.kind
    of akCount:
      result[key] = newJInt(matched.len)
    of akSum, akAvg:
      var sum = 0.0
      for i in matched:
        let v = vectors[sp.column][i]
        if v.kind == JInt: sum += float(v.getInt())
        elif v.kind == JFloat: sum += v.getFloat()
      if sp.kind == akSum:
        result[key] = newJFloat(sum)
      else:
        result[key] = newJFloat(if matched.len == 0: 0.0 else: sum / float(matched.len))
    of akMin, akMax:
      if matched.len == 0:
        result[key] = newJNull()
      else:
        var best = vectors[sp.column][matched[0]]
        for i in matched[1..^1]:
          let cur = vectors[sp.column][i]
          if (sp.kind == akMin and cmpJson(cur, best) < 0) or
             (sp.kind == akMax and cmpJson(cur, best) > 0):
            best = cur
        result[key] = best

proc join*(s: ColumnarStore, joinType: JoinType,
          leftTable, rightTable: string): seq[JsonNode] =
  ## TODO - Implement join logic for inner, left, right, full, and cross joins. This will likely involve scanning both tables, building hash maps for join keys, and producing combined rows according to the join type
  discard s
  discard joinType
  discard leftTable
  discard rightTable
  raise newException(ColumnarError, "Join execution is reserved for future SQL planner work")
