# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/[strutils, tables, options, json, algorithm, sequtils]

from pkg/openparser/json import toJson
import pkg/openparser/sql

# The vmext import MUST precede the vancode VM imports: it registers the SQL
# opcodes that get spliced into the VM at compile time.
import ./sqlengine/vmext

import pkg/vancode/interpreter/[chunk, value, vm, sym]

# rdbms.Value is excluded locally: inside the engine, `Value` refers to the
# vancode VM value. `export rdbms` below still re-exports everything so the
# driver layer sees the store API (including rdbms.Value) unqualified.
import ./stores/rdbms except Value
export rdbms

# Re-exported for drivers that need to construct placeholder bindings;
# note that `value.Value` is deliberately NOT exported (it would be
# ambiguous with `rdbms.Value`). Inside this module and for qualified
# access as `sqlengine.Value`, `Value` refers to the VM value type.
from pkg/vancode/interpreter/value import nil
export value.initValue

## This module implements a SQL engine on top of the RDBMS store. Statements
## are parsed with `openparser/sql`, lowered to vancode bytecode and evaluated
## by the vancode VM:
##
## - WHERE predicates compile into real VM code: comparisons and arithmetic
##   use the extended NULL-aware `opcSqlCmp`/`opcSqlArith` opcodes and control
##   flow uses plain jump opcodes, so filters run as tight interpreted loops.
## - Table access is bridged through foreign procs (`sql_scan_open`,
##   `sql_col`, `sql_insert`, ...) that capture the store connection.
## - Compiled statements are cached per connection; `?`/`$n`/`:name`
##   placeholders map to named VM globals so rebinding never recompiles.
##
## Supported statement subset (v1): CREATE TABLE [IF NOT EXISTS] with column
## types/PRIMARY KEY/NOT NULL/DEFAULT, DROP TABLE [IF EXISTS], INSERT with
## multiple rows and serial primary keys, SELECT with WHERE/ORDER BY/LIMIT/
## OFFSET/DISTINCT over a single table, UPDATE ... SET ... WHERE and DELETE
## FROM ... WHERE. Expressions support literals, columns, placeholders,
## comparisons, AND/OR/NOT, IS [NOT] NULL, IN (list) and arithmetic.
## JOINs, GROUP BY/HAVING, aggregates, subqueries and ALTER are not supported.

type
  SqlEngineError* = object of CatchableError
    ## Raised for parse errors, unsupported syntax and execution failures.
    ## The driver layer wraps this into a `DbError`.

  StmtKind* = enum
    skSelect, skInsert, skUpdate, skDelete, skCreateTable, skDropTable,
    skCreateIndex

  ExecResult* = object
    kind*: StmtKind
      ## what kind of statement produced this result
    columns*: seq[string]
      ## output column names (SELECT only)
    rows*: seq[seq[string]]
      ## materialized rows; NULL cells are empty strings (SELECT only)
    affected*: int64
      ## number of rows inserted/updated/deleted (DML only)
    lastPk*: string
      ## primary key of the last inserted row (INSERT only)

  ScanState = object
    tableName: string
    pks: seq[string]
      ## primary key snapshot in table order, taken when the scan opened
    pos: int
    closed: bool

  SqlExecCtx = ref object
    store: Store
    scans: seq[ScanState]
    results: seq[seq[Value]]
      ## projected rows of the running SELECT, kept typed for ORDER BY/DISTINCT
    cells: seq[Value]
      ## staging buffer filled by `sql_cell_push`, consumed by project/insert/update
    affected: int64

  CompiledStmt = object
    kind: StmtKind
    script: Script
    chunk: Chunk
    projCols: seq[string]
      ## visible output columns (SELECT)
    hiddenCols: int
      ## extra ORDER BY key columns appended after the visible projection
    orderKeys: seq[(int, bool)]
      ## (projected column index including hidden ones, descending?)
    isDistinct: bool
    deferredRange: bool
      ## true when LIMIT/OFFSET are applied after sorting instead of in the loop
    deferLimit: int64
      ## LIMIT value applied post-sort when deferredRange (-1 = none)
    deferOffset: int64
      ## OFFSET value applied post-sort when deferredRange
    placeholders: seq[string]
      ## global names bound positionally ("?1", "?2", ...)

  SqlEngine* = ref object
    ## Compiles and executes SQL against one store. One instance per
    ## connection; not thread-safe (like the underlying non-concurrent store).
    ctx: SqlExecCtx
    vm: Vm
    cache: Table[string, seq[CompiledStmt]]

proc fail(msg: string) {.noreturn.} =
  raise newException(SqlEngineError, msg)

type
  SqlValue* = value.Value
    ## Alias for the vancode VM value used for placeholder bindings.
    ## (`Value` inside this module refers to this same type; drivers should
    ## use `SqlValue` since `Value` is ambiguous through re-exports.)

const
  CtxSlot = 0'u8
    ## local slot holding the current scan handle during SELECT/UPDATE/DELETE

#
# value conversion
#
proc toVmValue(v: rdbms.Value): Value =
  case v.kind
  of dtNull: nil
  of dtInt: initValue(v.intVal)
  of dtFloat: initValue(v.floatVal)
  of dtBool: initValue(v.boolVal)
  of dtText: initValue(v.strVal)
  of dtJson: initValue(v.jsonVal)

proc cellToString*(v: Value): string =
  ## Renders one cell the way db_connector backends do: NULL becomes "".
  if v == nil:
    return ""
  case v.typeId
  of tyInt: $v.intVal
  of tyFloat: $v.floatVal
  of tyBool:
    if v.boolVal: "1" else: "0"
  of tyString: v.stringVal[]
  of tyJsonStorage:
    if v.jsonVal != nil: toJson(v.jsonVal) else: ""
  else: ""

proc cmpCells(a, b: Value): int =
  ## Total ordering over cells for ORDER BY: NULL sorts first, then numbers,
  ## strings and bools. Mixed kinds order by kind rank.
  if a == nil and b == nil: return 0
  if a == nil: return -1
  if b == nil: return 1
  template rank(x: Value): int =
    case x.typeId
    of tyInt, tyFloat: 0
    of tyString: 1
    of tyBool: 2
    else: 3
  let ra = rank(a)
  let rb = rank(b)
  if ra != rb: return cmp(ra, rb)
  if ra == 0:
    let av = if a.typeId == tyInt: a.intVal.float64 else: a.floatVal
    let bv = if b.typeId == tyInt: b.intVal.float64 else: b.floatVal
    cmp(av, bv)
  elif ra == 1: cmp(a.stringVal[], b.stringVal[])
  elif ra == 2: cmp(int(a.boolVal), int(b.boolVal))
  else: 0

#
# schema helpers used at compile time
#
proc sqlTypeToKind(name: string): DataType =
  let t = name.toUpperAscii()
  if t in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"]: dtInt
  elif t in ["REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"]: dtFloat
  elif t in ["BOOL", "BOOLEAN"]: dtBool
  elif t in ["JSON", "JSONB"]: dtJson
  elif t in ["TEXT", "VARCHAR", "CHAR", "CLOB", "STRING", "BLOB"]: dtText
  else: fail("unsupported column type: " & name)

proc nodeIdent(n: SqlNode): string =
  ## Extracts an identifier from literal-ish nodes
  case n.kind
  of nkIdent, nkQuotedIdent, nkIntegerLit, nkNumericLit, nkStringLit, nkRaw:
    n.strVal
  else: ""

#
# runtime context / foreign procs
#
proc scanPtr(ctx: SqlExecCtx, h: int64): ptr ScanState =
  ## Mutable view into the scan table. Callers must go through the pointer so
  ## position updates persist (assigning to a local would copy the record).
  if h < 0 or h >= ctx.scans.len.int64:
    fail("invalid scan handle")
  addr ctx.scans[int(h)]

proc coerceCell(v: Value, col: ColumnDef, tableName: string): rdbms.Value =
  ## Coerces a staged VM value to the declared column type
  if v == nil:
    return newNullValue()
  case col.kind
  of dtInt:
    case v.typeId
    of tyInt: result = newIntValue(v.intVal)
    of tyFloat: result = newIntValue(v.floatVal.int64)
    of tyString:
      try: result = newIntValue(parseBiggestInt(v.stringVal[]))
      except ValueError:
        fail("cannot convert '" & v.stringVal[] & "' to INTEGER for " &
             tableName & "." & col.name)
    else: fail("type mismatch for " & tableName & "." & col.name)
  of dtFloat:
    case v.typeId
    of tyInt: result = newFloatValue(v.intVal.float64)
    of tyFloat: result = newFloatValue(v.floatVal)
    of tyString:
      try: result = newFloatValue(parseFloat(v.stringVal[]))
      except ValueError:
        fail("cannot convert '" & v.stringVal[] & "' to REAL for " &
             tableName & "." & col.name)
    else: fail("type mismatch for " & tableName & "." & col.name)
  of dtBool:
    case v.typeId
    of tyBool: result = newBoolValue(v.boolVal)
    of tyInt: result = newBoolValue(v.intVal != 0)
    else: fail("type mismatch for " & tableName & "." & col.name)
  of dtText:
    case v.typeId
    of tyString: result = newTextValue(v.stringVal[])
    of tyInt: result = newTextValue($v.intVal)
    of tyFloat: result = newTextValue($v.floatVal)
    else: fail("type mismatch for " & tableName & "." & col.name)
  of dtJson:
    if v.typeId == tyString:
      result = rdbms.Value(kind: dtJson, jsonVal: v.stringVal[])
    else: fail("type mismatch for " & tableName & "." & col.name)
  of dtNull:
    fail("column type cannot be NULL: " & col.name)

proc makeForeignProcs(ctx: SqlExecCtx): seq[(string, int, bool, ForeignProc)] =
  ## The standard foreign proc set bound to one execution context. Entries are
  ## (name, paramCount, hasResult, impl). paramCount is fixed per proc; values
  ## travel through the staging buffer via `sql_cell_push` so no proc needs
  ## variable arity.
  proc fNull(args: StackView, argc: int): Value =
    nil

  proc fScanOpen(args: StackView, argc: int): Value =
    let name = args[0].stringVal[]
    if not ctx.store.hasTable(name):
      fail("no such table: " & name)
    let t = ctx.store.getTable(name).get()
    var pks: seq[string]
    for pk, _ in t.allRows:
      pks.add(pk)
    ctx.scans.add(ScanState(tableName: name, pks: pks, pos: -1))
    initValue(int64(ctx.scans.len - 1))

  proc fScanNext(args: StackView, argc: int): Value =
    let sc = ctx.scanPtr(args[0].intVal)
    if sc.closed:
      fail("scan is closed")
    let t = ctx.store.getTable(sc.tableName).get()
    inc sc.pos
    while sc.pos < sc.pks.len:
      # rows may have been deleted since the snapshot was taken
      if t.getRow(sc.pks[sc.pos]).isSome:
        return initValue(true)
      inc sc.pos
    initValue(false)

  proc fCol(args: StackView, argc: int): Value =
    let sc = ctx.scanPtr(args[0].intVal)
    let colName = args[1].stringVal[]
    if sc.closed or sc.pos < 0:
      fail("no current row")
    let row = ctx.store.getTable(sc.tableName).get().getRow(sc.pks[sc.pos])
    if row.isNone:
      return nil
    if row.get.hasKey(colName):
      return toVmValue(row.get[colName])
    nil

  proc fCellPush(args: StackView, argc: int): Value =
    ctx.cells.add(args[0])
    nil

  proc fProjectFlush(args: StackView, argc: int): Value =
    let n = args[0].intVal
    when defined(boogieSqlDebug):
      echo "[flush] n=", n, " cells=", ctx.cells.len, " ctxptr=", cast[uint](ctx)
    if ctx.cells.len < n:
      fail("projection underflow")
    ctx.results.add(ctx.cells[^int(n) .. ^1])
    ctx.cells.setLen(ctx.cells.len - int(n))
    nil

  proc fInsert(args: StackView, argc: int): Value =
    let tableName = args[0].stringVal[]
    if not ctx.store.hasTable(tableName):
      fail("no such table: " & tableName)
    let t = ctx.store.getTable(tableName).get()
    var data: RowData
    let colsJson = args[1].stringVal[].parseJson()
    if colsJson.len != ctx.cells.len:
      fail("insert column/value count mismatch")
    for idx in 0 ..< colsJson.len:
      let colName = colsJson[idx].getStr()
      let colDef = t.findColumn(colName)
      if colDef.isNone:
        fail("unknown column '" & colName & "' in table '" & tableName & "'")
      data[colName] = coerceCell(ctx.cells[idx], colDef.get, tableName)
    ctx.cells.setLen(0)

    # an explicit PK may arrive inside the column list or via args[2]
    var explicitPk = if args[2].typeId == tyString: args[2].stringVal[] else: ""
    if data.hasKey(t.primaryKey):
      let pv = data[t.primaryKey]
      explicitPk = case pv.kind
        of dtInt: $pv.intVal
        of dtText: pv.strVal
        else: fail("primary key must be int or text")
      data.del(t.primaryKey)

    let pk =
      if explicitPk.len > 0:
        ctx.store.insertRow(tableName, explicitPk, data)
        explicitPk
      elif t.pkMode == pkmSerial:
        ctx.store.insertRow(tableName, data)
      else:
        fail("primary key required for table '" & tableName & "'")
    inc ctx.affected
    initValue(pk)

  proc fUpdateRow(args: StackView, argc: int): Value =
    let sc = ctx.scanPtr(args[0].intVal)
    if sc.pos < 0 or sc.pos >= sc.pks.len:
      fail("no current row for update")
    let tableName = sc.tableName
    let t = ctx.store.getTable(tableName).get()
    let pk = sc.pks[sc.pos]
    var row = ctx.store.getRow(tableName, pk).get()
    let colsJson = args[1].stringVal[].parseJson()
    if colsJson.len != ctx.cells.len:
      fail("update column/value count mismatch")
    for idx in 0 ..< colsJson.len:
      let colName = colsJson[idx].getStr()
      let colDef = t.findColumn(colName)
      if colDef.isNone:
        fail("unknown column '" & colName & "' in table '" & tableName & "'")
      if colName == t.primaryKey:
        fail("updating primary key column '" & colName & "' is not supported")
      row[colName] = coerceCell(ctx.cells[idx], colDef.get, tableName)
    ctx.cells.setLen(0)
    ctx.store.updateRow(tableName, pk, row)
    inc ctx.affected
    initValue(1'i64)

  proc fDeleteRow(args: StackView, argc: int): Value =
    let sc = ctx.scanPtr(args[0].intVal)
    if sc.pos < 0 or sc.pos >= sc.pks.len:
      fail("no current row for delete")
    let pk = sc.pks[sc.pos]
    if ctx.store.deleteRow(sc.tableName, pk):
      inc ctx.affected
    initValue(1'i64)

  proc fFinish(args: StackView, argc: int): Value =
    let sc = ctx.scanPtr(args[0].intVal)
    sc.closed = true
    initValue(0'i64)

  proc fCreateTable(args: StackView, argc: int): Value =
    ## Schema JSON contract:
    ## {"name": ..., "pk": ..., "serial": bool,
    ##  "cols": [{"name":..., "kind":"dtInt"|..., "nullable":bool,
    ##            "default": <raw json literal>, ...]}]}
    when defined(boogieSqlDebug):
      echo "[fCreateTable] raw=", args[0].stringVal[]
    let spec = args[0].stringVal[].parseJson()
    let name = spec["name"].getStr()
    let created = not ctx.store.hasTable(name)
    if created:
      var cols: seq[ColumnDef]
      for c in spec["cols"]:
        var def = newColumn(
          c["name"].getStr(),
          parseEnum[DataType](c["kind"].getStr()),
          c["nullable"].getBool(true))
        if c.hasKey("default"):
          def.defaultValue = $c["default"]
        cols.add(def)
      let mode = if spec["serial"].getBool(false): pkmSerial else: pkmManual
      ctx.store.createTableIfNotExist(newTable(
        name = name, primaryKey = spec["pk"].getStr(),
        columns = cols, primaryKeyMode = mode))
      inc ctx.affected
    initValue(if created: 1'i64 else: 0'i64)

  proc fDropTable(args: StackView, argc: int): Value =
    let name = args[0].stringVal[]
    let ifExists = args[1].intVal != 0
    if ctx.store.hasTable(name):
      ctx.store.dropTable(name)
      inc ctx.affected
      initValue(1'i64)
    else:
      if not ifExists:
        fail("no such table: " & name)
      initValue(0'i64)

  proc fCreateIndex(args: StackView, argc: int): Value =
    ## Builds the equality index for one column of an existing table.
    let tableName = args[0].stringVal[]
    let colName = args[1].stringVal[]
    if not ctx.store.hasTable(tableName):
      fail("no such table: " & tableName)
    let t = ctx.store.getTable(tableName).get()
    if t.findColumn(colName).isNone:
      fail("unknown column '" & colName & "' in table '" & tableName & "'")
    t.createIndex(colName)
    inc ctx.affected
    initValue(1'i64)

  @[
    ("sql_null", 0, true, ForeignProc(fNull)),
    ("sql_scan_open", 1, true, ForeignProc(fScanOpen)),
    ("sql_scan_next", 1, true, ForeignProc(fScanNext)),
    ("sql_col", 2, true, ForeignProc(fCol)),
    ("sql_cell_push", 1, false, ForeignProc(fCellPush)),
    ("sql_project_flush", 1, false, ForeignProc(fProjectFlush)),
    ("sql_insert", 3, true, ForeignProc(fInsert)),
    ("sql_update_row", 2, true, ForeignProc(fUpdateRow)),
    ("sql_delete_row", 1, true, ForeignProc(fDeleteRow)),
    ("sql_finish", 1, true, ForeignProc(fFinish)),
    ("sql_create_table", 1, true, ForeignProc(fCreateTable)),
    ("sql_drop_table", 2, true, ForeignProc(fDropTable)),
    ("sql_create_index", 2, true, ForeignProc(fCreateIndex)),
  ]

#
# bytecode lowering
#
type
  STy = enum sUnknown, sNull, sInt, sFloat, sBool, sText

  Lowerer = object
    eng: SqlEngine
    ch: Chunk
    script: Script
    fileSid: uint16
    pids: Table[string, uint16]
    tbl: Option[DbTable]
    placeholders: seq[string]
      ## positional global names ("?1") in first-appearance order

proc initLowerer(eng: SqlEngine, chunkName: string): Lowerer =
  var ch = newChunk(chunkName)
  result = Lowerer(
    eng: eng,
    ch: ch,
    script: newScript(ch),
    fileSid: ch.getString(chunkName),
    tbl: none(DbTable)
  )
  for (name, params, hasRes, impl) in makeForeignProcs(eng.ctx):
    result.pids[name] = uint16(result.script.procs.len)
    result.script.procs.add(Proc(
      name: name, kind: pkForeign, foreign: impl,
      paramCount: params, hasResult: hasRes))

proc here(l: Lowerer): int = l.ch.code.len

proc callF(l: var Lowerer, name: string) =
  ## Emits a direct call to a registered foreign proc
  l.ch.emit(opcCallD)
  l.ch.emit(l.fileSid)
  l.ch.emit(l.pids[name])

proc jmpFwd(l: var Lowerer, opc: Opcode): int =
  ## Emits a forward jump with an unfilled distance hole; returns the hole
  ## position for `patchFwd`.
  l.ch.emit(opc)
  result = l.ch.code.len
  l.ch.emit(0'u16)

proc patchFwd(l: var Lowerer, hole, labelByte: int) =
  l.ch.fillHole(hole, uint16(labelByte - hole + 1))

proc jmpBack(l: var Lowerer, labelByte: int) =
  let dist = uint16(l.ch.code.len - labelByte)
  l.ch.emit(opcJumpBack)
  l.ch.emit(dist)

proc pushS(l: var Lowerer, s: string): uint16 =
  let sid = l.ch.getString(s)
  l.ch.emit(opcPushS)
  l.ch.emit(sid)
  sid

proc placeholderName(l: var Lowerer, raw: string): string =
  ## Maps a placeholder token (`?`, `$n`, `:name`) to its global variable
  ## name. Anonymous `?` markers get numbered by first appearance; numbered
  ## or named ones share a slot when repeated.
  var name = raw
  while name.len > 0 and name[0] in {'?', '$', ':'}:
    name = name[1 ..^ 1]
  if name.len == 0:
    return "?" & $(l.placeholders.len + 1)
  "?" & name

proc inferTy(l: Lowerer, n: SqlNode): STy =
  case n.kind
  of nkIntegerLit: sInt
  of nkNumericLit:
    if '.' in n.strVal or 'e' in n.strVal.toLowerAscii(): sFloat else: sInt
  of nkStringLit: sText
  of nkPlaceholder: sUnknown
  of nkIdent:
    case n.strVal.toLowerAscii()
    of "null": sNull
    of "true", "false": sBool
    else:
      # column reference
      if l.tbl.isNone: return sUnknown
      let def = l.tbl.get().findColumn(n.strVal)
      if def.isNone: return sUnknown
      case def.get().kind
      of dtInt: sInt
      of dtFloat: sFloat
      of dtBool: sBool
      of dtText: sText
      of dtJson, dtNull: sUnknown
  of nkDot: inferTy(l, n[1])
  of nkSelectPair: inferTy(l, n[0])
  of nkPrGroup: inferTy(l, n[0])
  of nkInfix:
    case n[0].strVal.toLowerAscii()
    of "=", "!=", "<>", "<", "<=", ">", ">=", "is": sBool
    of "and", "or": sBool
    of "+", "-", "*", "/":
      max(inferTy(l, n[1]), inferTy(l, n[2]))
    else: sUnknown
  else: sUnknown

proc genExpr(l: var Lowerer, n: SqlNode) {.locks: 0.}

proc genAndOr(l: var Lowerer, n: SqlNode) =
  let isAnd = n[0].strVal.toLowerAscii() == "and"
  let jumpOp = if isAnd: opcJumpFwdF else: opcJumpFwdT
  let labelOpc = if isAnd: opcPushFalse else: opcPushTrue
  let endOpc = if isAnd: opcPushTrue else: opcPushFalse
  genExpr(l, n[1])
  let holeShort = l.jmpFwd(jumpOp)
  l.ch.emit(opcDiscard)
  l.ch.emit(1'u8)
  genExpr(l, n[2])
  let holeShort2 = l.jmpFwd(jumpOp)
  l.ch.emit(opcDiscard)
  l.ch.emit(1'u8)
  l.ch.emit(endOpc)
  let holeEnd = l.jmpFwd(opcJumpFwd)
  let shortLabel = l.here()
  l.patchFwd(holeShort, shortLabel)
  l.patchFwd(holeShort2, shortLabel)
  l.ch.emit(opcDiscard)
  l.ch.emit(1'u8)
  l.ch.emit(labelOpc)
  l.patchFwd(holeEnd, l.here())

proc internPlaceholder(l: var Lowerer, g: string): string =
  if g notin l.placeholders:
    l.placeholders.add(g)
  g

proc genExpr(l: var Lowerer, n: SqlNode) =
  ## Lowers an expression, leaving its value on the stack
  case n.kind
  of nkIntegerLit:
    l.ch.emit(opcPushI)
    l.ch.emit(n.strVal.parseBiggestInt.int64)
  of nkNumericLit:
    if '.' in n.strVal or 'e' in n.strVal.toLowerAscii():
      l.ch.emit(opcPushF)
      l.ch.emit(parseFloat(n.strVal))
    else:
      l.ch.emit(opcPushI)
      l.ch.emit(n.strVal.parseBiggestInt.int64)
  of nkStringLit:
    discard l.pushS(n.strVal)
  of nkIdent:
    case n.strVal.toLowerAscii()
    of "null":
      l.callF("sql_null")
    of "true":
      l.ch.emit(opcPushTrue)
    of "false":
      l.ch.emit(opcPushFalse)
    else:
      # column reference on the current scan row
      let colName = n.strVal
      if l.tbl.isSome and l.tbl.get().findColumn(colName).isNone:
        fail("no such column: '" & colName & "' in table '" &
             l.tbl.get().name & "'")
      l.ch.emit(opcPushL)
      l.ch.emit(CtxSlot)
      discard l.pushS(colName)
      l.callF("sql_col")
  of nkDot:
    # qualified reference t.col: use the column part (single-table engine)
    l.genExpr(n[1])
  of nkSelectPair:
    l.genExpr(n[0])
  of nkPrGroup:
    l.genExpr(n[0])
  of nkPlaceholder:
    let g = l.internPlaceholder(l.placeholderName(n.strVal))
    let sid = l.ch.getString(g)
    l.ch.emit(opcPushG)
    l.ch.emit(sid)
  of nkPrefix:
    let op = n[0].strVal.toLowerAscii()
    case op
    of "-":
      # 0 - x keeps negation NULL-safe and type-agnostic
      l.ch.emit(opcPushI)
      l.ch.emit(0'i64)
      l.genExpr(n[1])
      l.ch.emit(opcSqlArith)
      l.ch.emit(SqlArithSub)
    of "not":
      if inferTy(l, n[1]) != sBool:
        fail("NOT requires a boolean expression")
      l.genExpr(n[1])
      l.ch.emit(opcInvB)
    of "+":
      l.genExpr(n[1])
    else:
      fail("unsupported unary operator: " & n[0].strVal)
  of nkInfix:
    let op = n[0].strVal.toLowerAscii()
    case op
    of "=", "!=", "<>", "<", "<=", ">", ">=":
      # SQL NOT binds looser than comparisons: hoist `not` prefixes on either
      # operand and fold them into the comparison mode instead.
      var lhs = n[1]
      var rhs = n[2]
      var negate = false
      while lhs.kind == nkPrefix and lhs.len >= 2 and
            lhs[0].strVal.toLowerAscii() == "not":
        negate = not negate
        lhs = lhs[1]
      while rhs.kind == nkPrefix and rhs.len >= 2 and
            rhs[0].strVal.toLowerAscii() == "not":
        negate = not negate
        rhs = rhs[1]
      let baseMode =
        case op
        of "=": SqlCmpEq
        of "<": SqlCmpLt
        of "<=": SqlCmpLe
        of ">": SqlCmpGt
        of ">=": SqlCmpGe
        else: SqlCmpNe
      let cmpMode =
        if negate:
          # invert: eq<->ne, lt<->ge, le<->gt
          case baseMode
          of SqlCmpEq: SqlCmpNe
          of SqlCmpNe: SqlCmpEq
          of SqlCmpLt: SqlCmpGe
          of SqlCmpLe: SqlCmpGt
          of SqlCmpGt: SqlCmpLe
          else: SqlCmpLt
        else: baseMode
      l.genExpr(lhs)
      l.genExpr(rhs)
      l.ch.emit(opcSqlCmp)
      l.ch.emit(cmpMode)
    of "is":
      if nodeIdent(n[2]).toUpperAscii() != "NULL":
        fail("only IS NULL is supported")
      l.genExpr(n[1])
      l.ch.emit(opcSqlIsNull)
    of "is not":
      if nodeIdent(n[2]).toUpperAscii() != "NULL":
        fail("only IS NOT NULL is supported")
      l.genExpr(n[1])
      l.ch.emit(opcSqlIsNull)
      l.ch.emit(opcInvB)
    of "and", "or":
      l.genAndOr(n)
    of "+", "-", "*", "/":
      l.genExpr(n[1])
      l.genExpr(n[2])
      l.ch.emit(opcSqlArith)
      l.ch.emit(case op
        of "+": SqlArithAdd
        of "-": SqlArithSub
        of "*": SqlArithMul
        else: SqlArithDiv)
    of "like", "ilike", "in", "between":
      fail("'" & op & "' is not supported yet")
    else:
      fail("unsupported operator: " & n[0].strVal)
  of nkCall:
    fail("function calls are not supported yet")
  else:
    fail("unsupported expression node: " & $n.kind)

#
# statement lowering
#
proc mustTable(l: Lowerer, name: string): DbTable =
  if not l.eng.ctx.store.hasTable(name):
    fail("no such table: '" & name & "'")
  l.eng.ctx.store.getTable(name).get()

proc genScanLoopPrelude(l: var Lowerer, tableName: string) =
  ## Opens a scan into local slot CtxSlot
  discard l.pushS(tableName)
  l.callF("sql_scan_open")
  l.ch.emit(opcPopL)
  l.ch.emit(CtxSlot)

proc genScanTail(l: var Lowerer) =
  ## Closes the scan in slot CtxSlot and discards the finish result
  l.ch.emit(opcPushL)
  l.ch.emit(CtxSlot)
  l.callF("sql_finish")
  l.ch.emit(opcDiscard)
  l.ch.emit(1'u8)

proc compileSelect(l: var Lowerer, root: SqlNode): CompiledStmt =
  let isDistinct = root.kind == nkSelectDistinct
  var tableName = ""
  var whereNode: SqlNode = nil
  var orderNode: SqlNode = nil
  var limitNode: SqlNode = nil
  var offsetNode: SqlNode = nil
  var colsNode: SqlNode = nil

  for child in root.sons:
    case child.kind
    of nkSelectColumns:
      if colsNode.isNil: colsNode = child
    of nkFrom:
      tableName = nodeIdent(child[0][0])
    of nkWhere: whereNode = child[0]
    of nkOrder: orderNode = child
    of nkLimit: limitNode = child[0]
    of nkOffset: offsetNode = child[0]
    else: discard

  if colsNode.isNil:
    fail("SELECT requires a column list")
  if tableName.len == 0:
    fail("SELECT requires a FROM clause")
  let tbl = l.mustTable(tableName)
  l.tbl = some(tbl)

  # aggregate detection: GROUP-less COUNT/SUM/AVG/MIN/MAX projection entries.
  # Runs before plain projection resolution so mixing is caught in one pass.
  type AggDesc = object
    fn: string
      ## normalized lowercase function name
    argExpr: SqlNode
      ## nil for COUNT(*)
    slot: uint8
    outName: string

  const AggNames = ["count", "sum", "avg", "min", "max"]
  var aggs: seq[AggDesc]
  for rawEntry in colsNode.sons:
    # the parser wraps projection items in nkSelectPair
    var aliasName = ""
    var entry = rawEntry
    if entry.kind == nkSelectPair and entry.len >= 1:
      if entry.len >= 2:
        aliasName = entry[1].nodeIdent()
      entry = entry[0]
    if entry.kind == nkCall and entry.len >= 1 and entry[0].kind == nkIdent:
      let fn = entry[0].strVal.toLowerAscii()
      if fn in AggNames:
        if fn == "count" and entry.len >= 2 and entry[1].kind == nkIdent and
           entry[1].strVal == "*":
          aggs.add(AggDesc(fn: fn, argExpr: nil,
            outName: if aliasName.len > 0: aliasName else: "COUNT(*)"))
        elif entry.len >= 2:
          let argName = nodeIdent(entry[1])
          aggs.add(AggDesc(fn: fn, argExpr: entry[1],
            outName: if aliasName.len > 0: aliasName
                     else: fn.toUpperAscii & "(" & argName & ")"))
        else:
          fail(fn.toUpperAscii & " requires an argument")
        continue
    if aggs.len > 0:
      fail("mixing aggregates with plain columns is not supported")

  # resolve projection entries: (exprNode, outputName)
  type ProjEntry = tuple[expr: SqlNode, name: string]
  var proj: seq[ProjEntry]
  for entry in colsNode.sons:
    if entry.kind == nkIdent and entry.strVal == "*":
      for c in tbl.columns:
        proj.add((newNode(nkIdent, c.name), c.name))
    elif entry.kind == nkSelectPair and entry.len >= 1:
      let expr = entry[0]
      var name = ""
      if entry.len >= 2:
        name = entry[1].nodeIdent()
      if name.len == 0:
        case expr.kind
        of nkIdent, nkQuotedIdent: name = expr.strVal
        of nkDot: name = expr[1].strVal
        else: name = "col" & $proj.len
      proj.add((expr, name))
    else:
      proj.add((entry, entry.nodeIdent()))

  # ORDER BY keys; hidden projection columns are appended after visible ones
  var orderKeys: seq[(int, bool)]
  var hiddenCols = 0
  if not orderNode.isNil:
    for key in orderNode.sons:
      var desc = false
      var colNode = key
      if key.kind == nkDesc:
        desc = true
        colNode = key[0]
      let colName = colNode.nodeIdent()
      if colName.len == 0:
        fail("unsupported ORDER BY expression")
      # reuse a projected column when possible
      var idx = -1
      for i, p in proj:
        if p.name.toLowerAscii() == colName.toLowerAscii():
          idx = i
          break
      if idx < 0:
        idx = proj.len
        proj.add((newNode(nkIdent, colName), colName))
        inc hiddenCols
      orderKeys.add((idx, desc))

  var limitVal = -1'i64
  var offsetVal = 0'i64
  try:
    if not limitNode.isNil: limitVal = limitNode.strVal.parseBiggestInt.int64
    if not offsetNode.isNil: offsetVal = offsetNode.strVal.parseBiggestInt.int64
  except ValueError:
    fail("LIMIT/OFFSET must be integer literals")

  if aggs.len > 0:
    if not orderNode.isNil:
      fail("ORDER BY is not supported together with aggregates")

  if aggs.len > 0:
    # ---- aggregate plan ----
    # locals: slot0 handle; one or two slots per aggregate from slot 3 up
    var nextSlot = 3
    for aIdx in 0 ..< aggs.len:
      var a = addr aggs[aIdx]
      a[].slot = uint8(nextSlot)
      case a[].fn
      of "count":
        l.ch.emit(opcPushI); l.ch.emit(0'i64)
        l.ch.emit(opcPopL); l.ch.emit(a[].slot)
      of "sum", "avg":
        l.ch.emit(opcPushI); l.ch.emit(0'i64)
        l.ch.emit(opcPopL); l.ch.emit(a[].slot)
        if a[].fn == "avg":
          l.ch.emit(opcPushI); l.ch.emit(0'i64)
          l.ch.emit(opcPopL); l.ch.emit(uint8(nextSlot + 1))
          inc nextSlot
      of "min", "max":
        l.callF("sql_null")
        l.ch.emit(opcPopL); l.ch.emit(a[].slot)
      else: discard
      inc nextSlot

    l.genScanLoopPrelude(tableName)

    let loopTop = l.here()
    l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
    l.callF("sql_scan_next")
    let holeLoopEnd = l.jmpFwd(opcJumpFwdF)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

    var holeCont = -1
    if not whereNode.isNil:
      l.genExpr(whereNode)
      holeCont = l.jmpFwd(opcJumpFwdF)
      l.ch.emit(opcDiscard); l.ch.emit(1'u8)

    # aggregate step per row
    for a in aggs:
      let mode =
        case a.fn
        of "count":
          if a.argExpr.isNil: SqlAggCountStar else: SqlAggCount
        of "sum": SqlAggSum
        of "avg": SqlAggAvg
        of "min": SqlAggMin
        else: SqlAggMax
      if mode == SqlAggCountStar:
        l.ch.emit(opcPushI); l.ch.emit(1'i64)
      else:
        l.genExpr(a.argExpr)
      l.ch.emit(opcSqlAgg)
      l.ch.emit(mode)
      l.ch.emit(a.slot)

    l.jmpBack(loopTop)

    if holeCont >= 0:
      let contLabel = l.here()
      l.patchFwd(holeCont, contLabel)
      l.ch.emit(opcDiscard); l.ch.emit(1'u8)
      l.jmpBack(loopTop)

    let endLabel = l.here()
    l.patchFwd(holeLoopEnd, endLabel)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

    # materialize the single result row from the accumulators
    for a in aggs:
      l.ch.emit(opcPushL); l.ch.emit(a.slot)
      if a.fn == "avg":
        l.ch.emit(opcPushL); l.ch.emit(uint8(int(a.slot) + 1))
        l.ch.emit(opcSqlArith); l.ch.emit(SqlArithDiv)
      l.callF("sql_cell_push")
    l.ch.emit(opcPushI); l.ch.emit(int64(aggs.len))
    l.callF("sql_project_flush")

    l.genScanTail()
    l.ch.emit(opcHalt)

    return CompiledStmt(
      kind: skSelect,
      script: l.script,
      chunk: l.ch,
      projCols: aggs.mapIt(it.outName),
      deferLimit: -1'i64,
      placeholders: l.placeholders
    )

  let deferredRange = orderKeys.len > 0
  # locals: slot0 handle, slot1 offset counter, slot2 limit counter
  if offsetVal > 0 and not deferredRange:
    l.ch.emit(opcPushI); l.ch.emit(offsetVal)
    l.ch.emit(opcPopL); l.ch.emit(1'u8)
  if limitVal >= 0 and not deferredRange:
    l.ch.emit(opcPushI); l.ch.emit(limitVal)
    l.ch.emit(opcPopL); l.ch.emit(2'u8)

  l.genScanLoopPrelude(tableName)

  var extraEndHoles: seq[int] = @[]

  # skip OFFSET rows before the main loop (unordered case only)
  if offsetVal > 0 and not deferredRange:
    let loopTop = l.here()
    # counter == 0 -> done skipping
    l.ch.emit(opcPushL); l.ch.emit(1'u8)
    l.ch.emit(opcPushI); l.ch.emit(0'i64)
    l.ch.emit(opcEqI)
    let holeDone = l.jmpFwd(opcJumpFwdT)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    # advance the scan by one row; exhaustion ends the whole statement
    l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
    l.callF("sql_scan_next")
    extraEndHoles.add(l.jmpFwd(opcJumpFwdF))
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.ch.emit(opcDecL); l.ch.emit(1'u8)
    l.jmpBack(loopTop)
    let doneLabel = l.here()
    l.patchFwd(holeDone, doneLabel)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  let loopTop = l.here()
  l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
  l.callF("sql_scan_next")
  let holeLoopEnd = l.jmpFwd(opcJumpFwdF)
  # taken or not, the condition stays on the stack: pop it on the fall-through
  l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  var holeCont = -1
  if not whereNode.isNil:
    l.genExpr(whereNode)
    holeCont = l.jmpFwd(opcJumpFwdF)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  # project cells then flush them as one row
  for p in proj:
    l.genExpr(p.expr)
    l.callF("sql_cell_push")
  l.ch.emit(opcPushI)
  l.ch.emit(int64(proj.len))
  l.callF("sql_project_flush")

  # LIMIT countdown (unordered case only)
  if limitVal >= 0 and not deferredRange:
    l.ch.emit(opcDecL); l.ch.emit(2'u8)
    l.ch.emit(opcPushL); l.ch.emit(2'u8)
    l.ch.emit(opcPushI); l.ch.emit(0'i64)
    l.ch.emit(opcEqI)
    let holeStop = l.jmpFwd(opcJumpFwdT)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.patchFwd(holeStop, l.here())

  l.jmpBack(loopTop)

  if holeCont >= 0:
    # rows filtered out by WHERE land here with their condition on the stack
    let contLabel = l.here()
    l.patchFwd(holeCont, contLabel)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.jmpBack(loopTop)

  let endLabel = l.here()
  l.patchFwd(holeLoopEnd, endLabel)
  for h in extraEndHoles:
    l.patchFwd(h, endLabel)
  l.ch.emit(opcDiscard); l.ch.emit(1'u8)
  l.genScanTail()
  l.ch.emit(opcHalt)

  result = CompiledStmt(
    kind: skSelect,
    script: l.script,
    chunk: l.ch,
    projCols: proj[0 ..< proj.len - hiddenCols].mapIt(it.name),
    hiddenCols: hiddenCols,
    orderKeys: orderKeys,
    isDistinct: isDistinct,
    deferredRange: deferredRange,
    deferLimit: if deferredRange: limitVal else: -1'i64,
    deferOffset: if deferredRange: offsetVal else: 0'i64,
    placeholders: l.placeholders
  )

proc compileUpdateDelete(l: var Lowerer, root: SqlNode): CompiledStmt =
  let isUpdate = root.kind == nkUpdate
  let tableName = nodeIdent(root[0])
  let tbl = l.mustTable(tableName)
  l.tbl = some(tbl)

  var setPairs: seq[(string, SqlNode)]
  var whereNode: SqlNode = nil
  var colsJson = ""

  if isUpdate:
    for child in root.sons:
      case child.kind
      of nkSelectColumns:
        for asgn in child.sons:
          if asgn.kind != nkAsgn or asgn.len < 2:
            fail("UPDATE SET expects column = value pairs")
          let colName = asgn[0].nodeIdent()
          if colName.len == 0:
            fail("invalid SET target")
          if tbl.findColumn(colName).isNone:
            fail("no such column: '" & colName & "' in table '" & tableName & "'")
          setPairs.add((colName, asgn[1]))
      of nkWhere: whereNode = child[0]
      else: discard
    colsJson = $(%*setPairs.mapIt(it[0]))
  else:
    for child in root.sons:
      if child.kind == nkWhere:
        whereNode = child[0]

  l.genScanLoopPrelude(tableName)

  let loopTop = l.here()
  l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
  l.callF("sql_scan_next")
  let holeLoopEnd = l.jmpFwd(opcJumpFwdF)
  l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  var holeCont = -1
  if not whereNode.isNil:
    l.genExpr(whereNode)
    holeCont = l.jmpFwd(opcJumpFwdF)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  if isUpdate:
    for pair in setPairs:
      l.genExpr(pair[1])
      l.callF("sql_cell_push")
    l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
    discard l.pushS(colsJson)
    l.callF("sql_update_row")
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
  else:
    l.ch.emit(opcPushL); l.ch.emit(CtxSlot)
    l.callF("sql_delete_row")
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)

  l.jmpBack(loopTop)

  if holeCont >= 0:
    let contLabel = l.here()
    l.patchFwd(holeCont, contLabel)
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.jmpBack(loopTop)

  let endLabel = l.here()
  l.patchFwd(holeLoopEnd, endLabel)
  l.ch.emit(opcDiscard); l.ch.emit(1'u8)
  l.genScanTail()
  l.ch.emit(opcHalt)

  CompiledStmt(
    kind: if isUpdate: skUpdate else: skDelete,
    script: l.script,
    chunk: l.ch,
    placeholders: l.placeholders
  )

proc compileInsert(l: var Lowerer, root: SqlNode): CompiledStmt =
  let tableName = nodeIdent(root[0])
  let tbl = l.mustTable(tableName)
  l.tbl = some(tbl)

  var colsNode: SqlNode = nil
  var valuesNode: SqlNode = nil
  for child in root.sons:
    case child.kind
    of nkColumnList: colsNode = child
    of nkValueList: valuesNode = child
    else: discard
  if valuesNode.isNil:
    fail("INSERT requires a VALUES clause")

  # explicit column list, otherwise every non-pk column for serial tables
  var colNames: seq[string]
  if not colsNode.isNil:
    for c in colsNode.sons:
      let nm = c.nodeIdent()
      if tbl.findColumn(nm).isNone:
        fail("unknown column '" & nm & "' in table '" & tableName & "'")
      colNames.add(nm)
  else:
    for c in tbl.columns:
      if not (tbl.pkMode == pkmSerial and c.name == tbl.primaryKey):
        colNames.add(c.name)
  let colsJson = $(%*colNames)

  for row in valuesNode.sons:
    if row.len != colNames.len:
      fail("insert has " & $row.len & " values but " & $colNames.len &
           " columns were expected")
    for i, valNode in row.sons:
      l.genExpr(valNode)
      l.callF("sql_cell_push")
    discard l.pushS(tableName)
    discard l.pushS(colsJson)
    discard l.pushS("")
    l.callF("sql_insert")
    # keep only the last row's pk on the stack
    if row != valuesNode[^1]:
      l.ch.emit(opcDiscard); l.ch.emit(1'u8)
  l.ch.emit(opcHalt)

  CompiledStmt(
    kind: skInsert,
    script: l.script,
    chunk: l.ch,
    placeholders: l.placeholders
  )

proc compileDdl(l: var Lowerer, root: SqlNode): CompiledStmt =
  case root.kind
  of nkCreateTable, nkCreateTableIfNotExists:
    let name = nodeIdent(root[0])
    var pkCol = ""
    var serial = false
    var colSpecs = newJArray()
    for child in root.sons[1 ..^ 1]:
      if child.kind != nkColumnDef:
        continue
      let colName = nodeIdent(child[0])
      let typeName = nodeIdent(child[1])
      let kind = sqlTypeToKind(typeName)
      var nullable = true
      var hasPk = false
      var defaultJson: JsonNode = nil
      for con in child.sons[2 ..^ 1]:
        case con.kind
        of nkPrimaryKey:
          hasPk = true
        of nkNotNull:
          nullable = false
        of nkDefault:
          case con[0].kind
          of nkIntegerLit: defaultJson = %*parseBiggestInt(con[0].strVal)
          of nkNumericLit: defaultJson = %*parseFloat(con[0].strVal)
          of nkStringLit: defaultJson = %*con[0].strVal
          of nkIdent:
            case con[0].strVal.toLowerAscii()
            of "true": defaultJson = %*true
            of "false": defaultJson = %*false
            of "null": defaultJson = newJNull()
            else: fail("unsupported DEFAULT value")
          else: fail("unsupported DEFAULT value")
        else: discard
      if hasPk:
        if pkCol.len > 0:
          fail("only a single PRIMARY KEY column is supported")
        pkCol = colName
        serial = kind == dtInt
        # a PRIMARY KEY column is implicitly NOT NULL
        nullable = false
      var spec = %*{"name": colName, "kind": $kind, "nullable": nullable}
      if defaultJson != nil:
        spec["default"] = defaultJson
      colSpecs.add(spec)
    if pkCol.len == 0:
      fail("CREATE TABLE requires a PRIMARY KEY column")
    let schemaJson = $(%*{
      "name": name, "pk": pkCol, "serial": serial, "cols": colSpecs})
    discard l.pushS(schemaJson)
    l.callF("sql_create_table")
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.ch.emit(opcHalt)
    CompiledStmt(
      kind: skCreateTable,
      script: l.script,
      chunk: l.ch,
      placeholders: l.placeholders
    )
  of nkDropTable, nkDropTableIfExists:
    let name = nodeIdent(root[0])
    let ifExists = uint8(ord(root.kind == nkDropTableIfExists))
    discard l.pushS(name)
    l.ch.emit(opcPushI)
    l.ch.emit(int64(ifExists))
    l.callF("sql_drop_table")
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.ch.emit(opcHalt)
    CompiledStmt(
      kind: skDropTable,
      script: l.script,
      chunk: l.ch,
      placeholders: l.placeholders
    )
  of nkCreateIndex, nkCreateIndexIfNotExists:
    # CREATE INDEX name ON table (col)
    let tableName = nodeIdent(root[1])
    let tbl = l.mustTable(tableName)
    if root[2].kind != nkColumnList or root[2].len == 0:
      fail("CREATE INDEX requires a column list")
    let colName = nodeIdent(root[2][0])
    if tbl.findColumn(colName).isNone:
      fail("unknown column '" & colName & "' in table '" & tableName & "'")
    discard l.pushS(tableName)
    discard l.pushS(colName)
    l.callF("sql_create_index")
    l.ch.emit(opcDiscard); l.ch.emit(1'u8)
    l.ch.emit(opcHalt)
    CompiledStmt(
      kind: skCreateIndex,
      script: l.script,
      chunk: l.ch,
      placeholders: l.placeholders
    )
  else:
    fail("unsupported DDL statement")

#
# engine / execution
#
proc newSqlEngine*(store: Store): SqlEngine =
  ## Creates a SQL engine bound to `store`. The store must be opened with the
  ## plain (non-concurrent) constructor.
  if store.isConcurrent():
    fail("SQL engine does not support concurrent stores")
  SqlEngine(
    ctx: SqlExecCtx(store: store),
    vm: newVm()
  )

proc runCompiled(eng: SqlEngine, stmt: CompiledStmt,
                 params: openArray[Value]): Value =
  # seed placeholder globals; unbound ones become NULL
  for i, g in stmt.placeholders:
    eng.vm.globals[g] =
      if i < params.len: params[i]
      else: nil
  eng.ctx.scans.setLen(0)
  eng.ctx.results.setLen(0)
  eng.ctx.cells.setLen(0)
  eng.ctx.affected = 0
  when defined(boogieSqlDebug):
    echo "[run] procs=", stmt.script.procs.len,
         " code=", stmt.chunk.code.len, " strings=", stmt.chunk.strings
    var i = 0
    while i < stmt.chunk.code.len:
      stdout.write "  ", int(stmt.chunk.code[i])
      inc i
    stdout.write "\n"
  eng.vm.interpret(stmt.script, stmt.chunk)

proc postProcessSelect(eng: SqlEngine, stmt: CompiledStmt): ExecResult =
  var rows = eng.ctx.results
  when defined(boogieSqlDebug):
    echo "[post] raw results=", rows.len, " castptr=", cast[uint](eng.ctx)
    for r in rows:
      echo "[post] row cells=", r.len
  if stmt.orderKeys.len > 0:
    var indexed = newSeq[(int, seq[Value])](rows.len)
    for i, r in rows: indexed[i] = (i, r)
    indexed.sort(proc(a, b: (int, seq[Value])): int =
      for (idx, desc) in stmt.orderKeys:
        var c = cmpCells(a[1][idx], b[1][idx])
        if desc: c = -c
        if c != 0: return c
      cmp(a[0], b[0]))
    rows = indexed.mapIt(it[1])
  if stmt.isDistinct:
    var seen = initTable[string, bool]()
    var uniq: seq[seq[Value]]
    for r in rows:
      var key = ""
      for i in 0 ..< stmt.projCols.len:
        key.add(cellToString(r[i]) & "\x1f")
      if not seen.hasKey(key):
        seen[key] = true
        uniq.add(r)
    rows = uniq
  # deferred LIMIT/OFFSET (the ordered case applies them after sorting)
  if stmt.deferOffset > 0 and stmt.deferOffset < rows.len.int64:
    rows = rows[int(stmt.deferOffset) .. ^1]
  elif stmt.deferOffset > 0:
    rows = @[]
  if stmt.deferLimit >= 0 and stmt.deferLimit < rows.len.int64:
    rows = rows[0 ..< int(stmt.deferLimit)]
  var outRows: seq[seq[string]]
  for r in rows:
    var srow = newSeqOfCap[string](stmt.projCols.len)
    for i in 0 ..< stmt.projCols.len:
      srow.add(cellToString(r[i]))
    outRows.add(srow)
  when defined(boogieSqlDebug):
    echo "[post] outRows=", outRows.len,
         " cell0=", (if outRows.len > 0: outRows[0][0] else: "-"),
         " projCols=", stmt.projCols.len
  ExecResult(
    kind: skSelect,
    columns: stmt.projCols,
    rows: outRows,
    affected: int64(outRows.len)
  )

proc compileStmts(eng: SqlEngine, sqlText: string): seq[CompiledStmt]
  ## Parses and lowers one SQL string into executable statements

proc placeholderCount*(eng: SqlEngine, sqlText: string): int =
  ## Compiles (or fetches from cache) a statement and returns how many
  ## positional placeholders it contains.
  if not eng.cache.hasKey(sqlText):
    eng.cache[sqlText] = eng.compileStmts(sqlText)
  eng.cache[sqlText][^1].placeholders.len

proc compileStmts(eng: SqlEngine, sqlText: string): seq[CompiledStmt] =
  ## Parses and lowers one SQL string into executable statements
  var compiled: seq[CompiledStmt]
  let root = parseSql(sqlText)
  when defined(boogieSqlDebug):
    echo "[compile] root kind=", $root.kind, " sons=", root.sons.len
  let roots =
    if root.kind == nkStmtList: root.sons
    else: @[root]
  for r in roots:
    when defined(boogieSqlDebug):
      echo "[compile] stmt kind=", $r.kind
    var l = initLowerer(eng, sqlText)
    let stmt =
      case r.kind
      of nkSelect, nkSelectDistinct: l.compileSelect(r)
      of nkInsert: l.compileInsert(r)
      of nkUpdate: l.compileUpdateDelete(r)
      of nkDelete: l.compileUpdateDelete(r)
      of nkCreateTable, nkCreateTableIfNotExists,
         nkDropTable, nkDropTableIfExists,
         nkCreateIndex, nkCreateIndexIfNotExists: l.compileDdl(r)
      else: fail("unsupported SQL statement")
    compiled.add(stmt)
  compiled

const MaxCachedStatements = 256
  ## Interpolated queries produce unique text per execution, so the plan cache
  ## needs a ceiling; on overflow it resets wholesale (plans are cheap to
  ## rebuild relative to unbounded memory growth).
proc execSql*(eng: SqlEngine, sqlText: string,
              params: openArray[Value] = []): ExecResult =
  ## Parses (or fetches from cache), compiles and executes one SQL string.
  ## Multiple semicolon-separated statements run sequentially; the last
  ## result is returned.
  if not eng.cache.hasKey(sqlText):
    if eng.cache.len >= MaxCachedStatements:
      eng.cache.clear()
    eng.cache[sqlText] = eng.compileStmts(sqlText)

  let stmts = eng.cache[sqlText]
  when defined(boogieSqlDebug):
    echo "[exec] stmts=", stmts.len
  for i, stmt in stmts:
    when defined(boogieSqlDebug):
      echo "[exec] running #", i, " kind=", $stmt.kind
    let last = i == stmts.len - 1
    let raw =
      if last: eng.runCompiled(stmt, params)
      else: eng.runCompiled(stmt, [])
    case stmt.kind
    of skSelect:
      result = eng.postProcessSelect(stmt)
    of skInsert:
      result = ExecResult(kind: skInsert, affected: eng.ctx.affected)
      if last and raw != nil and raw.typeId == tyString:
        result.lastPk = raw.stringVal[]
    of skUpdate, skDelete:
      result = ExecResult(
        kind: stmt.kind,
        affected: eng.ctx.affected
      )
    of skCreateTable, skDropTable, skCreateIndex:
      result = ExecResult(kind: stmt.kind, affected: eng.ctx.affected)
      # schema changed: cached plans may embed stale projections/column lists
      eng.cache.clear()

