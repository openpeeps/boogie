# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

import std/strutils

import db_connector/db_common
export db_common

import ./sqlengine

## This module implements a `db_connector <https://nim-lang.github.io/Nim/db_conn>`_
## compatible driver for Boogie's RDBMS store, so applications can switch from
## `db_connector/db_sqlite` to Boogie by changing a single import:
##
## .. code-block:: nim
##   import boogie/db_boogie
##
##   let db = open("mydb", "", "", "app")
##   db.exec(sql"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
##   db.exec(sql"INSERT INTO users (name) VALUES (?)", "alice")
##   for row in db.fastRows(sql"SELECT * FROM users"):
##     echo row
##   echo db.getValue(sql"SELECT name FROM users WHERE id = ?", 1)
##
## The `connection` argument of `open` is the store path (use ":memory:" for an
## in-memory store); `user`, `password` and `database` are accepted for API
## compatibility and ignored.
##
## SQL statements are compiled to vancode bytecode and evaluated by the vancode
## VM; see `boogie/sqlengine` for the supported statement subset. Statements run
## through this driver are logged in the store's WAL and survive restarts.

type
  Row* = seq[string]
    ## A row of a dataset. NULL values are empty strings, matching the
    ## db_connector backends.

  DbConn* = ref object
    ## A connection to a Boogie SQL database
    store: Store
    eng: SqlEngine

  SqlPrepared* = distinct int
    ## A prepared statement handle. Unlike the SQLite backend, statements here
    ## are precompiled bytecode; placeholders (`?`) bind positionally via
    ## `bindParam`.

  InstantRow* = distinct seq[string]
    ## A row of a dataset returned by `instantRows`

proc newDbConn(store: Store): DbConn =
  result = DbConn(store: store)
  result.eng = newSqlEngine(store)

proc open*(connection, user, password, database: string): DbConn =
  ## Opens a database. `connection` is the store path; passing ":memory:"
  ## creates a non-persistent in-memory store. The remaining arguments exist
  ## for API compatibility with other db_connector drivers and are ignored.
  try:
    if connection == ":memory:":
      result = newDbConn(newInMemoryStore())
    else:
      result = newDbConn(newStore(connection))
  except CatchableError as e:
    dbError(e.msg)

proc close*(db: DbConn) =
  ## Closes the database, flushing pending WAL writes and taking a checkpoint
  if db != nil:
    db.store.checkpoint()
    db.store.close()

proc dbQuote(s: string): string =
  ## Escapes a string for direct interpolation into a query
  result = "'"
  for c in items(s):
    if c == '\'': result.add("''")
    else: result.add(c)
  result.add('\'')

proc dbFormat(formatstr: SqlQuery, args: varargs[string]): string =
  ## Interpolates `?` placeholders into the query text, mirroring the SQLite
  ## backend behavior of `exec`
  result = ""
  var argIndex = 0
  var i = 0
  let f = formatstr.string
  while i < f.len:
    if f[i] == '?':
      if i + 1 < f.len and f[i + 1] == '?':
        result.add('?')
        inc(i)
      elif argIndex < args.len:
        result.add(dbQuote(args[argIndex]))
        inc(argIndex)
      else:
        dbError("missing query parameter for placeholder at " & $i)
    else:
      result.add(f[i])
    inc(i)

proc runSql(db: DbConn, queryText: string,
            params: varargs[string]): ExecResult =
  when defined(boogieSqlDebug):
    echo "[runSql] ", queryText
  try:
    let finalSql =
      if params.len > 0: dbFormat(SqlQuery(queryText), params)
      else: queryText
    when defined(boogieSqlDebug):
      echo "[runSql->] ", finalSql
    db.eng.execSql(finalSql, [])
  except CatchableError as e:
    dbError(e.msg)

proc exec*(db: DbConn, query: SqlQuery, args: varargs[string, `$`]) =
  ## Executes a statement. Args replace `?` placeholders and are quoted.
  discard db.runSql(query.string, args)

proc tryExec*(db: DbConn, query: SqlQuery,
              args: varargs[string, `$`]): bool =
  ## Like `exec` but returns false instead of raising on error
  try:
    discard db.runSql(query.string, args)
    true
  except DbError:
    false

proc newRow*(L: int): Row =
  ## Returns a row of `L` empty columns (compatibility helper)
  newSeq(result, L)

proc getAllRows*(db: DbConn, query: SqlQuery,
                 args: varargs[string, `$`]): seq[Row] =
  ## Runs a SELECT and returns all rows
  db.runSql(query.string, args).rows

proc getRow*(db: DbConn, query: SqlQuery, args: varargs[string, `$`]): Row =
  ## Runs a SELECT and returns the first row. When no rows match, a row filled
  ## with empty strings is returned (matching the SQLite backend).
  let res = db.runSql(query.string, args)
  if res.rows.len > 0:
    res.rows[0]
  else:
    newRow(res.columns.len)

proc getValue*(db: DbConn, query: SqlQuery, args: varargs[string, `$`]): string =
  ## Runs a SELECT and returns the first column of the first row, or "" when
  ## there is no result or the cell is NULL
  let res = db.runSql(query.string, args)
  if res.rows.len > 0 and res.rows[0].len > 0:
    res.rows[0][0]
  else:
    ""

iterator fastRows*(db: DbConn, query: SqlQuery,
                   args: varargs[string, `$`]): Row =
  ## Iterates over the rows produced by a SELECT. Rows are materialized
  ## eagerly before iteration starts (embedded engine, no server-side cursor).
  let all = db.runSql(query.string, args).rows
  for r in all:
    yield r

iterator rows*(db: DbConn, query: SqlQuery,
               args: varargs[string, `$`]): Row =
  ## Alias for `fastRows`
  for r in db.fastRows(query, args):
    yield r

iterator instantRows*(db: DbConn, query: SqlQuery,
                      args: varargs[string, `$`]): InstantRow =
  ## Like `fastRows` but yields `InstantRow` handles
  let all = db.runSql(query.string, args).rows
  for r in all:
    yield InstantRow(r)

proc `[]`*(row: InstantRow, col: int32): string {.inline.} =
  ## Returns the text of the given column of an instant row
  (seq[string])(row)[int(col)]

proc len*(row: InstantRow): int32 {.inline.} =
  ## Returns the number of columns of an instant row
  int32((seq[string])(row).len)

proc lastPkToInt64(res: ExecResult): int64 =
  ## Extracts the integer primary key from an INSERT result
  if res.lastPk.len == 0:
    dbError("insertID: no primary key was generated")
  try:
    parseBiggestInt(res.lastPk)
  except ValueError:
    dbError("insertID: primary key is not an integer: " & res.lastPk)

proc insertID*(db: DbConn, query: SqlQuery, args: varargs[string, `$`]): int64 =
  ## Executes an INSERT and returns the generated primary key of the inserted
  ## row. Requires an INTEGER PRIMARY KEY table.
  lastPkToInt64(db.runSql(query.string, args))

proc tryInsertID*(db: DbConn, query: SqlQuery,
                  args: varargs[string, `$`]): int64 =
  ## Like `insertID` but returns -1 instead of raising on error
  try:
    db.insertID(query, args)
  except DbError:
    -1'i64

proc execAffectedRows*(db: DbConn, query: SqlQuery,
                       args: varargs[string, `$`]): int64 =
  ## Returns the number of rows affected by the executed DML statement
  db.runSql(query.string, args).affected

proc setEncoding*(connection: DbConn, encoding: string): bool =
  ## Always succeeds when requesting UTF-8 (the only supported encoding)
  case encoding.toUpperAscii()
  of "UTF-8", "UTF8": true
  else: false

#
# prepared statements
#
type
  PreparedEntry = object
    sql: string
    params: seq[SqlValue]
      ## typed bindings, nil entries represent unbound/NULL placeholders

var preparedSeq {.global.}: seq[PreparedEntry] = @[]
  ## Process-wide registry backing `SqlPrepared` handles

proc prepare*(db: DbConn, q: string): SqlPrepared =
  ## Precompiles a statement. Placeholders stay symbolic; values are attached
  ## per execution with `bindParam`. Compilation errors raise immediately.
  try:
    # compile now so syntax errors surface at prepare time
    discard db.eng.placeholderCount(q)
  except CatchableError as e:
    dbError(e.msg)
  preparedSeq.add(PreparedEntry(sql: q))
  SqlPrepared(preparedSeq.len - 1)

proc finalize*(sqlPrepared: SqlPrepared) {.discardable.} =
  ## Releases the prepared statement handle. Existing handles remain valid
  ## (they are plain integers) but are no longer tracked.
  discard

proc bindAt(ps: SqlPrepared, paramIdx: int, val: SqlValue) =
  ## Assigns a typed binding, growing the slot list as needed. Unset slots
  ## stay nil which the engine treats as NULL.
  let entry = addr preparedSeq[int(ps)]
  while entry[].params.len < paramIdx:
    entry[].params.add(nil)
  entry[].params[paramIdx - 1] = val

proc bindParam*(ps: SqlPrepared, paramIdx: int, val: int32) =
  ## Binds an int32 value to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, initValue(int64(val)))

proc bindParam*(ps: SqlPrepared, paramIdx: int, val: int64) =
  ## Binds an int64 value to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, initValue(val))

proc bindParam*(ps: SqlPrepared, paramIdx: int, val: int) =
  ## Binds an int value to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, initValue(int64(val)))

proc bindParam*(ps: SqlPrepared, paramIdx: int, val: float64) =
  ## Binds a float value to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, initValue(val))

proc bindParam*(ps: SqlPrepared, paramIdx: int, val: string, copy = true) =
  ## Binds a string value to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, initValue(val))

proc bindNull*(ps: SqlPrepared, paramIdx: int) =
  ## Binds NULL to the placeholder at `paramIdx` (1-based)
  bindAt(ps, paramIdx, nil)

proc runPrepared(db: DbConn, ps: SqlPrepared): ExecResult =
  try:
    let entry = preparedSeq[int(ps)]
    db.eng.execSql(entry.sql, entry.params)
  except CatchableError as e:
    dbError(e.msg)

proc exec*(db: DbConn, stmtName: SqlPrepared) =
  ## Executes a previously prepared statement with its bound parameters
  discard db.runPrepared(stmtName)

proc tryExec*(db: DbConn, stmtName: SqlPrepared): bool =
  ## Like `exec(prepared)` but returns false instead of raising
  try:
    discard db.runPrepared(stmtName)
    true
  except DbError:
    false

proc getAllRows*(db: DbConn, stmtName: SqlPrepared): seq[Row] =
  db.runPrepared(stmtName).rows

proc getRow*(db: DbConn, stmtName: SqlPrepared): Row =
  let res = db.runPrepared(stmtName)
  if res.rows.len > 0: res.rows[0] else: newRow(res.columns.len)

proc getValue*(db: DbConn, stmtName: SqlPrepared): string =
  let res = db.runPrepared(stmtName)
  if res.rows.len > 0 and res.rows[0].len > 0: res.rows[0][0] else: ""

iterator fastRows*(db: DbConn, stmtName: SqlPrepared): Row =
  let all = db.runPrepared(stmtName).rows
  for r in all:
    yield r

iterator rows*(db: DbConn, stmtName: SqlPrepared): Row =
  for r in db.fastRows(stmtName):
    yield r

iterator instantRows*(db: DbConn, stmtName: SqlPrepared): InstantRow =
  let all = db.runPrepared(stmtName).rows
  for r in all:
    yield InstantRow(r)

proc insertID*(db: DbConn, stmtName: SqlPrepared): int64 =
  lastPkToInt64(db.runPrepared(stmtName))

proc execAffectedRows*(db: DbConn, stmtName: SqlPrepared): int64 =
  db.runPrepared(stmtName).affected

proc insert*(db: DbConn, query: SqlQuery, pkName: string,
             args: varargs[string, `$`]): int64 =
  ## Executes an INSERT and returns the generated primary key. `pkName` exists
  ## for API compatibility and is ignored; the primary key is inferred from the
  ## table schema.
  db.insertID(query, args)

proc tryInsert*(db: DbConn, query: SqlQuery, pkName: string,
                args: varargs[string, `$`]): int64 =
  ## Like `insert` but returns -1 instead of raising on error
  try:
    db.insertID(query, args)
  except DbError:
    -1'i64
