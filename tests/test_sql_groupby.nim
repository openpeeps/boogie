import std/[unittest, os, times, monotimes, sequtils, strutils, tables, options]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# P0: GROUP BY / HAVING + secondary indexes (named, UNIQUE, persisted).
# ---------------------------------------------------------------------------

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_p0_tests_" & unique)
  createDir(result)

proc seedGroups(db: DbConn) =
  db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INT)")
  for i in 1..10:
    let k = if i mod 2 == 0: "even" else: "odd"
    db.exec(sql"INSERT INTO t (k, v) VALUES (?, ?)", k, $i)

suite "GROUP BY basics":

  test "groups with count/sum/min/max/avg":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k, COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM t GROUP BY k ORDER BY k")
    check rows.len == 2
    # even: 2+4+6+8+10=30 ; odd: 1+3+5+7+9=25
    check rows[0][0] == "even"
    check rows[0][1] == "5"
    check rows[0][2] == "30"
    check rows[0][3] == "2"
    check rows[0][4] == "10"
    check rows[1][0] == "odd"
    check rows[1][1] == "5"
    check rows[1][2] == "25"
    db.close()

  test "NULL keys group together":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INT)")
    db.exec(sql"INSERT INTO t (k, v) VALUES ('a', 1)")
    db.exec(sql"INSERT INTO t (k, v) VALUES (NULL, 2)")
    db.exec(sql"INSERT INTO t (k, v) VALUES (NULL, 3)")
    db.exec(sql"INSERT INTO t (k, v) VALUES ('a', 4)")
    let rows = db.getAllRows(sql"SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k ORDER BY COUNT(*) DESC")
    check rows.len == 2
    # both groups have 2 rows; order between ties is group-creation order
    var seen = initTable[string, string]()
    for r in rows:
      seen[r[0]] = r[1] & "|" & r[2]
    check seen["a"] == "2|5"
    check seen[""] == "2|5"
    db.close()

  test "WHERE + GROUP BY filters before grouping":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k, COUNT(*) FROM t WHERE v > 5 GROUP BY k ORDER BY k")
    check rows.len == 2
    check rows[0] == @["even", "3"] # 6,8,10
    check rows[1] == @["odd", "2"] # 7,9
    db.close()

  test "aliases and ORDER BY over outputs with LIMIT":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k AS key, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY k ORDER BY s DESC LIMIT 1")
    check rows.len == 1
    check rows[0][0] == "even"
    check rows[0][1] == "5"
    check rows[0][2] == "30"
    db.close()

  test "empty table yields no groups":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INT)")
    check db.getAllRows(sql"SELECT k, COUNT(*) FROM t GROUP BY k").len == 0
    db.close()

suite "HAVING":

  test "having on SELECT aggregate":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 4 ORDER BY k")
    check rows.len == 2
    let none = db.getAllRows(sql"SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 5 ORDER BY k")
    check none.len == 0
    db.close()

  test "having-only aggregate not in SELECT list":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k FROM t GROUP BY k HAVING SUM(v) > 26 ORDER BY k")
    check rows.len == 1
    check rows[0][0] == "even"
    db.close()

  test "having without GROUP BY is a global group":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    check db.getAllRows(sql"SELECT COUNT(*) FROM t HAVING COUNT(*) = 10").len == 1
    check db.getAllRows(sql"SELECT COUNT(*) FROM t HAVING COUNT(*) > 100").len == 0
    # empty table: COUNT(*) = 0 still evaluates
    db.exec(sql"CREATE TABLE e (id INTEGER PRIMARY KEY, v INT)")
    check db.getValue(sql"SELECT COUNT(*) FROM e HAVING COUNT(*) = 0") == "0"
    check db.getAllRows(sql"SELECT COUNT(*) FROM e HAVING COUNT(*) > 0").len == 0
    db.close()

  test "having with placeholder":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let ps = db.prepare("SELECT k FROM t GROUP BY k HAVING SUM(v) > ? ORDER BY k")
    ps.bindParam(1, 26)
    let rows = db.getAllRows(ps)
    check rows.len == 1
    check rows[0][0] == "even"
    db.close()

  test "having on group key and AND nesting":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    let rows = db.getAllRows(sql"SELECT k, COUNT(*) FROM t GROUP BY k HAVING k = 'even' AND COUNT(*) = 5")
    check rows.len == 1
    check rows[0][0] == "even"
    db.close()

  test "multi-statement GROUP BY runs on the VM path":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT, v INT)")
    for i in 1..6:
      let k = if i mod 2 == 0: "even" else: "odd"
      discard eng.execSql("INSERT INTO t (k, v) VALUES ('" & k & "', " & $i & ")")
    let res = eng.execSql("SELECT id FROM t WHERE id < 0; SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c >= 3 ORDER BY k")
    check res.columns == @["k", "c"]
    check res.rows.len == 2
    check res.rows[0][0] == "even"
    check res.rows[1][0] == "odd"
    store.close()

suite "GROUP BY errors":

  test "SELECT * with GROUP BY fails":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM t GROUP BY k")
    db.close()

  test "non-grouped column fails":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT v FROM t GROUP BY k")
    db.close()

  test "HAVING on non-group column fails":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT k FROM t GROUP BY k HAVING v > 1")
    db.close()

  test "ORDER BY non-output fails for grouped queries":
    let db = open(":memory:", "", "", "")
    seedGroups(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT k FROM t GROUP BY k ORDER BY v")
    db.close()

suite "secondary indexes":

  test "CREATE UNIQUE INDEX enforces uniqueness":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
    db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
    db.exec(sql"CREATE UNIQUE INDEX idx_email ON t (email)")
    expect DbError:
      db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
    # NULLs are exempt like in standard SQL
    db.exec(sql"INSERT INTO t (email) VALUES (NULL)")
    db.exec(sql"INSERT INTO t (email) VALUES (NULL)")
    # update to a duplicate fails
    db.exec(sql"INSERT INTO t (email) VALUES ('b@x.io')")
    expect DbError:
      db.exec(sql"UPDATE t SET email = 'a@x.io' WHERE email = 'b@x.io'")
    db.close()

  test "CREATE INDEX IF NOT EXISTS and DROP INDEX":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"CREATE INDEX idx_v ON t (v)")
    db.exec(sql"CREATE INDEX IF NOT EXISTS idx_v ON t (v)")
    check db.getValue(sql"SELECT v FROM t WHERE v = 1") == "1"
    db.exec(sql"DROP INDEX idx_v")
    check db.getValue(sql"SELECT v FROM t WHERE v = 1") == "1"
    db.exec(sql"DROP INDEX IF EXISTS idx_v")
    expect DbError:
      db.exec(sql"DROP INDEX idx_v")
    db.close()

  test "index stays consistent across post-index writes":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1), (2), (3)")
    db.exec(sql"CREATE INDEX idx_v ON t (v)")
    # inserts after the index build are visible through it
    for i in 4 .. 200:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    check db.getValue(sql"SELECT id FROM t WHERE v = 150") == "150"
    # deletes drop the pk from the index
    db.exec(sql"DELETE FROM t WHERE v = 150")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = 150") == "0"
    # updates move the pk between index buckets
    db.exec(sql"UPDATE t SET v = 999 WHERE v = 151")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = 151") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = 999") == "1"
    # rename of the indexed column keeps the index usable
    db.exec(sql"ALTER TABLE t RENAME COLUMN v TO w")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE w = 999") == "1"
    db.close()

  test "duplicate index names and composite rejection":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT)")
    db.exec(sql"CREATE TABLE u (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"CREATE INDEX idx_a ON t (a)")
    expect DbError:
      db.exec(sql"CREATE INDEX idx_a ON u (a)")
    expect DbError:
      db.exec(sql"CREATE INDEX idx_ab ON t (a, b)")
    db.close()

  test "unique index survives reopen, drop persists":
    let root = testRoot()
    block:
      let db = open(root / "idx.db", "", "", "")
      db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
      db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
      db.exec(sql"CREATE UNIQUE INDEX idx_email ON t (email)")
      db.close()
    block:
      let db = open(root / "idx.db", "", "", "")
      expect DbError:
        db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
      db.exec(sql"DROP INDEX idx_email")
      db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
      db.close()
    block:
      let db = open(root / "idx.db", "", "", "")
      # duplicate now allowed: index is gone
      db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
      check db.getValue(sql"SELECT COUNT(*) FROM t") == "3"
      db.close()

  test "unique index on existing duplicates fails cleanly":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
    db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
    db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
    expect DbError:
      db.exec(sql"CREATE UNIQUE INDEX idx_email ON t (email)")
    # failed CREATE left no index and no WAL poison: table still usable
    db.exec(sql"INSERT INTO t (email) VALUES ('b@x.io')")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "3"
    # plain index on duplicates is fine
    db.exec(sql"CREATE INDEX idx_email ON t (email)")
    check db.getValue(sql"SELECT email FROM t WHERE email = 'a@x.io'") == "a@x.io"
    db.close()

  test "index DDL via multi-statement VM path":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    discard eng.execSql("INSERT INTO t (v) VALUES (1)")
    discard eng.execSql("INSERT INTO t (v) VALUES (2)")
    let r = eng.execSql("CREATE UNIQUE INDEX idx_v ON t (v); SELECT v FROM t WHERE v = 2")
    check r.rows == @[@["2"]]
    expect StoreError:
      discard eng.execSql("INSERT INTO t (v) VALUES (2)")
    discard eng.execSql("DROP INDEX idx_v; SELECT COUNT(*) FROM t")
    discard eng.execSql("INSERT INTO t (v) VALUES (2)")
    let c = eng.execSql("SELECT COUNT(*) FROM t")
    check c.rows == @[@["3"]]
    store.close()

  test "store-level named index catalog":
    let s = newInMemoryStore()
    s.createTable(newTable("t", "id",
      [newColumn("id", dtInt, false), newColumn("v", dtInt, true)]))
    s.createIndex("t", "idx_v", ["v"])
    check s.getTable("t").get().hasIndex("idx_v")
    check s.getTable("t").get().listIndexes().len == 1
    discard s.insertRow("t", row({"v": newIntValue(1)}))
    expect StoreError:
      s.createIndex("t", "idx_v", ["v"])
    s.dropIndex("idx_v")
    check not s.getTable("t").get().hasIndex("idx_v")
    expect StoreError:
      s.dropIndex("idx_v")
    s.close()
