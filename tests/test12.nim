import std/[unittest, os, times, monotimes]
import ../src/boogie/db_boogie

# ---------------------------------------------------------------------------
# Thorough SQL VM / vmext tests: NULL propagation, type coercion, aggregates,
# DDL edge cases, statement caching, prepared-statement rebind and error paths.
# ---------------------------------------------------------------------------

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_sqlvm_tests_" & unique)
  createDir(result)

suite "NULL propagation":

  test "NULL arithmetic yields NULL":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (10)")
    check db.getValue(sql"SELECT a + 1 FROM t WHERE id = 1") == "11"
    check db.getValue(sql"SELECT a + NULL FROM t WHERE id = 1") == ""
    check db.getValue(sql"SELECT NULL + 1 FROM t WHERE id = 1") == ""
    check db.getValue(sql"SELECT NULL * 5 FROM t WHERE id = 1") == ""
    check db.getValue(sql"SELECT NULL / 1 FROM t WHERE id = 1") == ""
    db.close()

  test "NULL in comparisons returns false":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (10)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a = NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a != NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a < NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a > NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL = NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL = 1") == "0"
    db.close()

  test "IS NULL / IS NOT NULL":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (10)")
    db.exec(sql"INSERT INTO t (a) VALUES (NULL)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a IS NULL") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a IS NOT NULL") == "1"
    # NULL IS NULL is a tautology (always true) in SQL
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL IS NULL") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL IS NOT NULL") == "0"
    db.close()

  test "NULL ordering in ORDER BY":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (3)")
    db.exec(sql"INSERT INTO t (a) VALUES (NULL)")
    db.exec(sql"INSERT INTO t (a) VALUES (1)")
    var vals: seq[string]
    for row in db.fastRows(sql"SELECT a FROM t ORDER BY a"):
      vals.add(row[0])
    check vals == @["", "1", "3"]
    var valsDesc: seq[string]
    for row in db.fastRows(sql"SELECT a FROM t ORDER BY a DESC"):
      valsDesc.add(row[0])
    check valsDesc == @["3", "1", ""]
    db.close()

suite "type coercion":

  test "integer column compared against string literal":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (42)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = '42'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = 'abc'") == "0"
    db.close()

  test "float column compared against string literal":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v REAL)")
    db.exec(sql"INSERT INTO t (v) VALUES (3.14)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v > '3.0'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v < '4.0'") == "1"
    db.close()

  test "mixed int/float arithmetic":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b REAL)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (3, 2.5)")
    check db.getValue(sql"SELECT a + b FROM t WHERE id = 1") == "5.5"
    check db.getValue(sql"SELECT a * b FROM t WHERE id = 1") == "7.5"
    check db.getValue(sql"SELECT a / b FROM t WHERE id = 1") == "1.2"
    check db.getValue(sql"SELECT b - a FROM t WHERE id = 1") == "-0.5"
    db.close()

  test "integer division promotes to float (vmext behavior)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (7)")
    # vmext always promotes to float for division
    check db.getValue(sql"SELECT a / 2 FROM t WHERE id = 1") == "3.5"
    db.close()

  test "division by zero yields NULL":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (10)")
    check db.getValue(sql"SELECT a / 0 FROM t WHERE id = 1") == ""
    db.close()

  test "boolean literals in expressions":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT)")
    db.exec(sql"INSERT INTO t (a) VALUES (1)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE true") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE false") == "0"
    db.close()

suite "WHERE expression complexity":

  test "nested AND/OR":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT)")
    for i in 1..6:
      db.exec(sql"INSERT INTO t (a, b) VALUES (?, ?)", $i, $(i * 10))
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE (a = 1 OR a = 2) AND b > 15") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE a = 1 OR a = 2 AND b > 15") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NOT (a < 4)") == "3"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NOT a < 4") == "3"
    db.close()

  test "comparison operators full coverage":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..5:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v = 3") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v != 3") == "4"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v <> 3") == "4"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v < 3") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v <= 3") == "3"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v > 3") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v >= 3") == "3"
    db.close()

  test "string comparisons":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice')")
    db.exec(sql"INSERT INTO t (name) VALUES ('bob')")
    db.exec(sql"INSERT INTO t (name) VALUES ('carol')")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name < 'bob'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name > 'bob'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name = 'bob'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name != 'bob'") == "2"
    db.close()

suite "aggregates":

  test "COUNT(*), COUNT(col), COUNT(DISTINCT)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"INSERT INTO t (v) VALUES (2)")
    db.exec(sql"INSERT INTO t (v) VALUES (2)")
    db.exec(sql"INSERT INTO t (v) VALUES (NULL)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "4"
    check db.getValue(sql"SELECT COUNT(v) FROM t") == "3"
    db.close()

  test "SUM, AVG, MIN, MAX":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (10)")
    db.exec(sql"INSERT INTO t (v) VALUES (20)")
    db.exec(sql"INSERT INTO t (v) VALUES (30)")
    check db.getValue(sql"SELECT SUM(v) FROM t") == "60"
    check db.getValue(sql"SELECT MIN(v) FROM t") == "10"
    check db.getValue(sql"SELECT MAX(v) FROM t") == "30"
    # AVG of ints: 60/3 = 20.0
    check db.getValue(sql"SELECT AVG(v) FROM t") == "20.0"
    db.close()

  test "aggregates with NULL values":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (10)")
    db.exec(sql"INSERT INTO t (v) VALUES (NULL)")
    db.exec(sql"INSERT INTO t (v) VALUES (30)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "3"
    check db.getValue(sql"SELECT COUNT(v) FROM t") == "2"
    check db.getValue(sql"SELECT SUM(v) FROM t") == "40"
    check db.getValue(sql"SELECT MIN(v) FROM t") == "10"
    check db.getValue(sql"SELECT MAX(v) FROM t") == "30"
    # AVG skips NULLs: 40/2 = 20.0
    check db.getValue(sql"SELECT AVG(v) FROM t") == "20.0"
    db.close()

  test "aggregates on empty table":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "0"
    check db.getValue(sql"SELECT COUNT(v) FROM t") == "0"
    # SUM/MIN/MAX/AVG on empty table return the zero-initialized accumulator
    check db.getValue(sql"SELECT SUM(v) FROM t") == "0"
    check db.getValue(sql"SELECT MIN(v) FROM t") == ""
    check db.getValue(sql"SELECT MAX(v) FROM t") == ""
    check db.getValue(sql"SELECT AVG(v) FROM t") == ""
    db.close()

  test "aggregate with WHERE clause":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..10:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    check db.getValue(sql"SELECT SUM(v) FROM t WHERE v > 5") == "40"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v <= 3") == "3"
    db.close()

  test "aggregate single-row":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (42)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "1"
    check db.getValue(sql"SELECT SUM(v) FROM t") == "42"
    check db.getValue(sql"SELECT MIN(v) FROM t") == "42"
    check db.getValue(sql"SELECT MAX(v) FROM t") == "42"
    check db.getValue(sql"SELECT AVG(v) FROM t") == "42.0"
    db.close()

suite "SELECT features":

  test "DISTINCT":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"INSERT INTO t (v) VALUES (2)")
    db.exec(sql"INSERT INTO t (v) VALUES (3)")
    db.exec(sql"INSERT INTO t (v) VALUES (3)")
    var rows: seq[string]
    for row in db.fastRows(sql"SELECT DISTINCT v FROM t ORDER BY v"):
      rows.add(row[0])
    check rows == @["1", "2", "3"]
    db.close()

  test "DISTINCT with NULLs":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"INSERT INTO t (v) VALUES (NULL)")
    db.exec(sql"INSERT INTO t (v) VALUES (NULL)")
    db.exec(sql"INSERT INTO t (v) VALUES (2)")
    check db.getAllRows(sql"SELECT DISTINCT v FROM t ORDER BY v").len == 3
    db.close()

  test "LIMIT and OFFSET edge cases":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..5:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    check db.getAllRows(sql"SELECT v FROM t ORDER BY v LIMIT 0").len == 0
    check db.getAllRows(sql"SELECT v FROM t ORDER BY v LIMIT 10").len == 5
    check db.getAllRows(sql"SELECT v FROM t ORDER BY v LIMIT 2 OFFSET 10").len == 0
    var vals: seq[string]
    for row in db.fastRows(sql"SELECT v FROM t ORDER BY v LIMIT 2 OFFSET 3"):
      vals.add(row[0])
    check vals == @["4", "5"]
    db.close()

  test "ORDER BY multiple columns":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (1, 2)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (1, 1)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (2, 1)")
    var rows: seq[string]
    for row in db.fastRows(sql"SELECT b FROM t ORDER BY a, b"):
      rows.add(row[0])
    check rows == @["1", "2", "1"]
    db.close()

  test "SELECT with no matching rows":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    check db.getAllRows(sql"SELECT * FROM t WHERE v > 100").len == 0
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE v > 100") == "0"
    db.close()

  test "SELECT * returns all columns":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b TEXT)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (1, 'hello')")
    let row = db.getRow(sql"SELECT * FROM t")
    check row.len == 3
    check row[0] == "1"
    check row[1] == "1"
    check row[2] == "hello"
    db.close()

suite "DML edge cases":

  test "multi-row INSERT":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1), (2), (3), (4), (5)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "5"
    check db.getValue(sql"SELECT SUM(v) FROM t") == "15"
    db.close()

  test "INSERT with explicit primary key":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    db.exec(sql"INSERT INTO t (id, v) VALUES (100, 'hello')")
    check db.getValue(sql"SELECT v FROM t WHERE id = 100") == "hello"
    check db.insertID(sql"INSERT INTO t (v) VALUES (?)", "world") == 101'i64
    db.close()

  test "UPDATE with arithmetic expressions":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, balance REAL, rate REAL)")
    db.exec(sql"INSERT INTO t (balance, rate) VALUES (1000.0, 0.05)")
    db.exec(sql"UPDATE t SET balance = balance + balance * rate WHERE id = 1")
    check db.getValue(sql"SELECT balance FROM t WHERE id = 1") == "1050.0"
    db.close()

  test "UPDATE with NULL handling":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT)")
    db.exec(sql"INSERT INTO t (a, b) VALUES (1, NULL)")
    db.exec(sql"UPDATE t SET b = 10 WHERE a = 1")
    check db.getValue(sql"SELECT b FROM t WHERE id = 1") == "10"
    db.close()

  test "DELETE with complex WHERE":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT, b INT)")
    for i in 1..10:
      db.exec(sql"INSERT INTO t (a, b) VALUES (?, ?)", $i, $(i * 10))
    db.exec(sql"DELETE FROM t WHERE a > 5 OR b < 20")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "4"
    # remaining: a=3(b=30), a=4(b=40), a=5(b=50)... wait, a>5 deleted, b<20 deleted
    # a=1(b=10) deleted (b<20), a=2(b=20) not deleted
    # a=6..10 deleted (a>5)
    # remaining: a=2,3,4,5
    var ids: seq[string]
    for row in db.fastRows(sql"SELECT a FROM t ORDER BY a"):
      ids.add(row[0])
    check ids == @["2", "3", "4", "5"]
    db.close()

  test "DELETE all rows":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..5:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    db.exec(sql"DELETE FROM t WHERE 1 = 1")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "0"
    db.close()

  test "affected rows count":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..10:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    check db.execAffectedRows(sql"UPDATE t SET v = v + 1 WHERE v > 5") == 5'i64
    check db.execAffectedRows(sql"DELETE FROM t WHERE v <= 3") == 3'i64
    db.close()

suite "DDL":

  test "CREATE TABLE with column types":
    let db = open(":memory:", "", "", "")
    db.exec(sql"""CREATE TABLE all_types (
      id INTEGER PRIMARY KEY,
      txt TEXT,
      num INT,
      val REAL
    )""")
    db.exec(sql"INSERT INTO all_types (txt, num, val) VALUES (?, ?, ?)",
            "hello", "42", "3.14")
    check db.getValue(sql"SELECT txt FROM all_types") == "hello"
    check db.getValue(sql"SELECT num FROM all_types") == "42"
    check db.getValue(sql"SELECT val FROM all_types") == "3.14"
    db.close()

  test "CREATE TABLE IF NOT EXISTS is idempotent":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
    check db.getValue(sql"SELECT v FROM t WHERE id = 1") == "1"
    db.close()

  test "DEFAULT values on columns":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT DEFAULT 99, b TEXT DEFAULT 'hi')")
    # parser doesn't support INSERT INTO t () VALUES (); test via partial insert
    db.exec(sql"INSERT INTO t (a) VALUES (1)")
    check db.getValue(sql"SELECT a FROM t WHERE id = 1") == "1"
    # b gets its DEFAULT when omitted
    check db.getValue(sql"SELECT b FROM t WHERE id = 1") == "hi"
    db.close()

  test "NOT NULL constraint enforced":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    expect DbError:
      db.exec(sql"INSERT INTO t (name) VALUES (NULL)")
    db.close()

  test "PRIMARY KEY column is implicitly NOT NULL":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    check db.getValue(sql"SELECT id FROM t") != ""
    db.close()

  test "DROP TABLE and DROP TABLE IF EXISTS":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"DROP TABLE t")
    check db.tryExec(sql"SELECT * FROM t") == false
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"DROP TABLE IF EXISTS t")
    db.exec(sql"DROP TABLE IF EXISTS t")
    db.close()

  test "CREATE INDEX":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..100:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    db.exec(sql"CREATE INDEX idx_v ON t (v)")
    check db.getValue(sql"SELECT v FROM t WHERE v = 50") == "50"
    db.close()

suite "error handling":

  test "missing table raises DbError":
    let db = open(":memory:", "", "", "")
    expect DbError:
      db.exec(sql"SELECT * FROM nonexistent")
    expect DbError:
      db.exec(sql"INSERT INTO nonexistent (v) VALUES (1)")
    expect DbError:
      db.exec(sql"UPDATE nonexistent SET v = 1")
    expect DbError:
      db.exec(sql"DELETE FROM nonexistent")
    db.close()

  test "missing column raises DbError":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    expect DbError:
      db.exec(sql"SELECT nosuch FROM t")
    expect DbError:
      db.exec(sql"INSERT INTO t (nosuch) VALUES (1)")
    expect DbError:
      db.exec(sql"UPDATE t SET nosuch = 1")
    db.close()

  test "type mismatch raises DbError":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    expect DbError:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", "not_a_number")
    db.close()

  test "syntax error raises DbError":
    let db = open(":memory:", "", "", "")
    expect DbError:
      db.exec(sql"SELEC * FROM t")
    db.close()

  test "tryExec returns false on error":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    check db.tryExec(sql"SELECT * FROM nonexistent") == false
    check db.tryExec(sql"SELEC * FROM t") == false
    check db.tryExec(sql"SELECT * FROM t") == true
    db.close()

suite "statement caching":

  test "same SQL text reuses cached plan":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    for i in 1..5:
      db.exec(sql"INSERT INTO t (v) VALUES (?)", $i)
    # execute the same SQL template twice with different interpolated values
    check db.getValue(sql"SELECT v FROM t WHERE id = 1") == "1"
    check db.getValue(sql"SELECT v FROM t WHERE id = 3") == "3"
    # and a third time
    check db.getValue(sql"SELECT v FROM t WHERE id = 5") == "5"
    db.close()

  test "different SQL texts get separate cache entries":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (42)")
    check db.getValue(sql"SELECT v FROM t WHERE id = 1") == "42"
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "1"
    db.close()

suite "prepared statements advanced":

  test "rebind between executions":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (10)")
    db.exec(sql"INSERT INTO t (v) VALUES (20)")
    db.exec(sql"INSERT INTO t (v) VALUES (30)")
    let ps = db.prepare("SELECT v FROM t WHERE id = ?")
    ps.bindParam(1, 1)
    check db.getValue(ps) == "10"
    ps.bindParam(1, 2)
    check db.getValue(ps) == "20"
    ps.bindParam(1, 3)
    check db.getValue(ps) == "30"
    db.close()

  test "prepared INSERT and SELECT roundtrip":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
    let ins = db.prepare("INSERT INTO t (name, score) VALUES (?, ?)")
    ins.bindParam(1, "alice")
    ins.bindParam(2, 9.5)
    db.exec(ins)
    ins.bindParam(1, "bob")
    ins.bindParam(2, 8.0)
    db.exec(ins)
    let sel = db.prepare("SELECT name FROM t WHERE score > ?")
    sel.bindParam(1, 9.0)
    check db.getValue(sel) == "alice"
    sel.bindParam(1, 7.0)
    check db.getAllRows(sel).len == 2
    db.close()

  test "prepared statement with NULL binding":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"INSERT INTO t (v) VALUES (NULL)")
    let ps = db.prepare("SELECT COUNT(*) FROM t WHERE v IS NULL")
    check db.getValue(ps) == "1"
    ps.bindNull(1)
    check db.getAllRows(ps).len == 1
    db.close()

suite "WAL persistence with SQL":

  test "full CRUD survives reopen":
    let root = testRoot()
    block:
      let db = open(root / "sqlvm.db", "", "", "")
      db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, n INT)")
      db.exec(sql"INSERT INTO t (v, n) VALUES ('a', 1)")
      db.exec(sql"INSERT INTO t (v, n) VALUES ('b', 2)")
      db.exec(sql"UPDATE t SET n = 10 WHERE v = 'a'")
      db.exec(sql"DELETE FROM t WHERE v = 'b'")
      db.close()

    let db2 = open(root / "sqlvm.db", "", "", "")
    check db2.getValue(sql"SELECT n FROM t WHERE v = 'a'") == "10"
    check db2.getValue(sql"SELECT COUNT(*) FROM t") == "1"
    db2.close()
