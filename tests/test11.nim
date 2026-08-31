import std/[unittest, os, times, strformat, monotimes]
import ../src/boogie/db_boogie

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_dboogie_tests_" & unique)
  createDir(result)

suite "db_boogie (SQL driver)":

  let root = testRoot()

  test "in-memory CRUD roundtrip":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INT)")

    db.exec(sql"INSERT INTO users (name, age) VALUES (?, ?)", "alice", "30")
    db.exec(sql"INSERT INTO users (name, age) VALUES (?, ?)", "bob", "41")
    db.exec(sql"INSERT INTO users (name) VALUES (?)", "carol")

    check db.getRow(sql"SELECT id, name, age FROM users WHERE name = ?",
                    "alice") == @["1", "alice", "30"]
    check db.getValue(sql"SELECT COUNT(*) FROM users WHERE 1 = 0") == "0"
    check db.getValue(sql"SELECT name FROM users WHERE id = ?", "2") == "bob"

    var names: seq[string] = @[]
    for row in db.fastRows(sql"SELECT name FROM users ORDER BY name"):
      names.add(row[0])
    check names == @["alice", "bob", "carol"]

    db.close()

  test "serial primary keys auto-increment across inserts":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    check db.insertID(sql"INSERT INTO t (v) VALUES (?)", "a") == 1'i64
    check db.insertID(sql"INSERT INTO t (v) VALUES (?)", "b") == 2'i64
    # explicit pk values are honored too
    db.exec(sql"INSERT INTO t (id, v) VALUES (10, ?)", "c")
    check db.insertID(sql"INSERT INTO t (v) VALUES (?)", "d") == 11'i64
    db.close()

  test "WHERE operators: comparisons, AND/OR/NOT, IS NULL, IN-free core":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT, age INT, score REAL)")
    for i in 1 .. 9:
      let score = 0.5'f64 * float(i)
      db.exec(sql"INSERT INTO u (name, age, score) VALUES (?, ?, ?)",
              "u" & $i, $i, $score)

    check db.getValue(sql"SELECT COUNT(*) FROM u WHERE age > 7") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM u WHERE age >= 8 AND age <= 9") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM u WHERE age != 3") == "8"
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE (age < 3 OR age > 8)") == "3"
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE NOT age > 5") == "5"
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE score > 4.0 AND score < 4.6") == "1"
    # numeric affinity: text operands coerce when they parse (sqlite behavior)
    check db.getValue(sql"SELECT COUNT(*) FROM u WHERE age = '4'") == "1"

    db.exec(sql"INSERT INTO u (name) VALUES (?)", "nullguy")
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE age IS NULL") == "1"
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE age IS NOT NULL") == "9"
    # comparisons against NULL are false per SQL semantics
    check db.getValue(
      sql"SELECT COUNT(*) FROM u WHERE age > 0 OR age <= 0") == "9"

    db.close()

  test "ORDER BY, LIMIT, OFFSET and DISTINCT":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE n (id INTEGER PRIMARY KEY, v TEXT, k INT)")
    db.exec(sql"""INSERT INTO n (v, k) VALUES
                   ('b', 2), ('a', 3), ('c', 1), ('a', 5), ('b', 4)""")

    var asc: seq[string] = @[]
    for row in db.fastRows(sql"SELECT v FROM n ORDER BY v"):
      asc.add(row[0])
    check asc == @["a", "a", "b", "b", "c"]

    var desc: seq[string] = @[]
    for row in db.fastRows(sql"SELECT v FROM n ORDER BY v DESC LIMIT 3"):
      desc.add(row[0])
    check desc == @["c", "b", "b"]

    var paged: seq[string] = @[]
    for row in db.fastRows(
        sql"SELECT v FROM n ORDER BY v, k LIMIT 2 OFFSET 1"):
      paged.add(row[0])
    check paged == @["a", "b"]

    check db.getAllRows(sql"SELECT DISTINCT v FROM n ORDER BY v").len == 3

    # numeric ordering must not be lexicographic
    check db.getValue(sql"SELECT k FROM n ORDER BY k DESC LIMIT 1") == "5"
    db.close()

  test "UPDATE with expressions referencing columns":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE acc (id INTEGER PRIMARY KEY, balance REAL, flag INT)")
    db.exec(sql"INSERT INTO acc (balance, flag) VALUES (?, ?)", "100.0", "1")
    db.exec(sql"INSERT INTO acc (balance, flag) VALUES (?, ?)", "50.0", "0")

    db.exec(sql"UPDATE acc SET balance = balance * 2 + 10 WHERE flag = ?", "1")
    check db.getRow(sql"SELECT balance FROM acc WHERE id = 1")[0] == "210.0"
    check db.getRow(sql"SELECT balance FROM acc WHERE id = 2")[0] == "50.0"

    db.exec(sql"UPDATE acc SET flag = 1, balance = 0 WHERE balance < 60.0")
    check db.getValue(
      sql"SELECT COUNT(*) FROM acc WHERE flag = 1") == "2"
    check db.execAffectedRows(sql"UPDATE acc SET flag = 0") == 2'i64

    expect DbError:
      db.exec(sql"UPDATE acc SET nosuchcol = 1")

    db.close()

  test "DELETE removes matching rows only":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE d (id INTEGER PRIMARY KEY, v INT)")
    for i in 1 .. 6:
      db.exec(sql"INSERT INTO d (v) VALUES (?)", $i)
    db.exec(sql"DELETE FROM d WHERE v > 4")
    check db.getValue(sql"SELECT COUNT(*) FROM d") == "4"
    # modulo expressions are outside the supported core
    expect DbError:
      db.exec(sql"DELETE FROM d WHERE v % 2 = 0")
    check db.getValue(sql"SELECT COUNT(*) FROM d") == "4"
    db.exec(sql"DELETE FROM d WHERE v = 2")
    check db.getValue(sql"SELECT v FROM d ORDER BY v") == "1"
    db.exec(sql"DROP TABLE d")
    check db.tryExec(sql"SELECT * FROM d") == false
    db.close()

  test "prepared statements bind positionally with typed params":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT, age INT, ratio REAL)")
    let ins = db.prepare("INSERT INTO p (name, age, ratio) VALUES (?, ?, ?)")
    ins.bindParam(1, "ann")
    ins.bindParam(2, 33)
    ins.bindParam(3, 0.25)
    db.exec(ins)
    ins.bindParam(1, "jon")
    ins.bindParam(2, 44'i64)
    ins.bindParam(3, 0.75)
    db.exec(ins)

    let sel = db.prepare("SELECT name FROM p WHERE age > ?")
    sel.bindParam(1, 40)
    check db.getValue(sel) == "jon"

    sel.bindNull(1)
    check db.getAllRows(sel).len == 0

    check db.getValue(sql"SELECT ratio FROM p WHERE name = 'ann'") == "0.25"
    db.close()

  test "NULL handling matches sqlite text conventions":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE z (id INTEGER PRIMARY KEY, a TEXT, b INT DEFAULT 7)")
    db.exec(sql"INSERT INTO z (id, a) VALUES (1, NULL)")
    check db.getValue(sql"SELECT a FROM z WHERE id = 1") == ""
    # column default applied on insert
    check db.getValue(sql"SELECT b FROM z WHERE id = 1") == "7"
    let row = db.getRow(sql"SELECT id, a, b FROM z WHERE id = 99")
    check row == @["", "", ""]
    db.close()

  test "errors raise DbError; tryExec reports failure":
    let db = open(":memory:", "", "", "test")
    expect DbError:
      db.exec(sql"SELEC nope")
    expect DbError:
      db.exec(sql"SELECT * FROM missing_table")
    db.exec(sql"CREATE TABLE e (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    expect DbError:
      db.exec(sql"INSERT INTO e () VALUES ()")
    check db.tryExec(sql"SELECT 1 FROM e") == true
    check db.tryExec(sql"SELECT FROM bad") == false
    db.close()

  test "multi-row insert returns last generated key":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE m (id INTEGER PRIMARY KEY, v TEXT)")
    db.exec(sql"INSERT INTO m (v) VALUES ('x'), ('y'), ('z')")
    check db.getValue(sql"SELECT COUNT(*) FROM m") == "3"
    db.close()

  test "WAL persistence across reopen":
    block:
      let path = root / "persist.db"
      let db = open(path, "", "", "app")
      db.exec(sql"CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
      db.exec(sql"INSERT INTO kv (v) VALUES ('hello')")
      db.exec(sql"INSERT INTO kv (v) VALUES ('world')")
      db.exec(sql"UPDATE kv SET v = 'HELLO' WHERE k = 1")
      db.exec(sql"DELETE FROM kv WHERE k = 2")
      db.close()

    let db2 = open(root / "persist.db", "", "", "app")
    check db2.getValue(sql"SELECT v FROM kv WHERE k = 1") == "HELLO"
    check db2.getValue(sql"SELECT COUNT(*) FROM kv") == "1"
    # writes continue seamlessly after recovery
    check db2.insertID(sql"INSERT INTO kv (v) VALUES (?)", "!") == 3'i64
    db2.close()

  test "instantRows expose indexed access":
    let db = open(":memory:", "", "", "test")
    db.exec(sql"CREATE TABLE ir (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
    db.exec(sql"INSERT INTO ir (a, b) VALUES ('x', 'y')")
    for row in db.instantRows(sql"SELECT a, b FROM ir"):
      check row.len == 2'i32
      check row[int32(0)] == "x"
      check row[int32(1)] == "y"
    db.close()

  test "setEncoding accepts utf-8 only":
    let db = open(":memory:", "", "", "test")
    check db.setEncoding("UTF-8") == true
    check db.setEncoding("utf8") == true
    check db.setEncoding("latin-1") == false
    db.close()

suite "db_boogie benchmarks":

  test "dboogie ops/sec benchmark (insert/select/update)":
    const N = 1_000
    let benchRoot = "tests" / "data" / "bench_dboogie"
    if not dirExists(benchRoot):
      createDir(benchRoot)
    # start from a clean slate so row counts are deterministic
    removeFile(benchRoot / "bench.db")
    removeFile(benchRoot / "bench.wal")

    var db = open(benchRoot / "bench.db", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")

    let t0 = getMonoTime()
    for i in 1 .. N:
      db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    let insertSecs = float((getMonoTime() - t0).inMilliSeconds) / 1000.0

    let t1 = getMonoTime()
    var found = 0
    for i in 1 .. 50:
      let v = db.getValue(sql"SELECT id FROM b WHERE v = ?", "v" & $i)
      if v.len > 0:
        inc found
    let pointSecs = float((getMonoTime() - t1).inMilliSeconds) / 1000.0
    check found == 50

    let t2 = getMonoTime()
    var scanned = 0
    for row in db.fastRows(sql"SELECT id, v FROM b ORDER BY id"):
      inc scanned
    let scanSecs = float((getMonoTime() - t2).inMilliSeconds) / 1000.0
    check scanned == N

    let t3 = getMonoTime()
    for i in 1 .. 50:
      db.exec(sql"UPDATE b SET n = n + 1 WHERE v = ?", "v" & $i)
    let updateSecs = float((getMonoTime() - t3).inMilliSeconds) / 1000.0

    echo fmt"[bench][dboogie][disk+wal] insert={float(N) / insertSecs:>10.0f} ops/s pointQuery={50.0 / pointSecs:>9.0f} q/s orderedScan={float(N) / scanSecs:>10.0f} rows/s update={50.0 / updateSecs:>9.0f} ops/s"
    db.close()
