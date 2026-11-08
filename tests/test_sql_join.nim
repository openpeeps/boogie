import std/[unittest, os, times, monotimes, strutils]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# P1: INNER/CROSS JOINs (nested loop, aliases, USING, ON).
# ---------------------------------------------------------------------------

proc seedAB(db: DbConn) =
  db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, x INT, name TEXT)")
  db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT, y INT)")
  db.exec(sql"INSERT INTO a (x, name) VALUES (1, 'one')")
  db.exec(sql"INSERT INTO a (x, name) VALUES (2, 'two')")
  db.exec(sql"INSERT INTO a (x, name) VALUES (3, 'three')")
  db.exec(sql"INSERT INTO b (aid, y) VALUES (1, 10)")
  db.exec(sql"INSERT INTO b (aid, y) VALUES (2, 20)")
  db.exec(sql"INSERT INTO b (aid, y) VALUES (2, 21)")
  db.exec(sql"INSERT INTO b (aid, y) VALUES (99, 99)")

suite "basic joins":

  test "inner join with ON and ORDER BY":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    let rows = db.getAllRows(sql"SELECT a.name, b.y FROM a JOIN b ON a.id = b.aid ORDER BY b.y")
    check rows == @[@["one", "10"], @["two", "20"], @["two", "21"]]
    db.close()

  test "aliases in FROM, SELECT, ON and WHERE":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    let rows = db.getAllRows(sql"SELECT u.name, o.y FROM a AS u INNER JOIN b AS o ON u.id = o.aid WHERE o.y > 15 ORDER BY u.name")
    check rows == @[@["two", "20"], @["two", "21"]]
    db.close()

  test "USING expands to equality":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE l (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE r (id INTEGER PRIMARY KEY, w INT)")
    db.exec(sql"INSERT INTO l (v) VALUES (1)")
    db.exec(sql"INSERT INTO l (v) VALUES (2)")
    db.exec(sql"INSERT INTO r (w) VALUES (10)")
    db.exec(sql"INSERT INTO r (w) VALUES (20)")
    let rows = db.getAllRows(sql"SELECT l.v, r.w FROM l JOIN r USING (id) ORDER BY l.v")
    check rows == @[@["1", "10"], @["2", "20"]]
    db.close()

  test "comma and CROSS JOIN are cross products":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE m (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE n (id INTEGER PRIMARY KEY, w INT)")
    db.exec(sql"INSERT INTO m (v) VALUES (1)")
    db.exec(sql"INSERT INTO m (v) VALUES (2)")
    db.exec(sql"INSERT INTO n (w) VALUES (7)")
    db.exec(sql"INSERT INTO n (w) VALUES (8)")
    check db.getValue(sql"SELECT COUNT(*) FROM m, n") == "4"
    check db.getValue(sql"SELECT COUNT(*) FROM m CROSS JOIN n") == "4"
    let rows = db.getAllRows(sql"SELECT m.v, n.w FROM m, n WHERE m.v = n.w")
    check rows.len == 0
    db.close()

  test "self-join with aliases":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE e (id INTEGER PRIMARY KEY, mgr INT, n TEXT)")
    db.exec(sql"INSERT INTO e (mgr, n) VALUES (0, 'boss')")
    db.exec(sql"INSERT INTO e (mgr, n) VALUES (1, 'a1')")
    db.exec(sql"INSERT INTO e (mgr, n) VALUES (1, 'a2')")
    let rows = db.getAllRows(sql"SELECT x.n, y.n FROM e AS x JOIN e AS y ON y.mgr = x.id WHERE x.n = 'boss' ORDER BY y.n")
    check rows == @[@["boss", "a1"], @["boss", "a2"]]
    db.close()

  test "three-table chain":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE t2 (id INTEGER PRIMARY KEY, a INT, w INT)")
    db.exec(sql"CREATE TABLE t3 (id INTEGER PRIMARY KEY, b INT, z INT)")
    db.exec(sql"INSERT INTO t1 (v) VALUES (1)")
    db.exec(sql"INSERT INTO t2 (a, w) VALUES (1, 10)")
    db.exec(sql"INSERT INTO t3 (b, z) VALUES (1, 100)")
    db.exec(sql"INSERT INTO t3 (b, z) VALUES (2, 200)")
    let rows = db.getAllRows(sql"SELECT t1.v, t2.w, t3.z FROM t1 JOIN t2 ON t2.a = t1.id JOIN t3 ON t3.b = t2.id ORDER BY t3.z")
    check rows == @[@["1", "10", "100"]]
    db.close()

  test "NULL never matches in ON":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE p (id INTEGER PRIMARY KEY, k INT)")
    db.exec(sql"CREATE TABLE q (id INTEGER PRIMARY KEY, k INT)")
    db.exec(sql"INSERT INTO p (k) VALUES (NULL)")
    db.exec(sql"INSERT INTO p (k) VALUES (1)")
    db.exec(sql"INSERT INTO q (k) VALUES (NULL)")
    db.exec(sql"INSERT INTO q (k) VALUES (1)")
    check db.getValue(sql"SELECT COUNT(*) FROM p JOIN q ON p.k = q.k") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM p JOIN q USING (k)") == "1"
    db.close()

  test "SELECT * over joins uses qualified names":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE a (id INTEGER PRIMARY KEY, x INT)")
    discard eng.execSql("CREATE TABLE b (id INTEGER PRIMARY KEY, y INT)")
    discard eng.execSql("INSERT INTO a (x) VALUES (1)")
    discard eng.execSql("INSERT INTO b (y) VALUES (2)")
    let r = eng.execSql("SELECT * FROM a, b")
    check r.columns == @["a.id", "a.x", "b.id", "b.y"]
    check r.rows.len == 1
    store.close()

suite "join filtering and output":

  test "WHERE, DISTINCT, LIMIT/OFFSET over joins":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    let d = db.getAllRows(sql"SELECT DISTINCT a.name FROM a JOIN b ON a.id = b.aid ORDER BY a.name")
    check d == @[@["one"], @["two"]]
    let lim = db.getAllRows(sql"SELECT a.name, b.y FROM a JOIN b ON a.id = b.aid ORDER BY b.y LIMIT 2 OFFSET 1")
    check lim == @[@["two", "20"], @["two", "21"]]
    db.close()

  test "placeholders in ON and WHERE":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    let ps = db.prepare("SELECT a.name FROM a JOIN b ON a.id = b.aid AND b.y > ? WHERE a.x < ? ORDER BY a.name")
    ps.bindParam(1, 15)
    ps.bindParam(2, 3)
    check db.getAllRows(ps) == @[@["two"], @["two"]]
    db.close()

  test "aggregates over joins":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    check db.getValue(sql"SELECT COUNT(*) FROM a JOIN b ON a.id = b.aid") == "3"
    check db.getValue(sql"SELECT SUM(b.y) FROM a JOIN b ON a.id = b.aid") == "51"
    check db.getValue(sql"SELECT MIN(a.name), MAX(b.y) FROM a JOIN b ON a.id = b.aid") == "one"
    let rows = db.getAllRows(sql"SELECT COUNT(*) AS c, AVG(b.y) AS a FROM a JOIN b ON a.id = b.aid")
    check rows.len == 1
    check rows[0][0] == "3"
    check rows[0][1] == "17.0"
    db.close()

  test "multi-statement JOIN runs on the VM path":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE a (id INTEGER PRIMARY KEY, x INT)")
    discard eng.execSql("CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT)")
    discard eng.execSql("INSERT INTO a (x) VALUES (5)")
    discard eng.execSql("INSERT INTO b (aid) VALUES (1)")
    let r = eng.execSql("SELECT id FROM a WHERE id < 0; SELECT a.x, b.aid FROM a JOIN b ON b.aid = a.id")
    check r.columns == @["x", "aid"]
    check r.rows == @[@["5", "1"]]
    store.close()

suite "join errors":

  test "ambiguous bare column fails":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT id FROM a JOIN b ON a.id = b.aid")
    db.close()

  test "unknown table qualifier and column fail":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT z.name FROM a JOIN b ON a.id = b.aid")
    expect DbError:
      discard db.getAllRows(sql"SELECT a.nope FROM a JOIN b ON a.id = b.aid")
    expect DbError:
      discard db.getAllRows(sql"SELECT a.name FROM a JOIN b ON a.name = z.y")
    db.close()

  test "LEFT JOIN is rejected clearly":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT a.name FROM a LEFT JOIN b ON a.id = b.aid")
    expect DbError:
      discard db.getAllRows(sql"SELECT a.name FROM a LEFT OUTER JOIN b ON a.id = b.aid")
    db.close()

  test "GROUP BY with JOIN is rejected clearly":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT a.name, COUNT(*) FROM a JOIN b ON a.id = b.aid GROUP BY a.name")
    db.close()

  test "UPDATE and DELETE with JOIN are rejected clearly":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      db.exec(sql"UPDATE a SET x = 1 FROM a JOIN b ON a.id = b.aid")
    expect DbError:
      db.exec(sql"DELETE FROM a USING a JOIN b ON a.id = b.aid")
    db.close()

  test "mixed aggregates and plain columns fail":
    let db = open(":memory:", "", "", "")
    seedAB(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT a.name, COUNT(*) FROM a JOIN b ON a.id = b.aid")
    db.close()

  test "composite USING and CROSS with ON fail":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE l (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE r (id INTEGER PRIMARY KEY, w INT)")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM l JOIN r USING (id, v)")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM l CROSS JOIN r ON l.id = r.id")
    db.close()
