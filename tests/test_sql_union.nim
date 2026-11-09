import std/[unittest, strutils]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# UNION / UNION ALL over full SELECT branches (plain, grouped, joins).
# Branches fold left into typed rows (per-junction DISTINCT unless ALL);
# trailing ORDER BY/LIMIT belongs to the whole union.
# ---------------------------------------------------------------------------

proc seedTU(db: DbConn) =
  db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
  db.exec(sql"CREATE TABLE u (id INTEGER PRIMARY KEY, v INT)")
  db.exec(sql"INSERT INTO t (v) VALUES (1), (2), (3)")
  db.exec(sql"INSERT INTO u (v) VALUES (2), (3), (4)")

suite "UNION basics":

  test "ALL keeps duplicates, DISTINCT dedupes":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    check db.getAllRows(sql"SELECT v FROM t UNION ALL SELECT v FROM u ORDER BY v") ==
      @[@["1"], @["2"], @["2"], @["3"], @["3"], @["4"]]
    check db.getAllRows(sql"SELECT v FROM t UNION SELECT v FROM u ORDER BY v") ==
      @[@["1"], @["2"], @["3"], @["4"]]
    db.close()

  test "NULLs dedupe together under DISTINCT":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO a (v) VALUES (NULL), (1)")
    db.exec(sql"INSERT INTO b (v) VALUES (NULL), (2)")
    check db.getAllRows(sql"SELECT v FROM a UNION SELECT v FROM b ORDER BY v") ==
      @[@[""], @["1"], @["2"]]
    db.close()

  test "DISTINCT is typed (1 == 1.0)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, f REAL)")
    db.exec(sql"INSERT INTO a (v) VALUES (1)")
    db.exec(sql"INSERT INTO b (f) VALUES (1.0)")
    check db.getValue(sql"SELECT COUNT(*) FROM a UNION SELECT f FROM b") == "1"
    check db.getAllRows(sql"SELECT COUNT(*) FROM a UNION ALL SELECT f FROM b") ==
      @[@["1"], @["1.0"]]
    db.close()

  test "column names come from the first branch":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    let r = db.getAllRows(sql"SELECT v AS x FROM t UNION ALL SELECT v AS y FROM u ORDER BY x LIMIT 1")
    check r == @[@["1"]]
    db.close()

  test "width mismatches fail":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, x INT, y INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, x INT)")
    db.exec(sql"INSERT INTO a (x, y) VALUES (1, 2)")
    db.exec(sql"INSERT INTO b (x) VALUES (1)")
    expect DbError:
      discard db.getAllRows(sql"SELECT x, y FROM a UNION ALL SELECT x FROM b")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM a UNION ALL SELECT * FROM b")
    db.close()

suite "UNION ordering and limits":

  test "ORDER BY / LIMIT / OFFSET hoist to union level":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    check db.getAllRows(sql"SELECT v FROM t UNION ALL SELECT v FROM u ORDER BY v DESC LIMIT 2") ==
      @[@["4"], @["3"]]
    check db.getAllRows(sql"SELECT v FROM t UNION ALL SELECT v FROM u ORDER BY v LIMIT 2 OFFSET 4") ==
      @[@["3"], @["4"]]
    db.close()

  test "ORDER BY must reference a SELECT output":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT v FROM t UNION ALL SELECT v FROM u ORDER BY nope")
    db.close()

  test "ORDER BY / LIMIT on non-last branches fail":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    expect DbError:
      discard db.getAllRows(sql"SELECT v FROM t ORDER BY v UNION ALL SELECT v FROM u")
    expect DbError:
      discard db.getAllRows(sql"SELECT v FROM t LIMIT 1 UNION ALL SELECT v FROM u")
    db.close()

suite "UNION placeholders":

  test "positional placeholders span branches in order":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    check db.getAllRows(
      sql"SELECT v FROM t WHERE v > ? UNION ALL SELECT v FROM u WHERE v > ? ORDER BY v",
      "1", "2") == @[@["2"], @["3"], @["3"], @["4"]]
    db.close()

  test "named placeholders share slots across branches":
    let db = open(":memory:", "", "", "")
    seedTU(db)
    check db.getAllRows(
      sql"SELECT v FROM t WHERE v > :m UNION ALL SELECT v FROM u WHERE v > :m ORDER BY v",
      "2") == @[@["3"], @["3"], @["4"]]
    db.close()

suite "UNION mixed branches":

  test "plain, grouped and join branches mix":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE u (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1), (2)")
    db.exec(sql"INSERT INTO u (v) VALUES (2), (2)")
    check db.getAllRows(sql"SELECT v FROM t UNION ALL SELECT COUNT(*) FROM u ORDER BY v") ==
      @[@["1"], @["2"], @["2"]]
    check db.getAllRows(sql"SELECT v FROM t UNION SELECT COUNT(*) FROM u ORDER BY v") ==
      @[@["1"], @["2"]]
    check db.getAllRows(
      sql"SELECT t.v FROM t JOIN u ON u.v = t.v UNION ALL SELECT v FROM t WHERE v = 1 ORDER BY v") ==
      @[@["1"], @["2"], @["2"]]
    check db.getAllRows(
      sql"SELECT v, COUNT(*) FROM t GROUP BY v UNION ALL SELECT v, COUNT(*) FROM u GROUP BY v ORDER BY v") ==
      @[@["1", "1"], @["2", "1"], @["2", "2"]]
    db.close()

  test "DISTINCT branches dedupe before folding":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1), (1), (2)")
    check db.getAllRows(sql"SELECT DISTINCT v FROM t UNION ALL SELECT v FROM t WHERE v = 9 ORDER BY v") ==
      @[@["1"], @["2"]]
    db.close()

  test "fold is left-associative across mixed junctions":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO a (v) VALUES (1)")
    db.exec(sql"INSERT INTO b (v) VALUES (1)")
    db.exec(sql"INSERT INTO c (v) VALUES (1)")
    # (a U b) U-ALL c = [1] ++ [1]
    check db.getAllRows(sql"SELECT v FROM a UNION SELECT v FROM b UNION ALL SELECT v FROM c") ==
      @[@["1"], @["1"]]
    # (a U-ALL b) U c = dedupe([1,1,1])
    check db.getValue(sql"SELECT COUNT(*) FROM a UNION ALL SELECT v FROM b UNION SELECT v FROM c") == "1"
    db.close()

  test "UNION over outer joins":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT, w TEXT)")
    db.exec(sql"INSERT INTO a (v) VALUES (1), (2)")
    db.exec(sql"INSERT INTO b (aid, w) VALUES (2, 'x')")
    # NOTE: bare `SELECT 9, 'z'` without FROM is not supported by the engine
    # anywhere, so the constant branch selects from a one-row slice instead.
    check db.getAllRows(
      sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id UNION ALL SELECT 9, 'z' FROM a WHERE a.v = 1 ORDER BY v, w") ==
      @[@["1", ""], @["2", "x"], @["9", "z"]]
    db.close()

suite "UNION misc":

  test "UNION via multi-statement VM path":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    discard eng.execSql("CREATE TABLE u (id INTEGER PRIMARY KEY, v INT)")
    discard eng.execSql("INSERT INTO t (v) VALUES (1), (2)")
    discard eng.execSql("INSERT INTO u (v) VALUES (2), (3)")
    let r = eng.execSql("SELECT COUNT(*) FROM t; SELECT v FROM t UNION ALL SELECT v FROM u ORDER BY v")
    check r.kind == skSelect
    check r.columns == @["v"]
    check r.rows == @[@["1"], @["2"], @["2"], @["3"]]
    # placeholders bind through the VM path too
    let r2 = eng.execSql(
      "SELECT COUNT(*) FROM t; SELECT v FROM t WHERE v > ? UNION ALL SELECT v FROM u WHERE v > ? ORDER BY v",
      [initValue(1'i64), initValue(2'i64)])
    check r2.rows == @[@["2"], @["3"]]
    store.close()
