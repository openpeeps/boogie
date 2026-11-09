import std/[unittest, strutils]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# LEFT / RIGHT / FULL OUTER JOINs over the shared left-deep row source.
# Each step applies only its own ON (NULL-supplying for outer kinds);
# WHERE filters afterwards. NULLs render as "" through the driver.
# ---------------------------------------------------------------------------

proc seedBasic(db: DbConn) =
  db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
  db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT, w TEXT)")
  db.exec(sql"INSERT INTO a (v) VALUES (1), (2), (3)")
  db.exec(sql"INSERT INTO b (aid, w) VALUES (2, 'x'), (2, 'y'), (4, 'z')")

suite "LEFT JOIN":

  test "matches plus NULL-supplied rows":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.v, b.w") ==
      @[@["1", ""], @["2", "x"], @["2", "y"], @["3", ""]]
    db.close()

  test "OUTER spelling and aliases":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT x.v, y.w FROM a AS x LEFT OUTER JOIN b AS y ON y.aid = x.id ORDER BY x.v, y.w") ==
      @[@["1", ""], @["2", "x"], @["2", "y"], @["3", ""]]
    db.close()

  test "empty sides":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE e (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE f (id INTEGER PRIMARY KEY, eid INT, w TEXT)")
    db.exec(sql"INSERT INTO e (v) VALUES (1), (2)")
    # empty right: every left row NULL-extends
    check db.getAllRows(sql"SELECT e.v, f.w FROM e LEFT JOIN f ON f.eid = e.id ORDER BY e.v") ==
      @[@["1", ""], @["2", ""]]
    # empty left: empty result
    check db.getAllRows(sql"SELECT e.v, f.w FROM f LEFT JOIN e ON f.eid = e.id") ==
      newSeq[seq[string]]()
    db.close()

  test "ON narrows matching but keeps NULL-supply":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id AND b.w = 'x' ORDER BY a.v, b.w") ==
      @[@["1", ""], @["2", "x"], @["3", ""]]
    db.close()

  test "WHERE filters NULL-supplied rows afterwards":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id WHERE b.w IS NULL ORDER BY a.v") ==
      @[@["1", ""], @["3", ""]]
    check db.getAllRows(sql"SELECT a.v FROM a LEFT JOIN b ON b.aid = a.id WHERE b.w = 'x'") ==
      @[@["2"]]
    db.close()

suite "RIGHT and FULL JOIN":

  test "RIGHT JOIN mirrors":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a RIGHT JOIN b ON b.aid = a.id ORDER BY b.w") ==
      @[@["2", "x"], @["2", "y"], @["", "z"]]
    check db.getAllRows(sql"SELECT a.v, b.w FROM a RIGHT OUTER JOIN b ON b.aid = a.id ORDER BY b.w") ==
      @[@["2", "x"], @["2", "y"], @["", "z"]]
    db.close()

  test "RIGHT JOIN with empty left preserves right":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE e (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE f (id INTEGER PRIMARY KEY, eid INT, w TEXT)")
    db.exec(sql"INSERT INTO f (eid, w) VALUES (1, 'x'), (2, 'y')")
    check db.getAllRows(sql"SELECT e.v, f.w FROM e RIGHT JOIN f ON f.eid = e.id ORDER BY f.w") ==
      @[@["", "x"], @["", "y"]]
    db.close()

  test "FULL JOIN keeps both unmatched sides":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a FULL JOIN b ON b.aid = a.id ORDER BY a.v, b.w") ==
      @[@["", "z"], @["1", ""], @["2", "x"], @["2", "y"], @["3", ""]]
    check db.getAllRows(sql"SELECT a.v, b.w FROM a FULL OUTER JOIN b ON b.aid = a.id ORDER BY a.v, b.w") ==
      @[@["", "z"], @["1", ""], @["2", "x"], @["2", "y"], @["3", ""]]
    db.close()

suite "chained and mixed":

  test "chained LEFT JOINs":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY, bid INT, w TEXT)")
    db.exec(sql"INSERT INTO a (v) VALUES (1), (2)")
    db.exec(sql"INSERT INTO b (aid) VALUES (2)")
    db.exec(sql"INSERT INTO c (bid, w) VALUES (1, 'p'), (9, 'q')")
    check db.getAllRows(sql"SELECT a.v, b.aid, c.w FROM a LEFT JOIN b ON b.aid = a.id LEFT JOIN c ON c.bid = b.id ORDER BY a.v") ==
      @[@["1", "", ""], @["2", "2", "p"]]
    db.close()

  test "mixed LEFT then RIGHT is left-deep":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY)")
    db.exec(sql"INSERT INTO a (id) VALUES (1), (2)")
    db.exec(sql"INSERT INTO b (id) VALUES (2)")
    db.exec(sql"INSERT INTO c (id) VALUES (2), (3)")
    # (a LEFT b) = (1,NULL),(2,2); RIGHT c on b.id=c.id: (2,2,2) + (NULL,NULL,3)
    check db.getAllRows(sql"SELECT a.id, b.id, c.id FROM a LEFT JOIN b ON b.id = a.id RIGHT JOIN c ON c.id = b.id ORDER BY c.id") ==
      @[@["2", "2", "2"], @["", "", "3"]]
    db.close()

  test "comma plus outer join":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY, bid INT)")
    db.exec(sql"INSERT INTO a (id) VALUES (1)")
    db.exec(sql"INSERT INTO b (id) VALUES (10)")
    db.exec(sql"INSERT INTO c (bid) VALUES (10)")
    check db.getAllRows(sql"SELECT a.id, b.id, c.bid FROM a, b LEFT JOIN c ON c.bid = b.id ORDER BY a.id") ==
      @[@["1", "10", "10"]]
    db.close()

  test "USING with LEFT JOIN":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, w TEXT)")
    db.exec(sql"INSERT INTO a (v) VALUES (1), (2)")
    db.exec(sql"INSERT INTO b (id, w) VALUES (2, 'x')")
    check db.getAllRows(sql"SELECT a.id, b.w FROM a LEFT JOIN b USING (id) ORDER BY a.id") ==
      @[@["1", ""], @["2", "x"]]
    db.close()

  test "self LEFT JOIN":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, mgr INT)")
    db.exec(sql"INSERT INTO emp (name, mgr) VALUES ('ceo', NULL), ('dev', 1)")
    check db.getAllRows(sql"SELECT e.name, m.name FROM emp e LEFT JOIN emp m ON m.id = e.mgr ORDER BY e.name") ==
      @[@["ceo", ""], @["dev", "ceo"]]
    db.close()

suite "aggregates and groups over outer joins":

  test "COUNT(*) counts NULL-supplied rows, COUNT(col) skips":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getValue(sql"SELECT COUNT(*) FROM a LEFT JOIN b ON b.aid = a.id") == "4"
    check db.getValue(sql"SELECT COUNT(b.w) FROM a LEFT JOIN b ON b.aid = a.id") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM a FULL JOIN b ON b.aid = a.id") == "5"
    db.close()

  test "GROUP BY over LEFT JOIN with NULL group":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT b.w, COUNT(*) FROM a LEFT JOIN b ON b.aid = a.id GROUP BY b.w ORDER BY b.w") ==
      @[@["", "2"], @["x", "1"], @["y", "1"]]
    check db.getAllRows(sql"SELECT b.w FROM a LEFT JOIN b ON b.aid = a.id GROUP BY b.w HAVING COUNT(*) > 1") ==
      @[@[""]]
    db.close()

  test "aggregates and HAVING over FULL JOIN":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getValue(sql"SELECT SUM(a.v) FROM a FULL JOIN b ON b.aid = a.id") == "8"
    check db.getAllRows(sql"SELECT COUNT(*) FROM a FULL JOIN b ON b.aid = a.id WHERE a.v BETWEEN 1 AND 3") == @[@["4"]]
    db.close()

suite "misc":

  test "ORDER BY, LIMIT and placeholders":
    let db = open(":memory:", "", "", "")
    seedBasic(db)
    check db.getAllRows(sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.v DESC LIMIT 2") ==
      @[@["3", ""], @["2", "x"]]
    check db.getAllRows(sql"SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id AND b.w = ? ORDER BY a.v", "y") ==
      @[@["1", ""], @["2", "y"], @["3", ""]]
    db.close()

  test "outer join via multi-statement VM path":
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE a (id INTEGER PRIMARY KEY, v INT)")
    discard eng.execSql("CREATE TABLE b (id INTEGER PRIMARY KEY, aid INT, w TEXT)")
    discard eng.execSql("INSERT INTO a (v) VALUES (1), (2)")
    discard eng.execSql("INSERT INTO b (aid, w) VALUES (2, 'x')")
    let r = eng.execSql("SELECT COUNT(*) FROM a; SELECT a.v, b.w FROM a LEFT JOIN b ON b.aid = a.id ORDER BY a.v")
    check r.kind == skSelect
    check r.rows == @[@["1", ""], @["2", "x"]]
    store.close()

  test "outer join errors":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY)")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM a LEFT JOIN b")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM a RIGHT JOIN b")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM a FULL JOIN b")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM a CROSS JOIN b ON a.id = b.id")
    db.close()
