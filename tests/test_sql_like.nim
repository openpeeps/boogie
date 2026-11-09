import std/[unittest, strutils]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# LIKE/ILIKE + BETWEEN (+ single-value IN) predicates.
#
# LIKE is a rune-based `%`/`_` matcher (no ESCAPE clause upstream: both are
# always wildcards). BETWEEN normalizes at parse time (`x BETWEEN lo AND hi`
# arrives mangled by precedence) into plain comparisons, so it works in
# WHERE/ON/HAVING, joins, grouped queries and DML alike. Multi-value
# `IN (a,b)` cannot parse upstream; `IN (SELECT ...)` needs subqueries.
# ---------------------------------------------------------------------------

suite "LIKE basics":

  test "prefix, suffix, contains, exact":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('bob'), ('albert'), ('x')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'al%' ORDER BY name") ==
      @[@["albert"], @["alice"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE '%ob'") ==
      @[@["bob"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE '%bert%'") ==
      @[@["albert"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'bob'") ==
      @[@["bob"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE '%' ORDER BY name") ==
      @[@["albert"], @["alice"], @["bob"], @["x"]]
    db.close()

  test "underscore matches exactly one character":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('abc'), ('ac'), ('axc'), ('abbc'), ('q')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'a_c' ORDER BY name") ==
      @[@["abc"], @["axc"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'a__c'") ==
      @[@["abbc"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE '_'") == @[@["q"]]
    db.close()

  test "empty pattern and empty match":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('a'), ('')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE ''") == @[@[""]]
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE '%'") == "2"
    db.close()

  test "LIKE is case-sensitive, ILIKE is not":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('Alice')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'al%'") ==
      @[@["alice"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'Al%'") ==
      @[@["Alice"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE 'AL%'") ==
      newSeq[seq[string]]()
    check db.getAllRows(sql"SELECT name FROM t WHERE name ILIKE 'AL%' ORDER BY name") ==
      @[@["Alice"], @["alice"]]
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name NOT ILIKE 'al%'") == "0"
    db.close()

  test "wildcards are always wild (no ESCAPE clause upstream)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('a_c'), ('abc')")
    # `_` matches a literal underscore too
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE 'a_c'") == "2"
    # backslash is literal: 'a\%' means 'a', '\', then anything
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE 'a\\%'") == "0"
    db.close()

  test "multibyte text counts characters":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('café')")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE 'caf_'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE 'caf%'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE '___'") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE '____'") == "1"
    db.close()

suite "LIKE nulls and negation":

  test "NULL never matches, on either side":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), (NULL)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE 'a%'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name NOT LIKE 'a%'") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL LIKE 'a%'") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE 'alice' LIKE NULL") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE NULL") == "0"
    db.close()

  test "NOT LIKE and NOT over LIKE":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('bob')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name NOT LIKE 'a%'") ==
      @[@["bob"]]
    check db.getAllRows(sql"SELECT name FROM t WHERE NOT (name LIKE 'a%')") ==
      @[@["bob"]]
    db.close()

  test "LIKE with placeholder patterns":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('bob')")
    check db.getAllRows(sql"SELECT name FROM t WHERE name LIKE ?", "a%") ==
      @[@["alice"]]
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name LIKE ?", "zzz%") ==
      "0"
    db.close()

  test "LIKE over non-text columns stringifies":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT)")
    db.exec(sql"INSERT INTO t (age) VALUES (25), (30), (35)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age LIKE '3%'") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age LIKE '2_'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age NOT LIKE '3%'") == "1"
    db.close()

  test "LIKE in SELECT projection (fast and VM paths)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('bob'), (NULL)")
    check db.getAllRows(sql"SELECT name LIKE 'a%' FROM t ORDER BY id") ==
      @[@["1"], @["0"], @[""]]
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    discard eng.execSql("INSERT INTO t (name) VALUES ('alice'), ('bob')")
    let r = eng.execSql(
      "SELECT COUNT(*) FROM t; SELECT name LIKE 'a%' FROM t ORDER BY id")
    check r.rows == @[@["1"], @["0"]]
    store.close()
    db.close()

suite "LIKE elsewhere":

  test "LIKE in JOIN ON and WHERE":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, tag TEXT, aid INT)")
    db.exec(sql"INSERT INTO a (name) VALUES ('alice'), ('bob')")
    db.exec(sql"INSERT INTO b (tag, aid) VALUES ('x-al', 1), ('x-bo', 2), ('y', 1)")
    check db.getAllRows(sql"SELECT a.name FROM a JOIN b ON b.aid = a.id AND b.tag LIKE 'x-%' ORDER BY a.name") ==
      @[@["alice"], @["bob"]]
    check db.getAllRows(sql"SELECT a.name FROM a JOIN b ON b.aid = a.id WHERE b.tag LIKE '%bo'") ==
      @[@["bob"]]
    db.close()

  test "LIKE in HAVING over groups and over joins":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, v INT)")
    db.exec(sql"INSERT INTO t (grp, v) VALUES ('aa', 1), ('aa', 2), ('bb', 3)")
    check db.getAllRows(sql"SELECT grp FROM t GROUP BY grp HAVING grp LIKE 'a%'") ==
      @[@["aa"]]
    check db.getAllRows(sql"SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > 1 AND grp LIKE '%a'") ==
      @[@["aa", "2"]]
    db.exec(sql"CREATE TABLE u (id INTEGER PRIMARY KEY, g TEXT)")
    db.exec(sql"INSERT INTO u (g) VALUES ('aa'), ('bb')")
    check db.getAllRows(sql"SELECT u.g FROM t JOIN u ON u.g = t.grp GROUP BY u.g HAVING u.g LIKE 'b%'") ==
      @[@["bb"]]
    db.close()

  test "LIKE in UPDATE and DELETE":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    db.exec(sql"INSERT INTO t (name) VALUES ('alice'), ('bob'), ('albert')")
    db.exec(sql"UPDATE t SET name = 'AL' WHERE name LIKE 'al%'")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name = 'AL'") == "2"
    db.exec(sql"DELETE FROM t WHERE name LIKE '%o%'")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "2"
    db.close()

suite "BETWEEN":

  test "inclusive int bounds and NOT BETWEEN":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT)")
    db.exec(sql"INSERT INTO t (age) VALUES (20), (25), (30), (NULL)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN 20 AND 25") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN 25 AND 25") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age NOT BETWEEN 20 AND 25") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN 100 AND 200") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL BETWEEN 1 AND 2") == "0"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE NULL NOT BETWEEN 1 AND 2") == "0"
    db.close()

  test "text and float bounds":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, f REAL)")
    db.exec(sql"INSERT INTO t (name, f) VALUES ('b', 1.5), ('c', 2.5), ('e', 9.0)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE name BETWEEN 'b' AND 'd'") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE f BETWEEN 1.0 AND 3.0") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE f NOT BETWEEN 1.0 AND 3.0") == "1"
    db.close()

  test "BETWEEN with placeholders":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT)")
    db.exec(sql"INSERT INTO t (age) VALUES (20), (25), (30)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN ? AND ?", "20", "25") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN ? AND ?", "25", "25") == "1"
    db.close()

  test "BETWEEN combined with AND/OR":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT, name TEXT)")
    db.exec(sql"INSERT INTO t (age, name) VALUES (20, 'a'), (25, 'b'), (30, 'c')")
    # right-of-AND shape the parser mangles: must still work
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age = 25 AND age BETWEEN 20 AND 30") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN 20 AND 30 AND name LIKE '%b%'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age BETWEEN 20 AND 30 OR name = 'c'") == "3"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE (age BETWEEN 20 AND 25) AND name <> 'a'") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age = 20 AND age = 25 AND age BETWEEN 20 AND 30") == "0"
    db.close()

  test "BETWEEN in joins, groups and DML":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY, age INT)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, lo INT, hi INT, aid INT)")
    db.exec(sql"INSERT INTO a (age) VALUES (25), (80)")
    db.exec(sql"INSERT INTO b (lo, hi, aid) VALUES (20, 30, 1), (70, 90, 2)")
    check db.getValue(sql"SELECT COUNT(*) FROM a JOIN b ON b.aid = a.id WHERE a.age BETWEEN b.lo AND b.hi") == "2"
    check db.getValue(sql"SELECT COUNT(*) FROM a JOIN b ON a.age BETWEEN b.lo AND b.hi") == "2"
    db.exec(sql"CREATE TABLE g (id INTEGER PRIMARY KEY, grp TEXT, v INT)")
    db.exec(sql"INSERT INTO g (grp, v) VALUES ('x', 1), ('x', 5), ('y', 50)")
    check db.getAllRows(sql"SELECT grp FROM g GROUP BY grp HAVING COUNT(*) BETWEEN 1 AND 2 ORDER BY grp") ==
      @[@["x"], @["y"]]
    check db.getValue(sql"SELECT COUNT(*) FROM g WHERE v BETWEEN 1 AND 10") == "2"
    db.exec(sql"UPDATE g SET v = 99 WHERE v BETWEEN 40 AND 60")
    check db.getValue(sql"SELECT v FROM g WHERE grp = 'y'") == "99"
    db.exec(sql"DELETE FROM g WHERE v BETWEEN 1 AND 2")
    check db.getValue(sql"SELECT COUNT(*) FROM g") == "2"
    db.close()

  test "malformed BETWEEN fails loudly":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT)")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM t WHERE age BETWEEN 25")
    db.close()

suite "IN single-value":

  test "IN (v) works, IN (SELECT) fails clearly":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, age INT)")
    db.exec(sql"INSERT INTO t (age) VALUES (25), (30), (NULL)")
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age IN (25)") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age NOT IN (25)") == "1"
    check db.getValue(sql"SELECT COUNT(*) FROM t WHERE age IN (?)", "30") == "1"
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM t WHERE age IN (SELECT age FROM t)")
    db.close()
