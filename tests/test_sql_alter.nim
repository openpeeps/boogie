import std/[unittest, os, times, monotimes, strutils]
import ../src/boogie/db_boogie
import ../src/boogie/sqlengine
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# ALTER TABLE: ADD COLUMN, RENAME COLUMN, RENAME TO (+ WAL persistence).
# DROP COLUMN is unsupported: the SQL parser drops the clause, so an
# action-less ALTER fails loudly instead of silently succeeding.
# ---------------------------------------------------------------------------

proc testRoot(): string =
  let unique = $getTime().toUnix() & "_" & $getMonoTime().ticks
  let base = "tests" / "data"
  if not dirExists(base):
    createDir(base)
  result = base / ("boogie_alter_tests_" & unique)
  createDir(result)

suite "ADD COLUMN":

  test "nullable add backfills NULL, later inserts work":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"ALTER TABLE t ADD COLUMN w TEXT")
    check db.getAllRows(sql"SELECT id, v, w FROM t") == @[@["1", "1", ""]]
    db.exec(sql"INSERT INTO t (v, w) VALUES (2, 'hi')")
    check db.getValue(sql"SELECT w FROM t WHERE id = 2") == "hi"
    db.close()

  test "NOT NULL with DEFAULT backfills and applies":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"ALTER TABLE t ADD COLUMN w INT NOT NULL DEFAULT 7")
    check db.getValue(sql"SELECT w FROM t WHERE id = 1") == "7"
    db.exec(sql"INSERT INTO t (v) VALUES (2)")
    check db.getValue(sql"SELECT w FROM t WHERE id = 2") == "7"
    db.exec(sql"INSERT INTO t (v, w) VALUES (3, 9)")
    check db.getValue(sql"SELECT w FROM t WHERE id = 3") == "9"
    db.close()

  test "missing type defaults to TEXT":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY)")
    db.exec(sql"ALTER TABLE t ADD COLUMN note")
    db.exec(sql"INSERT INTO t (note) VALUES ('x')")
    check db.getValue(sql"SELECT note FROM t WHERE id = 1") == "x"
    db.close()

  test "cached plans see the new column":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    check db.getValue(sql"SELECT v FROM t WHERE id = 1") == "1"
    db.exec(sql"ALTER TABLE t ADD COLUMN w INT DEFAULT 3")
    check db.getValue(sql"SELECT w FROM t WHERE id = 1") == "3"
    check db.getValue(sql"SELECT v, w FROM t WHERE w = 3") == "1"
    db.close()

  test "ADD COLUMN rejections":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    expect DbError: # duplicate
      db.exec(sql"ALTER TABLE t ADD COLUMN v INT")
    expect DbError: # no new PK
      db.exec(sql"ALTER TABLE t ADD COLUMN k INT PRIMARY KEY")
    expect DbError: # inline UNIQUE unsupported
      db.exec(sql"ALTER TABLE t ADD COLUMN u INT UNIQUE")
    expect DbError: # inline REFERENCES unsupported
      db.exec(sql"ALTER TABLE t ADD COLUMN r INT REFERENCES o (id)")
    expect DbError: # NOT NULL without default on non-empty table
      db.exec(sql"ALTER TABLE t ADD COLUMN n INT NOT NULL")
    expect DbError: # unknown table
      db.exec(sql"ALTER TABLE nope ADD COLUMN w INT")
    expect DbError: # action-less ALTER (parser swallows DROP COLUMN)
      db.exec(sql"ALTER TABLE t DROP COLUMN v")
    db.close()

  test "NOT NULL without DEFAULT allowed on empty table":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY)")
    db.exec(sql"ALTER TABLE t ADD COLUMN w INT NOT NULL DEFAULT 0")
    db.exec(sql"INSERT INTO t (id) VALUES (1)")
    check db.getValue(sql"SELECT w FROM t WHERE id = 1") == "0"
    db.close()

suite "RENAME COLUMN":

  test "rename preserves data, indexes follow":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
    db.exec(sql"INSERT INTO t (email) VALUES ('a@x.io')")
    db.exec(sql"CREATE UNIQUE INDEX idx_email ON t (email)")
    db.exec(sql"ALTER TABLE t RENAME COLUMN email TO mail")
    check db.getValue(sql"SELECT mail FROM t WHERE id = 1") == "a@x.io"
    expect DbError:
      db.exec(sql"SELECT email FROM t WHERE id = 1")
    expect DbError: # uniqueness still enforced under the new name
      db.exec(sql"INSERT INTO t (mail) VALUES ('a@x.io')")
    check db.getValue(sql"SELECT mail FROM t WHERE mail = 'a@x.io'") == "a@x.io"
    db.close()

  test "rename PRIMARY KEY column":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (5)")
    db.exec(sql"ALTER TABLE t RENAME COLUMN id TO pk")
    check db.getValue(sql"SELECT v FROM t WHERE pk = 1") == "5"
    db.exec(sql"INSERT INTO t (v) VALUES (6)")
    check db.getValue(sql"SELECT COUNT(*) FROM t") == "2"
    db.close()

  test "rename rejections":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    expect DbError:
      db.exec(sql"ALTER TABLE t RENAME COLUMN nope TO w")
    expect DbError:
      db.exec(sql"ALTER TABLE t RENAME COLUMN v TO id")
    expect DbError:
      db.exec(sql"ALTER TABLE nope RENAME COLUMN v TO w")
    db.close()

  test "rename keeps child FOREIGN KEYs working":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE p (id INTEGER PRIMARY KEY)")
    db.exec(sql"INSERT INTO p (id) VALUES (1)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT REFERENCES p (id))")
    db.exec(sql"INSERT INTO c (pid) VALUES (1)")
    expect DbError:
      db.exec(sql"INSERT INTO c (pid) VALUES (999)")
    db.exec(sql"ALTER TABLE p RENAME COLUMN id TO pk")
    db.exec(sql"INSERT INTO c (pid) VALUES (1)")
    expect DbError:
      db.exec(sql"INSERT INTO c (pid) VALUES (999)")
    check db.getValue(sql"SELECT COUNT(*) FROM c") == "2"
    db.close()

suite "RENAME TO":

  test "rename table moves data, indexes, FK refs":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE p (id INTEGER PRIMARY KEY)")
    db.exec(sql"INSERT INTO p (id) VALUES (1)")
    db.exec(sql"CREATE TABLE c (id INTEGER PRIMARY KEY, pid INT REFERENCES p (id))")
    db.exec(sql"INSERT INTO c (pid) VALUES (1)")
    db.exec(sql"CREATE INDEX idx_pid ON c (pid)")
    db.exec(sql"ALTER TABLE p RENAME TO parent")
    expect DbError:
      discard db.getAllRows(sql"SELECT * FROM p")
    check db.getValue(sql"SELECT COUNT(*) FROM parent") == "1"
    db.exec(sql"INSERT INTO c (pid) VALUES (1)")
    expect DbError:
      db.exec(sql"INSERT INTO c (pid) VALUES (999)")
    check db.getValue(sql"SELECT COUNT(*) FROM c WHERE pid = 1") == "2"
    db.close()

  test "rename to existing table fails":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE a (id INTEGER PRIMARY KEY)")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY)")
    expect DbError:
      db.exec(sql"ALTER TABLE a RENAME TO b")
    db.close()

suite "ALTER misc":

  test "multiple actions apply sequentially":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    db.exec(sql"INSERT INTO t (v) VALUES (1)")
    db.exec(sql"ALTER TABLE t ADD COLUMN a INT DEFAULT 1, ADD COLUMN b TEXT")
    check db.getAllRows(sql"SELECT a, b FROM t") == @[@["1", ""]]
    db.exec(sql"ALTER TABLE t RENAME COLUMN a TO aa, RENAME COLUMN b TO bb")
    check db.getAllRows(sql"SELECT aa, bb FROM t") == @[@["1", ""]]
    db.close()

  test "ALTER via multi-statement VM path":
    # NOTE: DDL and its dependents cannot share one multi-statement string:
    # every statement is lowered before any runs (pre-existing for all DDL).
    let store = newInMemoryStore()
    var eng = newSqlEngine(store)
    discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
    discard eng.execSql("INSERT INTO t (v) VALUES (1)")
    let r = eng.execSql("SELECT COUNT(*) FROM t; ALTER TABLE t ADD COLUMN w INT DEFAULT 4")
    check r.kind == skAlterTable
    check r.affected == 1
    check eng.execSql("SELECT w FROM t").rows == @[@["4"]]
    let r2 = eng.execSql("ALTER TABLE t RENAME COLUMN w TO ww; SELECT COUNT(*) FROM t")
    check r2.kind == skSelect
    check r2.rows == @[@["1"]]
    check eng.execSql("SELECT ww FROM t").rows == @[@["4"]]
    store.close()

  test "ALTER persists across reopen":
    let root = testRoot()
    block:
      let db = open(root / "a.db", "", "", "")
      db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, v INT)")
      db.exec(sql"INSERT INTO t (v) VALUES (1)")
      db.exec(sql"ALTER TABLE t ADD COLUMN w TEXT NOT NULL DEFAULT 'z'")
      db.exec(sql"ALTER TABLE t RENAME COLUMN v TO vv")
      db.exec(sql"ALTER TABLE t RENAME TO t2")
      db.close()
    block:
      let db = open(root / "a.db", "", "", "")
      check db.getAllRows(sql"SELECT vv, w FROM t2") == @[@["1", "z"]]
      db.exec(sql"INSERT INTO t2 (vv) VALUES (2)")
      check db.getValue(sql"SELECT w FROM t2 WHERE vv = 2") == "z"
      db.close()

  test "DEFAULT types survive WAL replay":
    let root = testRoot()
    block:
      let db = open(root / "d.db", "", "", "")
      db.exec(sql"CREATE TABLE t (id INTEGER PRIMARY KEY, a INT DEFAULT 42, s TEXT DEFAULT 'hi', f REAL DEFAULT 1.5, b BOOL DEFAULT true)")
      db.exec(sql"INSERT INTO t (id) VALUES (1)")
      check db.getValue(sql"SELECT a FROM t WHERE id = 1") == "42"
      db.close()
    block:
      let db = open(root / "d.db", "", "", "")
      db.exec(sql"INSERT INTO t (id) VALUES (2)")
      check db.getValue(sql"SELECT a FROM t WHERE id = 2") == "42"
      check db.getValue(sql"SELECT s FROM t WHERE id = 2") == "hi"
      check db.getValue(sql"SELECT f FROM t WHERE id = 2") == "1.5"
      check db.getValue(sql"SELECT b FROM t WHERE id = 2") == "1"
      db.close()
