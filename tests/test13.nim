import std/[unittest, os, times, strformat, monotimes]
import ../src/boogie/db_boogie

# ---------------------------------------------------------------------------
# SQL VM benchmarks: in-memory vs disk+WAL, measuring ops/s for insert,
# point query, range scan, update, delete, aggregate, prepared statements,
# ORDER BY, and DISTINCT.
#
# Each benchmark is self-contained (creates its own tables) so results are
# independent. Results are printed in the same [bench] format as test11.
# ---------------------------------------------------------------------------

const
  InsN = 10_000
  QueryN = 1_000
  ScanN = 5_000
  SmallN = 500
  AggN = 5_000

template timeIt(body: untyped): float =
  let t0 = getMonoTime()
  body
  float((getMonoTime() - t0).inMicroseconds) / 1_000_000.0

suite "benchmarks (in-memory)":

  test "insert":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")
    let secs = timeIt:
      for i in 1 .. InsN:
        db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    echo fmt"[bench][sqlvm][mem] insert={float(InsN) / secs:>10.0f} ops/s"
    db.close()

  test "point query (unindexed scan)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")
    for i in 1 .. QueryN:
      db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    var found = 0
    let secs = timeIt:
      for i in 1 .. QueryN:
        let v = db.getValue(sql"SELECT id FROM b WHERE v = ?", "v" & $i)
        if v.len > 0: inc found
    check found == QueryN
    echo fmt"[bench][sqlvm][mem] pointQuery={float(QueryN) / secs:>9.0f} q/s"
    db.close()

  test "range scan (ORDER BY + LIMIT)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")
    for i in 1 .. ScanN:
      db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    var count = 0
    let secs = timeIt:
      for _ in db.fastRows(sql"SELECT id, v FROM b ORDER BY n DESC LIMIT 100"):
        inc count
    check count == 100
    echo fmt"[bench][sqlvm][mem] rangeScan={float(count) / secs:>10.0f} rows/s"
    db.close()

  test "full scan (SELECT *)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")
    for i in 1 .. ScanN:
      db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    var count = 0
    let secs = timeIt:
      for _ in db.fastRows(sql"SELECT * FROM b"):
        inc count
    check count == ScanN
    echo fmt"[bench][sqlvm][mem] fullScan={float(ScanN) / secs:>10.0f} rows/s"
    db.close()

  test "update (full scan per statement)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1 .. SmallN:
      db.exec(sql"INSERT INTO b (n) VALUES (?)", $i)
    let secs = timeIt:
      for i in 1 .. SmallN:
        db.exec(sql"UPDATE b SET n = n + 1 WHERE id = ?", $i)
    echo fmt"[bench][sqlvm][mem] update={float(SmallN) / secs:>9.0f} ops/s"
    db.close()

  test "delete (full scan per statement)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1 .. SmallN:
      db.exec(sql"INSERT INTO b (n) VALUES (?)", $i)
    let secs = timeIt:
      for i in 1 .. SmallN:
        db.exec(sql"DELETE FROM b WHERE n = ?", $i)
    echo fmt"[bench][sqlvm][mem] delete={float(SmallN) / secs:>9.0f} ops/s"
    db.close()

  test "aggregate (COUNT/SUM/AVG/MIN/MAX)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1 .. AggN:
      db.exec(sql"INSERT INTO b (n) VALUES (?)", $i)
    let secs = timeIt:
      discard db.getValue(sql"SELECT COUNT(*) FROM b")
      discard db.getValue(sql"SELECT SUM(n) FROM b")
      discard db.getValue(sql"SELECT AVG(n) FROM b")
      discard db.getValue(sql"SELECT MIN(n) FROM b")
      discard db.getValue(sql"SELECT MAX(n) FROM b")
    echo fmt"[bench][sqlvm][mem] aggregate={5 * AggN} cells in {secs:.3f}s"
    db.close()

  test "prepared statement rebind":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v INT)")
    for i in 1 .. QueryN:
      db.exec(sql"INSERT INTO b (v) VALUES (?)", $i)
    let ps = db.prepare("SELECT v FROM b WHERE id = ?")
    var found = 0
    let secs = timeIt:
      for i in 1 .. QueryN:
        ps.bindParam(1, i)
        let v = db.getValue(ps)
        if v.len > 0: inc found
    check found == QueryN
    echo fmt"[bench][sqlvm][mem] preparedQuery={float(QueryN) / secs:>9.0f} q/s"
    db.close()

  test "ORDER BY (full sort)":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
    for i in 1 .. ScanN:
      db.exec(sql"INSERT INTO b (v) VALUES (?)", "v" & $((ScanN - i) mod 1000))
    var count = 0
    let secs = timeIt:
      for _ in db.fastRows(sql"SELECT v FROM b ORDER BY v"):
        inc count
    check count == ScanN
    echo fmt"[bench][sqlvm][mem] orderBy={float(ScanN) / secs:>10.0f} rows/s"
    db.close()

  test "DISTINCT":
    let db = open(":memory:", "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v INT)")
    for i in 1 .. ScanN:
      db.exec(sql"INSERT INTO b (v) VALUES (?)", $(i mod 100))
    var count = 0
    let secs = timeIt:
      for _ in db.fastRows(sql"SELECT DISTINCT v FROM b"):
        inc count
    check count == 100
    echo fmt"[bench][sqlvm][mem] distinct={float(count) / secs:>10.0f} unique/s"
    db.close()

suite "benchmarks (disk + WAL)":

  test "insert (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk.db"
    removeFile(path)
    removeFile(path & ".wal")
    let db = open(path, "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT, n INT)")
    let secs = timeIt:
      for i in 1 .. InsN:
        db.exec(sql"INSERT INTO b (v, n) VALUES (?, ?)", "v" & $i, $i)
    echo fmt"[bench][sqlvm][disk] insert={float(InsN) / secs:>10.0f} ops/s"
    db.close()

  test "point query (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk.db"
    let db = open(path, "", "", "")
    var found = 0
    let secs = timeIt:
      for i in 1 .. QueryN:
        let v = db.getValue(sql"SELECT id FROM b WHERE v = ?", "v" & $i)
        if v.len > 0: inc found
    check found == QueryN
    echo fmt"[bench][sqlvm][disk] pointQuery={float(QueryN) / secs:>9.0f} q/s"
    db.close()

  test "full scan (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk.db"
    let db = open(path, "", "", "")
    var count = 0
    let secs = timeIt:
      for _ in db.fastRows(sql"SELECT * FROM b"):
        inc count
    check count == InsN
    echo fmt"[bench][sqlvm][disk] fullScan={float(InsN) / secs:>10.0f} rows/s"
    db.close()

  test "update (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk_updel.db"
    removeFile(path)
    removeFile(path & ".wal")
    let db = open(path, "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1 .. SmallN:
      db.exec(sql"INSERT INTO b (n) VALUES (?)", $i)
    let secs = timeIt:
      for i in 1 .. SmallN:
        db.exec(sql"UPDATE b SET n = n + 1 WHERE id = ?", $i)
    echo fmt"[bench][sqlvm][disk] update={float(SmallN) / secs:>9.0f} ops/s"
    db.close()

  test "delete (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk_updel.db"
    let db = open(path, "", "", "")
    let secs = timeIt:
      for i in 1 .. SmallN:
        db.exec(sql"DELETE FROM b WHERE n = ?", $(i + 1))
    echo fmt"[bench][sqlvm][disk] delete={float(SmallN) / secs:>9.0f} ops/s"
    db.close()

  test "aggregate (disk)":
    let path = "tests" / "data" / "bench_sqlvm_disk_agg.db"
    removeFile(path)
    removeFile(path & ".wal")
    let db = open(path, "", "", "")
    db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1 .. AggN:
      db.exec(sql"INSERT INTO b (n) VALUES (?)", $i)
    let secs = timeIt:
      discard db.getValue(sql"SELECT COUNT(*) FROM b")
      discard db.getValue(sql"SELECT SUM(n) FROM b")
      discard db.getValue(sql"SELECT AVG(n) FROM b")
      discard db.getValue(sql"SELECT MIN(n) FROM b")
      discard db.getValue(sql"SELECT MAX(n) FROM b")
    echo fmt"[bench][sqlvm][disk] aggregate={5 * AggN} cells in {secs:.3f}s"
    db.close()

  test "WAL persistence roundtrip":
    let path = "tests" / "data" / "bench_sqlvm_persist.db"
    removeFile(path)
    removeFile(path & ".wal")
    block:
      let db = open(path, "", "", "")
      db.exec(sql"CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)")
      for i in 1 .. 1_000:
        db.exec(sql"INSERT INTO b (v) VALUES (?)", "val_" & $i)
      db.close()

    let db2 = open(path, "", "", "")
    check db2.getValue(sql"SELECT COUNT(*) FROM b") == "1000"
    check db2.getValue(sql"SELECT v FROM b WHERE id = 500") == "val_500"
    db2.close()
