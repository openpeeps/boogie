import std/[unittest, times, sequtils, algorithm, strutils]
import ../src/boogie/sqlengine
import ../src/boogie/sqlengine/jit
import ../src/boogie/stores/rdbms

# ---------------------------------------------------------------------------
# Differential tests for the JIT WHERE predicates: every query runs once as
# a single statement (native fast path / interpreter) and once inside a
# multi-statement string (VM loop, JIT predicate when the shape is in the
# compilable subset). Results must be identical.
#
# A/B benchmark at the end compares the VM loop with the JIT predicate
# against the pure interpreter (`jitEnabled = false`, distinct SQL text so
# the plan cache cannot share compiled plans across modes).
# ---------------------------------------------------------------------------

proc newEng(): SqlEngine =
  newSqlEngine(newInMemoryStore())

proc setupT(eng: SqlEngine) =
  discard eng.execSql("CREATE TABLE t (id INTEGER PRIMARY KEY, n INT, m INT, b BOOL)")
  for i in 1..200:
    let n = if i mod 7 == 0: "NULL" else: $i
    let m = if i mod 11 == 0: "NULL" else: $(200 - i)
    let b = if i mod 13 == 0: "NULL"
            elif i mod 2 == 0: "true"
            else: "false"
    discard eng.execSql("INSERT INTO t (n, m, b) VALUES (" & n & ", " & m & ", " & b & ")")

proc rowsOf(res: ExecResult): seq[string] =
  res.rows.mapIt(it.join(",")).sorted()

proc checkBoth(pred: string) =
  ## Runs `SELECT id, n, m, b FROM t WHERE <pred>` single (fast path) and
  ## `SELECT ...; SELECT ... WHERE <pred>` (VM + JIT), comparing row sets.
  let eng = newEng()
  setupT(eng)
  let singleSql = "SELECT id, n, m, b FROM t WHERE " & pred
  let multiSql = "SELECT id FROM t WHERE id < 0; SELECT id, n, m, b FROM t WHERE " & pred
  let a = rowsOf(eng.execSql(singleSql))
  let b = rowsOf(eng.execSql(multiSql))
  check a == b

suite "JIT predicate differential battery":
  test "integer comparisons":
    for pred in ["n > 5", "n <= 5", "n = 7", "n <> 7", "n < 0", "5 < n",
                 "n = m", "n > m", "m <= n", "n <> m", "200 >= n",
                 "n > 100 AND m > 100"]:
      checkBoth(pred)

  test "boolean comparisons":
    for pred in ["b = true", "b = false", "b <> true", "b = b",
                 "b = true AND n > 5", "b = false OR n < 3"]:
      checkBoth(pred)

  test "IS NULL":
    for pred in ["n IS NULL", "n IS NOT NULL", "b IS NULL", "b IS NOT NULL",
                 "m IS NULL AND n > 5", "n IS NOT NULL AND b = true"]:
      checkBoth(pred)

  test "NULL constants":
    for pred in ["n = NULL", "n <> NULL", "NULL = NULL", "n + NULL > 0",
                 "NULL > 0", "n IS NULL OR n > 190"]:
      checkBoth(pred)

  test "arithmetic":
    for pred in ["n + 1 > 5", "n - m > 0", "n * 2 = 10", "2 * n + m > 100",
                 "-n < -5", "(n + m) * 2 > 10", "n + m * 2 > 200",
                 "n * m > 1000", "0 - n < -190"]:
      checkBoth(pred)

  test "AND OR nesting":
    for pred in ["n > 5 AND m > 5 AND b = true",
                 "n > 195 OR m > 195 OR b = false",
                 "(n > 5 OR n < 2) AND m <> 3",
                 "n > 5 AND (m > 5 OR b = true)",
                 "NOT (n > 195)", "NOT b"]:
      checkBoth(pred)

  test "interpreter-only shapes stay correct":
    for pred in ["n / 2 > 2", "m = m + 0", "n > 5 OR b = true"]:
      checkBoth(pred)

  test "jit engaged for at least one shape":
    let eng = newEng()
    setupT(eng)
    let before = eng.jitPredCount()
    discard eng.execSql("SELECT id FROM t WHERE id < 0; SELECT id FROM t WHERE n > 5")
    check eng.jitPredCount() > before

  test "unknown column still errors on the JIT path":
    let eng = newEng()
    setupT(eng)
    var raisedSingle = false
    try:
      discard eng.execSql("SELECT id FROM t WHERE nope > 5")
    except SqlEngineError:
      raisedSingle = true
    check raisedSingle
    var raisedMulti = false
    try:
      discard eng.execSql("SELECT id FROM t WHERE id < 0; SELECT id FROM t WHERE nope > 5")
    except SqlEngineError:
      raisedMulti = true
    check raisedMulti

  test "integer overflow raises on both paths":
    for sql in ["SELECT id, n FROM t WHERE n + 1 > 0",
                "SELECT id FROM t WHERE id < 0; SELECT id, n FROM t WHERE n + 1 > 0",
                "SELECT id, n FROM t WHERE n * 2 > 0",
                "SELECT id FROM t WHERE id < 0; SELECT id, n FROM t WHERE n * 2 > 0"]:
      let eng = newEng()
      setupT(eng)
      # push n to int64.high first
      discard eng.execSql("INSERT INTO t (n, m, b) VALUES (9223372036854775807, 1, true)")
      var raised = false
      try:
        discard eng.execSql(sql)
      except OverflowDefect:
        raised = true
      check raised
  
suite "JIT predicate A/B benchmark":
  test "scan filter JIT vs interpreter":
    const N = 20_000
    let eng = newEng()
    discard eng.execSql("CREATE TABLE b (id INTEGER PRIMARY KEY, n INT)")
    for i in 1..N:
      discard eng.execSql("INSERT INTO b (n) VALUES (?)", [initValue(int64(i))])
    # Warm up both plans (distinct SQL texts so the plan cache cannot
    # share a compiled plan across modes).
    let jitSql = "SELECT id FROM b WHERE id < 0; SELECT id, n FROM b WHERE n > 5000 AND n < 15000"
    let intSql = "SELECT id FROM b WHERE id < 0; SELECT id, n FROM b WHERE n > 5001 AND n < 15001"
    check rowsOf(eng.execSql(jitSql)).len == rowsOf(eng.execSql(intSql)).len
    jitEnabled = true
    let t0 = cpuTime()
    for _ in 0..<5:
      discard eng.execSql(jitSql)
    let jitSecs = cpuTime() - t0
    jitEnabled = false
    let t1 = cpuTime()
    for _ in 0..<5:
      discard eng.execSql(intSql)
    let intSecs = cpuTime() - t1
    jitEnabled = true
    echo "[bench][jit-pred] jit=", float(5 * N) / jitSecs,
      " rows/s interp=", float(5 * N) / intSecs, " rows/s"
