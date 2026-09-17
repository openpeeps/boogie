# Multi-process race worker: SQL driver (db_boogie).
# Usage: worker_sql <basePath> <workerId> <numOps>
import std/[os, strutils]
import ../../src/boogie/db_boogie

proc main() =
  if paramCount() < 3:
    quit("usage: worker_sql <basePath> <workerId> <numOps>", 1)
  let base = paramStr(1)
  let wid = parseInt(paramStr(2))
  let n = parseInt(paramStr(3))
  let db = open(base, "", "", "mp")
  for i in 0 ..< n:
    db.exec(sql"INSERT INTO b (v) VALUES (?)", "w" & $wid & "_" & $i)
  db.close()

main()
