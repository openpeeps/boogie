import std/[os, json]

import ../src/boogie/stores/columnar

# Disk-backed columnar store: the table and its columns are ordinary files
# under `dir`, so a store can be closed and reopened later without losing
# data. This example writes a batch in one session and reads it back in
# another, using only the on-disk files.

let dir = getTempDir() / "boogie_columnar_disk_example"
if dirExists(dir): removeDir(dir)
createDir(dir)

# First session: create the schema and append rows. The data is flushed to
# the column files under `dir` before close() returns.
block:
  var s = openColumnarStore(dir)
  s.createTable(TableSchema(
    name: "events",
    primaryKey: "id",
    rowCount: 0,
    columns: @[
      ColumnSchema(name: "id", kind: ctInt64, nullable: false, codec: ccNone),
      ColumnSchema(name: "user", kind: ctString, nullable: false, codec: ccNone),
      ColumnSchema(name: "amount", kind: ctFloat64, nullable: false, codec: ccNone)
    ]
  ))

  s.insertBatch("events", @[
    %*{"id": 1, "user": "alice", "amount": 10.5},
    %*{"id": 2, "user": "bob", "amount": 25.0},
    %*{"id": 3, "user": "alice", "amount": 7.25}
  ])
  s.close()

# Second session: reopen from the same directory. The schema and rows are
# recovered from the files on disk; the first scan parses the column files
# (cold), later scans reuse the in-memory cache (warm).
block:
  var s = openColumnarStore(dir)

  let rows = s.scan("events", @["id", "user"], filters = @[
    Filter(column: "amount", op: foGt, value: newJFloat(10.0))
  ])
  for r in rows:
    echo r["id"].getInt(), " ", r["user"].getStr()

  let ag = s.aggregate("events", @[
    AggregateSpec(column: "", kind: akCount, alias: "cnt"),
    AggregateSpec(column: "amount", kind: akAvg, alias: "avg_amount")
  ])
  echo ag["cnt"].getInt(), " rows, avg amount ", ag["avg_amount"].getFloat()

  s.close()

removeDir(dir)
