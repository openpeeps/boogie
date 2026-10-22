import std/[os, json]

import ../src/boogie/stores/columnar

# The columnar store is shaped for analytics: tables live in a directory,
# rows go in as JSON batches, and columns are cached in memory after their
# first read so repeated scans only pay for parsing once. A temp directory
# keeps this example side-effect free.

let dir = getTempDir() / "boogie_columnar_example"
if dirExists(dir): removeDir(dir)
createDir(dir)

var s = openColumnarStore(dir)

# The schema declares each column's type once; batches must match it.
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
  %*{"id": 2, "user": "bob", "amount": 25.0}
])

# Scans project only the columns you ask for and can filter on the rest.
let rows = s.scan("events", @["id", "user"], filters = @[
  Filter(column: "amount", op: foGt, value: newJFloat(10.0))
])
for r in rows:
  echo r["id"].getInt(), " ", r["user"].getStr()

# Aggregations run over the same batches without materializing rows.
let ag = s.aggregate("events", @[
  AggregateSpec(column: "", kind: akCount, alias: "cnt"),
  AggregateSpec(column: "amount", kind: akAvg, alias: "avg_amount")
])
echo ag["cnt"].getInt(), " rows, avg amount ", ag["avg_amount"].getFloat()

removeDir(dir)
