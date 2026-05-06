import std/[unittest, os, times, json]
import ../src/boogie/stores/columnar

proc mkTempRoot(prefix = "boogie-columnar-test"): string =
  result = "tests" / (prefix & "-" & $epochTime())
  createDir(result)

proc baseSchema(): TableSchema =
  TableSchema(
    name: "events",
    primaryKey: "id",
    rowCount: 0,
    columns: @[
      ColumnSchema(name: "id", kind: ctInt64, nullable: false, codec: ccNone),
      ColumnSchema(name: "user", kind: ctString, nullable: false, codec: ccNone),
      ColumnSchema(name: "amount", kind: ctFloat64, nullable: false, codec: ccNone),
      ColumnSchema(name: "ok", kind: ctBool, nullable: false, codec: ccNone)
    ]
  )

suite "columnar store":
  test "create table + batch insert + projection scan":
    let root = mkTempRoot()
    defer:
      if dirExists(root): removeDir(root)

    var s = openColumnarStore(root)
    s.createTable(baseSchema())

    s.insertBatch("events", @[
      %*{"id": 1, "user": "alice", "amount": 10.5, "ok": true},
      %*{"id": 2, "user": "bob", "amount": 25.0, "ok": false},
      %*{"id": 3, "user": "alice", "amount": 7.25, "ok": true}
    ])

    let rows = s.scan("events", @["id", "user"])
    check rows.len == 3
    check rows[0]["id"].getInt() == 1
    check rows[1]["user"].getStr() == "bob"

  test "filters work (gt + in)":
    let root = mkTempRoot()
    defer:
      if dirExists(root): removeDir(root)

    var s = openColumnarStore(root)
    s.createTable(baseSchema())
    s.insertBatch("events", @[
      %*{"id": 1, "user": "alice", "amount": 10.5, "ok": true},
      %*{"id": 2, "user": "bob", "amount": 25.0, "ok": false},
      %*{"id": 3, "user": "chris", "amount": 7.25, "ok": true}
    ])

    let gtRows = s.scan("events", @["id"], filters = @[
      Filter(column: "amount", op: foGt, value: newJFloat(10.0))
    ])
    check gtRows.len == 2

    let inRows = s.scan("events", @["user"], filters = @[
      Filter(column: "user", op: foIn, values: @[newJString("alice"), newJString("chris")])
    ])
    check inRows.len == 2

  test "aggregates (count/sum/min/max/avg)":
    let root = mkTempRoot()
    defer:
      if dirExists(root): removeDir(root)

    var s = openColumnarStore(root)
    s.createTable(baseSchema())
    s.insertBatch("events", @[
      %*{"id": 1, "user": "alice", "amount": 10.0, "ok": true},
      %*{"id": 2, "user": "bob", "amount": 20.0, "ok": false},
      %*{"id": 3, "user": "chris", "amount": 30.0, "ok": true}
    ])

    let ag = s.aggregate("events", @[
      AggregateSpec(column: "", kind: akCount, alias: "cnt"),
      AggregateSpec(column: "amount", kind: akSum, alias: "sum_amount"),
      AggregateSpec(column: "amount", kind: akMin, alias: "min_amount"),
      AggregateSpec(column: "amount", kind: akMax, alias: "max_amount"),
      AggregateSpec(column: "amount", kind: akAvg, alias: "avg_amount")
    ])

    check ag["cnt"].getInt() == 3
    check ag["sum_amount"].getFloat() == 60.0
    check ag["min_amount"].getFloat() == 10.0
    check ag["max_amount"].getFloat() == 30.0
    check ag["avg_amount"].getFloat() == 20.0

  test "duplicate primary key raises":
    let root = mkTempRoot()
    defer:
      if dirExists(root): removeDir(root)

    var s = openColumnarStore(root)
    s.createTable(baseSchema())
    s.insertBatch("events", @[
      %*{"id": 1, "user": "alice", "amount": 10.5, "ok": true}
    ])

    expect ColumnarError:
      s.insertBatch("events", @[
        %*{"id": 1, "user": "bob", "amount": 12.0, "ok": false}
      ])

  test "data persists after reopen":
    let root = mkTempRoot()
    defer:
      if dirExists(root): removeDir(root)

    block:
      var s = openColumnarStore(root)
      s.createTable(baseSchema())
      s.insertBatch("events", @[
        %*{"id": 1, "user": "alice", "amount": 10.5, "ok": true},
        %*{"id": 2, "user": "bob", "amount": 25.0, "ok": false}
      ])

    block:
      let s2 = openColumnarStore(root)
      let rows = s2.scan("events", @["id", "user", "amount", "ok"])
      check rows.len == 2
      check rows[0]["user"].getStr() == "alice"
      check rows[1]["amount"].getFloat() == 25.0