import std/[unittest, os, times, json, sequtils]
import ../src/boogie/stores/graphstore

proc mkTestDir(name: string): string =
  let data = "tests" / "data"
  result = data / ("boogie-graphstore-" & name & "-" & $epochTime())
  createDir(result)

proc cleanupTestDir(path: string) =
  # graphstore currently writes a small fixed set of files
  let data = "tests" / "data" / path
  for f in ["nodes.gbin", "rels.gbin", "meta.gbin", "graph.wal",
            "nodes.gbin.tmp", "rels.gbin.tmp", "meta.gbin.tmp"]:
    if fileExists(data): removeFile(data / f)
  if dirExists(data): removeDir(path)

suite "graphstore":
  test "create node + relationship + query":
    let dir = mkTestDir("basic")
    defer: cleanupTestDir(dir)

    var gs = openGraphStore(dir)
    defer: closeGraphStore(gs)

    var tx = beginTx(gs)
    let aliceId = createNode(tx, @["Person"], %*{"name": "Alice"})
    let bobId = createNode(tx, @["Person"], %*{"name": "Bob"})
    let relId = createRelationship(tx, aliceId, bobId, "KNOWS", %*{"since": 2024})
    commit(tx)

    var alice: GraphNode
    check getNode(gs, aliceId, alice)
    check "Person" in alice.labels
    check alice.props["name"].getStr() == "Alice"

    var rel: Relationship
    check getRelationship(gs, relId, rel)
    check rel.fromId == aliceId
    check rel.toId == bobId
    check rel.relType == "KNOWS"

    let outp = outgoing(gs, aliceId)
    check outp.len == 1
    check outp[0].id == relId

    let neigh = neighbors(gs, aliceId)
    check neigh == @[bobId]

  test "rollback discards tx operations":
    let dir = mkTestDir("rollback")
    defer: cleanupTestDir(dir)

    var gs = openGraphStore(dir)
    defer: closeGraphStore(gs)

    var tx = beginTx(gs)
    let nid = createNode(tx, @["Temp"], %*{"v": 1})
    rollback(tx)

    var n: GraphNode
    check not getNode(gs, nid, n)

  test "delete node cascades relationships":
    let dir = mkTestDir("cascade")
    defer: cleanupTestDir(dir)

    var gs = openGraphStore(dir)
    defer: closeGraphStore(gs)

    var tx1 = beginTx(gs)
    let a = createNode(tx1, @["N"], %*{})
    let b = createNode(tx1, @["N"], %*{})
    let c = createNode(tx1, @["N"], %*{})
    let r1 = createRelationship(tx1, a, b, "E", %*{})
    let r2 = createRelationship(tx1, c, a, "E", %*{})
    commit(tx1)

    var tx2 = beginTx(gs)
    deleteNode(tx2, a)
    commit(tx2)

    var rel: Relationship
    check not getRelationship(gs, r1, rel)
    check not getRelationship(gs, r2, rel)

    check outgoing(gs, a).len == 0
    check neighbors(gs, c).len == 0

  test "persistence across reopen":
    let dir = mkTestDir("persist")
    defer: cleanupTestDir(dir)

    block:
      var gs = openGraphStore(dir)
      var tx = beginTx(gs)
      let p1 = createNode(tx, @["Person"], %*{"name": "Persisted"})
      let p2 = createNode(tx, @["Person"], %*{"name": "Node2"})
      discard createRelationship(tx, p1, p2, "LINKS", %*{})
      commit(tx)
      closeGraphStore(gs)

    block:
      var gs = openGraphStore(dir)
      defer: closeGraphStore(gs)

      let people = findNodesByLabel(gs, "Person")
      check people.len == 2

      let starts = people.filterIt(it.props.hasKey("name") and it.props["name"].getStr() == "Persisted")
      check starts.len == 1
      let startId = starts[0].id

      let bfs = traverseBfs(gs, startId, 2)
      check bfs.len == 2

  test "bfs + relType filter":
    let dir = mkTestDir("bfs")
    defer: cleanupTestDir(dir)

    var gs = openGraphStore(dir)
    defer: closeGraphStore(gs)

    var tx = beginTx(gs)
    let a = createNode(tx, @["N"], %*{})
    let b = createNode(tx, @["N"], %*{})
    let c = createNode(tx, @["N"], %*{})
    discard createRelationship(tx, a, b, "KNOWS", %*{})
    discard createRelationship(tx, b, c, "LIKES", %*{})
    commit(tx)

    let allReach = traverseBfs(gs, a, 2)
    check allReach.len == 3

    let knowsOnly = traverseBfs(gs, a, 2, "KNOWS")
    check knowsOnly.len == 2

  test "commit fails if relationship endpoints do not exist":
    let dir = mkTestDir("invalid-rel")
    defer: cleanupTestDir(dir)

    var gs = openGraphStore(dir)
    defer: closeGraphStore(gs)

    var tx = beginTx(gs)
    discard createRelationship(tx, 111'u64, 222'u64, "BROKEN", %*{})
    expect(GraphError):
      commit(tx)