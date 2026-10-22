import std/[os, json]

import ../src/boogie/stores/graphstore

# The graph store keeps labeled nodes with JSON properties plus typed
# relationships between them, all in a directory with WAL durability.
# Writes go through an explicit transaction so a group of nodes and edges
# either lands together or not at all. A temp directory keeps this
# example side-effect free.

let dir = getTempDir() / "boogie_graph_example"
if dirExists(dir): removeDir(dir)
createDir(dir)

var gs = openGraphStore(dir)

var tx = beginTx(gs)
let alice = createNode(tx, @["Person"], %*{"name": "Alice"})
let bob = createNode(tx, @["Person"], %*{"name": "Bob"})
discard createRelationship(tx, alice, bob, "KNOWS", %*{"since": 2024})
commit(tx)

# Neighborhood walks stay local: neighbors and outgoing edges never scan
# the whole graph, they start from the node you already hold.
echo gs.neighbors(alice)
for r in gs.outgoing(alice, "KNOWS"):
  echo r.toId

# Labels act as a coarse index when you need every node of a kind.
for n in gs.findNodesByLabel("Person"):
  echo n.id

echo gs.traverseBfs(alice, maxDepth = 2).len, " nodes within two hops"

closeGraphStore(gs)
removeDir(dir)
