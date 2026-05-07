# Boogie - A suite of WAL-based embedded data stores.
# RDBMS, KV Store, GraphStore, VectorStore, Columnar and more 
#
# (c) 2025 George Lemon | LGPL-3.0-or-later License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie

when not defined(builddocs):
  {.error: "This module is not meant to be imported directly. Import a specific store module instead".}
else:
  import ./boogie/wal
  export wal

  import ./boogie/stores/[columnar, rdbms, kv, graphstore, vectorstore]
  export columnar, rdbms, kv, graphstore, vectorstore