# Boogie - A stupid simple embedded database for Nim
#
# (c) 2025 George Lemon | LGPLv3 License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/boogie
when not defined(builddocs):
  {.error: "This module is not meant to be imported directly. Import a specific store module instead".}
else:
  import ./boogie/wal
  export wal

  import ./boogie/stores/[columnar, rdbms, kvstore, graphstore, vectorstore]
  export columnar, rdbms, kvstore, graphstore, vectorstore