import ../src/boogie/db_boogie

# The SQL driver speaks the db_connector API, so applications written
# against db_sqlite keep working by changing a single import. Statements
# are compiled once and cached per connection; placeholders stay symbolic
# so repeated executions never recompile. Here we use an in-memory
# database, while a file path would add WAL durability instead.

let db = open(":memory:", "", "", "app")

db.exec(sql"""CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age INT DEFAULT 0)""")

# Arguments replace the ? placeholders positionally.
db.exec(sql"INSERT INTO users (name, age) VALUES (?, ?)", "alice", "30")
echo db.insertID(sql"INSERT INTO users (name) VALUES (?)", "bob")

for row in db.fastRows(sql"SELECT id, name FROM users WHERE age > ? ORDER BY name", "18"):
  echo row

# Prepared statements go further: values bind with their real types and
# the same plan runs again with fresh bindings and no recompile.
let ps = db.prepare("UPDATE users SET age = age + 1 WHERE id = ?")
ps.bindParam(1, 1'i64)
db.exec(ps)

echo db.getValue(sql"SELECT COUNT(*) FROM users")
db.close()
