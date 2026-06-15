use ddl_db

db.dropDatabase();

db.createCollection("rename_me");
db.createCollection("dropped_coll");

use ddl_drop_db

db.dropDatabase();

db.createCollection("to_be_dropped");
