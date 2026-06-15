use ddl_db

db.createCollection("created_coll");
db.created_coll.insertOne({ "_id": "created_doc", "name": "created_by_ddl" });

db.rename_me.renameCollection("renamed_coll");
db.renamed_coll.insertOne({ "_id": "renamed_doc", "name": "kept_after_rename" });

db.dropped_coll.drop();

db.createCollection("ignored_coll");
db.ignored_coll.insertOne({ "_id": "ignored_doc", "name": "should_not_sync" });

use ddl_drop_db

db.dropDatabase();
