use ddl_db

db.createCollection("created_coll");
db.created_coll.insertOne({ "_id": "created_doc", "name": "created_by_ddl", "profile": { "state": "created", "nested": { "level": 1 } }, "attrs": ["seed"], "history": [{ "step": 1, "state": "created" }] });
db.created_coll.updateOne({ "_id": "created_doc" }, { "$set": { "profile.state": "updated_after_create", "profile.nested.level": 2, "profile.nested.flags": ["ddl", "dml"], "history.0.state": "updated_seed" } });
db.created_coll.updateOne({ "_id": "created_doc" }, { "$push": { "attrs": "after_update", "history": { "step": 2, "state": "updated_after_create" } } });
db.created_coll.insertOne({ "_id": "created_after_ddl_doc", "name": "created_after_ddl", "profile": { "state": "inserted" }, "attrs": ["after_ddl"] });

db.rename_me.insertOne({ "_id": "renamed_doc", "name": "before_rename", "profile": { "state": "before_rename", "nested": { "version": 1 } }, "attrs": ["before"] });
db.rename_me.updateOne({ "_id": "renamed_doc" }, { "$set": { "profile.state": "updated_before_rename", "profile.nested.version": 2, "profile.nested.tags": ["pre", "rename"] }, "$push": { "attrs": "updated_before_rename" } });
db.rename_me.renameCollection("renamed_coll");
db.renamed_coll.updateOne({ "_id": "renamed_doc" }, { "$set": { "profile.state": "updated_after_rename", "profile.nested.version": 3 }, "$push": { "attrs": "updated_after_rename" } });
db.renamed_coll.insertOne({ "_id": "renamed_temp_doc", "name": "delete_after_rename", "profile": { "state": "temp" }, "attrs": ["temp"] });
db.renamed_coll.deleteOne({ "_id": "renamed_temp_doc" });
db.runCommand({ "update": "renamed_coll", "updates": [{ "q": { "_id": "renamed_doc" }, "u": { "_id": "renamed_doc", "name": "replaced_after_rename", "profile": { "state": "replaced_after_rename", "nested": { "version": 4, "tags": ["replace", "after_rename"] } }, "attrs": ["replace", "after_rename"], "history": [{ "step": 1, "state": "replaced_after_rename" }] }, "multi": false }] });

db.dropped_coll.insertOne({ "_id": "drop_doc", "name": "before_drop", "profile": { "state": "inserted" }, "attrs": ["drop"] });
db.dropped_coll.updateOne({ "_id": "drop_doc" }, { "$set": { "profile.state": "updated_before_drop", "attrs": ["drop", "updated"] } });
db.dropped_coll.deleteOne({ "_id": "drop_doc" });
db.dropped_coll.drop();

db.createCollection("ignored_coll");
db.ignored_coll.insertOne({ "_id": "ignored_doc", "name": "should_not_sync" });

use ddl_drop_db

db.dropDatabase();
