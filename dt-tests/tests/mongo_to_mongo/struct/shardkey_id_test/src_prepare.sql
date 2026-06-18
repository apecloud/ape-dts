use mongo_struct_shardkey_id_db;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_struct_shardkey_id_db" });
db.createCollection("by_object_id");
db.createCollection("by_string_id");
admin.runCommand({ "shardCollection": "mongo_struct_shardkey_id_db.by_object_id", "key": { "_id": "hashed" } });
admin.runCommand({ "shardCollection": "mongo_struct_shardkey_id_db.by_string_id", "key": { "_id": 1 } });
