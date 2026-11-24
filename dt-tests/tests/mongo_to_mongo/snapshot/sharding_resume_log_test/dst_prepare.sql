use mongo_snapshot_resume_dst;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_snapshot_resume_dst", "primaryShard": "target-shard-rs" });
db.createCollection("duplicate_ids");
db.duplicate_ids.createIndex({ "tenant": 1, "region": 1 });
admin.runCommand({ "shardCollection": "mongo_snapshot_resume_dst.duplicate_ids", "key": { "tenant": 1, "region": 1 } });
admin.runCommand({ "split": "mongo_snapshot_resume_dst.duplicate_ids", "middle": { "tenant": "A", "region": "west" } });
admin.runCommand({ "moveChunk": "mongo_snapshot_resume_dst.duplicate_ids", "find": { "tenant": "A", "region": "west" }, "to": "target-shard2-rs" });
db.duplicate_ids.insertOne({ "_id": 42, "tenant": "A", "region": "east", "checkpoint_marker": true });
