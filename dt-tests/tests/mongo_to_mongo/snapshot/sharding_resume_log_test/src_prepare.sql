use mongo_snapshot_resume_src;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_snapshot_resume_src", "primaryShard": "source-shard-rs" });
db.createCollection("duplicate_ids");
db.duplicate_ids.createIndex({ "tenant": 1, "region": 1 });
admin.runCommand({ "shardCollection": "mongo_snapshot_resume_src.duplicate_ids", "key": { "tenant": 1, "region": 1 } });
admin.runCommand({ "split": "mongo_snapshot_resume_src.duplicate_ids", "middle": { "tenant": "A", "region": "west" } });
admin.runCommand({ "moveChunk": "mongo_snapshot_resume_src.duplicate_ids", "find": { "tenant": "A", "region": "west" }, "to": "source-shard2-rs" });
