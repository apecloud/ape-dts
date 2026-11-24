use mongo_snapshot_sharding_db;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_snapshot_sharding_db", "primaryShard": "source-shard-rs" });
db.createCollection("accounts");
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
admin.runCommand({ "shardCollection": "mongo_snapshot_sharding_db.accounts", "key": { "tenant_id": 1, "account_id": 1 } });
db.createCollection("events_hashed");
db.events_hashed.createIndex({ "region": "hashed" }, { "name": "region_hashed_idx" });
admin.runCommand({ "shardCollection": "mongo_snapshot_sharding_db.events_hashed", "key": { "region": "hashed" } });
db.createCollection("upsert_accounts");
db.upsert_accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_upsert_idx" });
admin.runCommand({ "shardCollection": "mongo_snapshot_sharding_db.upsert_accounts", "key": { "tenant_id": 1, "account_id": 1 } });

db.createCollection("duplicate_ids");
db.duplicate_ids.createIndex({ "tenant": 1 });
admin.runCommand({ "shardCollection": "mongo_snapshot_sharding_db.duplicate_ids", "key": { "tenant": 1 } });
admin.runCommand({ "split": "mongo_snapshot_sharding_db.duplicate_ids", "middle": { "tenant": "AA" } });
admin.runCommand({ "split": "mongo_snapshot_sharding_db.duplicate_ids", "middle": { "tenant": "B" } });
admin.runCommand({ "moveChunk": "mongo_snapshot_sharding_db.duplicate_ids", "find": { "tenant": "B" }, "to": "source-shard2-rs" });
