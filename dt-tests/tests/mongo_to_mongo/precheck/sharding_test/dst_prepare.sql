use precheck_sharding_db;
db.dropDatabase();
admin.runCommand({ "enableSharding": "precheck_sharding_db" });
db.createCollection("table1");
db.table1.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
admin.runCommand({ "shardCollection": "precheck_sharding_db.table1", "key": { "tenant_id": 1, "account_id": 1 } });
