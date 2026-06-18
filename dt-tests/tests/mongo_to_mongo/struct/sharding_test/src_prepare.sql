use mongo_struct_sharding_db;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_struct_sharding_db" });
db.createCollection("accounts", { "capped": false });
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
admin.runCommand({ "shardCollection": "mongo_struct_sharding_db.accounts", "key": { "tenant_id": 1, "account_id": 1 } });
