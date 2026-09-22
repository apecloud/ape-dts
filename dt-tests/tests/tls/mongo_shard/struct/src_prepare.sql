use mongo_tls_sharding_struct;
db.dropDatabase();
admin.runCommand({ "enableSharding": "mongo_tls_sharding_struct" });
db.createCollection("accounts");
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
admin.runCommand({ "shardCollection": "mongo_tls_sharding_struct.accounts", "key": { "tenant_id": 1, "account_id": 1 } });
