use mongo_tls_sharding_cdc;

admin.runCommand({ "enableSharding": "mongo_tls_sharding_cdc" });
db.createCollection("accounts");
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
admin.runCommand({ "shardCollection": "mongo_tls_sharding_cdc.accounts", "key": { "tenant_id": 1, "account_id": 1 } });
db.runCommand({ "collMod": "accounts", "changeStreamPreAndPostImages": { "enabled": true } });

db.accounts.insertOne({ "_id": "acct_1", "tenant_id": "tenant_a", "account_id": 1, "status": "created" });
db.accounts.insertOne({ "_id": "acct_2", "tenant_id": "tenant_a", "account_id": 2, "status": "delete_me" });
db.accounts.updateOne({ "tenant_id": "tenant_a", "account_id": 1 }, { "$set": { "status": "updated" } });
db.accounts.deleteOne({ "tenant_id": "tenant_a", "account_id": 2 });
