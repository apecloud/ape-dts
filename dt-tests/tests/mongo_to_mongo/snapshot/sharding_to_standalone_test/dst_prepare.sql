use mongo_snapshot_sharding_to_standalone_db;
db.dropDatabase();
db.createCollection("accounts");
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
db.createCollection("events_hashed");
db.events_hashed.createIndex({ "region": "hashed" }, { "name": "region_hashed_idx" });
db.createCollection("upsert_accounts");
db.upsert_accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_upsert_idx" });
db.upsert_accounts.insertMany([
  { "_id": "upsert_s_1", "tenant_id": "tenant_s", "account_id": 1, "status": "old", "balance": 1 },
  { "_id": "upsert_s_2", "tenant_id": "tenant_s", "account_id": 2, "status": "old", "balance": 2 }
]);
