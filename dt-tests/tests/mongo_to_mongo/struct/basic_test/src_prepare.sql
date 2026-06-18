use mongo_struct_basic_db;
db.dropDatabase();
db.createCollection("accounts", { "capped": true, "size": 4096 });
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
