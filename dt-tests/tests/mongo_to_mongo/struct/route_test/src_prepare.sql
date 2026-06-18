use mongo_struct_route_db;
db.dropDatabase();
db.createCollection("accounts", { "capped": false });
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 }, { "name": "tenant_account_idx" });
