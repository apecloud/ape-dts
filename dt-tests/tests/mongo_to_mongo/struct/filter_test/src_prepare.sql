use mongo_struct_filter_db;
db.dropDatabase();
db.createCollection("keep_accounts");
db.keep_accounts.createIndex({ "tenant_id": 1 }, { "name": "tenant_idx" });
db.createCollection("keep_ignored");
db.createCollection("drop_accounts");
