use mongo_tls_sharding_snapshot;

db.accounts.insertMany([
  { "_id": "acct_1", "tenant_id": "tenant_a", "account_id": 1, "status": "active" },
  { "_id": "acct_2", "tenant_id": "tenant_a", "account_id": 2, "status": "frozen" },
  { "_id": "acct_3", "tenant_id": "tenant_b", "account_id": 1, "status": "active" }
]);
