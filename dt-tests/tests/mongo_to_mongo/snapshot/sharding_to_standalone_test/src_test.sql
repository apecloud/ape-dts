use mongo_snapshot_sharding_to_standalone_db;

db.accounts.insertMany([
  { "_id": "acct_s_1", "tenant_id": "tenant_a", "account_id": 1, "status": "active", "profile": { "score": 11, "level": "gold" }, "tags": ["vip", "east"] },
  { "_id": "acct_s_2", "tenant_id": "tenant_a", "account_id": 2, "status": "frozen", "profile": { "score": 22, "level": "silver" }, "tags": ["risk"] },
  { "_id": "acct_s_3", "tenant_id": "tenant_b", "account_id": 1, "status": "active", "profile": { "score": 33, "level": "bronze" }, "tags": [] }
]);

db.events_hashed.insertMany([
  { "_id": "event_s_1", "region": "east", "kind": "login", "payload": { "ip": "10.1.0.1" } },
  { "_id": "event_s_2", "region": "west", "kind": "logout", "payload": { "ip": "10.1.0.2" } },
  { "_id": "event_s_3", "region": "north", "kind": "pay", "payload": { "amount": 23.5 } }
]);

db.upsert_accounts.insertMany([
  { "_id": "upsert_s_1", "tenant_id": "tenant_s", "account_id": 1, "status": "new", "balance": 111, "profile": { "score": 11 } },
  { "_id": "upsert_s_2", "tenant_id": "tenant_s", "account_id": 2, "status": "new", "balance": 222, "profile": { "score": 22 } }
]);
