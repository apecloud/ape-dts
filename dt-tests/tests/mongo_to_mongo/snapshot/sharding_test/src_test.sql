use mongo_snapshot_sharding_db;

db.accounts.insertMany([
  { "_id": "acct_1", "tenant_id": "tenant_a", "account_id": 1, "status": "active", "profile": { "score": 10, "level": "gold" }, "tags": ["vip", "east"] },
  { "_id": "acct_2", "tenant_id": "tenant_a", "account_id": 2, "status": "frozen", "profile": { "score": 20, "level": "silver" }, "tags": ["risk"] },
  { "_id": "acct_3", "tenant_id": "tenant_b", "account_id": 1, "status": "active", "profile": { "score": 30, "level": "bronze" }, "tags": [] }
]);

db.events_hashed.insertMany([
  { "_id": "event_1", "region": "east", "kind": "login", "payload": { "ip": "10.0.0.1" } },
  { "_id": "event_2", "region": "west", "kind": "logout", "payload": { "ip": "10.0.0.2" } },
  { "_id": "event_3", "region": "north", "kind": "pay", "payload": { "amount": 12.5 } }
]);

db.upsert_accounts.insertMany([
  { "_id": "upsert_1", "tenant_id": "tenant_u", "account_id": 1, "status": "new", "balance": 101, "profile": { "score": 1 } },
  { "_id": "upsert_2", "tenant_id": "tenant_u", "account_id": 2, "status": "new", "balance": 202, "profile": { "score": 2 } }
]);
