use sharding_cdc_db

admin.runCommand({ "enableSharding": "sharding_cdc_db" });

db.createCollection("accounts");
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1 });
db.accounts.createIndex({ "tenant_id": 1, "account_id": 1, "region": 1 });
admin.runCommand({ "shardCollection": "sharding_cdc_db.accounts", "key": { "tenant_id": 1, "account_id": 1 } });
db.runCommand({ "collMod": "accounts", "changeStreamPreAndPostImages": { "enabled": true } });

db.accounts.insertOne({ "_id": "acct_1", "tenant_id": "tenant_a", "account_id": 1, "region": "east", "status": "created", "profile": { "tier": "gold", "score": 1 }, "attrs": ["seed"], "history": [{ "step": 1, "state": "created" }] });
db.accounts.insertOne({ "_id": "acct_2", "tenant_id": "tenant_a", "account_id": 2, "region": "west", "status": "created", "profile": { "tier": "silver", "score": 2 }, "attrs": ["seed"] });
db.accounts.insertOne({ "_id": "acct_delete", "tenant_id": "tenant_a", "account_id": 3, "region": "north", "status": "delete_me" });

db.accounts.updateOne({ "tenant_id": "tenant_a", "account_id": 1 }, { "$set": { "status": "updated", "profile.score": 10, "profile.flags": ["normal", "update"], "history.0.state": "updated_seed" }, "$push": { "attrs": "normal_update" } });
db.accounts.updateOne({ "tenant_id": "tenant_a", "account_id": 1 }, { "$push": { "history": { "step": 2, "state": "updated" } } });
db.accounts.updateOne({ "tenant_id": "tenant_a", "account_id": 2 }, { "$set": { "tenant_id": "tenant_b", "status": "moved_by_shard_key_update", "profile.score": 20 } });
db.accounts.deleteOne({ "tenant_id": "tenant_a", "account_id": 3 });

admin.runCommand({ "refineCollectionShardKey": "sharding_cdc_db.accounts", "key": { "tenant_id": 1, "account_id": 1, "region": 1 } });
db.accounts.updateOne({ "tenant_id": "tenant_b", "account_id": 2, "region": "west" }, { "$set": { "status": "updated_after_refine", "profile.refined": true } });

db.createCollection("events_hashed");
db.events_hashed.createIndex({ "region": "hashed" });
admin.runCommand({ "shardCollection": "sharding_cdc_db.events_hashed", "key": { "region": "hashed" } });
db.events_hashed.insertOne({ "_id": "event_1", "region": "east", "kind": "inserted", "payload": { "items": [1, 2, 3] } });
db.events_hashed.updateOne({ "_id": "event_1" }, { "$set": { "kind": "updated", "payload.items.1": 20, "payload.extra": { "ok": true } } });

db.createCollection("plain_ops");
db.plain_ops.insertOne({ "_id": "plain_1", "kind": "inserted", "arr": [1, 2] });
db.plain_ops.updateOne({ "_id": "plain_1" }, { "$set": { "kind": "updated", "nested": { "level": 1 } }, "$push": { "arr": 3 } });
db.plain_ops.insertOne({ "_id": "plain_delete", "kind": "delete_me" });
db.plain_ops.deleteOne({ "_id": "plain_delete" });

db.createCollection("rename_src");
db.rename_src.insertOne({ "_id": "rename_doc", "state": "before_rename" });
db.rename_src.renameCollection("rename_dst");
db.rename_dst.updateOne({ "_id": "rename_doc" }, { "$set": { "state": "after_rename" } });

db.createCollection("drop_me");
db.drop_me.insertOne({ "_id": "drop_doc", "state": "before_drop" });
db.drop_me.drop();
