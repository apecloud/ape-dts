use mongo_struct_advanced_db;
db.dropDatabase();
db.createCollection("advanced_accounts", { "validator": { "$jsonSchema": { "bsonType": "object", "required": ["tenant_id", "email"], "properties": { "tenant_id": { "bsonType": "string" }, "email": { "bsonType": "string" }, "location": { "bsonType": "object" } } } }, "validationLevel": "strict", "validationAction": "error" });
db.advanced_accounts.createIndex({ "tenant_id": "hashed" }, { "name": "tenant_hashed_idx" });
db.advanced_accounts.createIndex({ "bio": "text", "notes": "text" }, { "name": "profile_text_idx", "default_language": "english", "weights": { "bio": 10, "notes": 2 } });
db.advanced_accounts.createIndex({ "$**": 1 }, { "name": "attributes_wildcard_idx", "wildcardProjection": { "attributes.secret": 0 } });
db.advanced_accounts.createIndex({ "location": "2dsphere" }, { "name": "location_2dsphere_idx" });
db.advanced_accounts.createIndex({ "archived_at": 1 }, { "name": "archived_hidden_idx", "hidden": true });
db.advanced_accounts.createIndex({ "last_name": 1 }, { "name": "last_name_fr_idx", "collation": { "locale": "fr", "strength": 1 } });
