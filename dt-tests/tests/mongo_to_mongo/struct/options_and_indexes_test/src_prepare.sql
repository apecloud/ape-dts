use mongo_struct_options_db;
db.dropDatabase();
db.createCollection("validated_accounts", { "validator": { "$jsonSchema": { "bsonType": "object", "required": ["tenant_id", "email"], "properties": { "tenant_id": { "bsonType": "string" }, "email": { "bsonType": "string" }, "age": { "bsonType": "int" } } } }, "validationLevel": "moderate", "validationAction": "warn", "collation": { "locale": "en", "strength": 2 } });
db.validated_accounts.createIndex({ "tenant_id": 1, "email": 1 }, { "name": "tenant_email_unique_idx", "unique": true });
db.validated_accounts.createIndex({ "age": -1 }, { "name": "age_partial_idx", "partialFilterExpression": { "age": { "$gte": 18 } } });
db.validated_accounts.createIndex({ "email": 1 }, { "name": "email_sparse_idx", "sparse": true });
db.createCollection("ttl_events");
db.ttl_events.createIndex({ "expire_at": 1 }, { "name": "expire_at_ttl_idx", "expireAfterSeconds": 3600 });
