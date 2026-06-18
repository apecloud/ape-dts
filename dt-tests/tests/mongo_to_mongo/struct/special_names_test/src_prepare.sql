use mongo_struct_special_db;
db.dropDatabase();
db.runCommand({ "create": "orders-2026" });
db.runCommand({ "createIndexes": "orders-2026", "indexes": [{ "key": { "tenant.id": 1, "order-no": 1 }, "name": "tenant.id_order-no_idx" }] });
