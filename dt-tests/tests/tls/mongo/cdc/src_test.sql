use mongo_tls_cdc

db.items.insertOne({ "_id": 1, "name": "first", "status": "new" });
db.items.insertOne({ "_id": 2, "name": "second", "status": "new" });
db.items.updateOne({ "_id": 1 }, { "$set": { "status": "updated" } });
db.items.deleteOne({ "_id": 2 });
