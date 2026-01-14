use sample_db
db.dropDatabase();
db.createCollection("sample_tb");
db.sample_tb.insertOne({ "_id": 1, "name": "Alice" });
db.sample_tb.insertOne({ "_id": 2, "name": "Bob" });
