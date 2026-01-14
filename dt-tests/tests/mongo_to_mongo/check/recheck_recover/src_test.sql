use sample_db
db.sample_tb.updateOne({ "_id": 1 }, { "$set": { "name": "Alice" } });
db.sample_tb.updateOne({ "_id": 2 }, { "$set": { "name": "Bob" } });
