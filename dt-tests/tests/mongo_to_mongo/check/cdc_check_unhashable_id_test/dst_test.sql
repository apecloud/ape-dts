use test_db_1

-- Pre-insert some data, CDC will upsert and overwrite
-- Normal ObjectId _id
db.tb_1.insertOne({ "name": "normal_1", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4a") });

-- Document _id (will be skipped during check)
db.tb_1.insertOne({ "name": "doc_id", "age": "3" , "_id": { "user_id": 123, "order_id": 456 } });

