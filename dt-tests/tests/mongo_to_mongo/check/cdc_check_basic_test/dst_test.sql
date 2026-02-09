use test_db_1

-- Pre-insert data, CDC will upsert and overwrite
db.tb_1.insertOne({ "name": "a", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4a") });
db.tb_1.insertOne({ "name": "b", "age": "2000" , "_id": ObjectId("65733a82fb2ce9836745de4b") });

-- DELETE: should be removed by CDC sync
db.tb_1.deleteOne({ "_id": ObjectId("65733a82fb2ce9836745de4b") });
