use test_db_1

-- Normal ObjectId _id (should be checked)
db.tb_1.insertOne({ "name": "normal_1", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4a") });
db.tb_1.insertOne({ "name": "normal_2", "age": "2" , "_id": ObjectId("65733a82fb2ce9836745de4b") });

-- Document _id (should be skipped with error log)
db.tb_1.insertOne({ "name": "doc_id", "age": "3" , "_id": { "user_id": 123, "order_id": 456 } });

-- String _id (should be checked)
db.tb_1.insertOne({ "name": "string_id", "age": "4" , "_id": "custom_string_id" });

-- Int64 _id (should be checked)
db.tb_1.insertOne({ "name": "int_id", "age": "5" , "_id": NumberLong(9999999) });
