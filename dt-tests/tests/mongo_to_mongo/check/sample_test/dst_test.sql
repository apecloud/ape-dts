use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "10", "_id": ObjectId("65733a82fb2ce9836745de41") });
db.tb_2.insertOne({ "name": "a", "age": "10", "_id": ObjectId("65733a82fb2ce9836745de51") });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "10", "_id": ObjectId("65733a82fb2ce9836745de61") });
