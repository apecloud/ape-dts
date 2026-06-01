use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1", "_id": ObjectId("65733a82fb2ce9836745de41") });
db.tb_1.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de42") });
db.tb_1.insertOne({ "name": "c", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de43") });
db.tb_1.insertOne({ "name": "d", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de44") });

db.tb_2.insertOne({ "name": "a", "age": "1", "_id": ObjectId("65733a82fb2ce9836745de51") });
db.tb_2.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de52") });
db.tb_2.insertOne({ "name": "c", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de53") });
db.tb_2.insertOne({ "name": "d", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de54") });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "1", "_id": ObjectId("65733a82fb2ce9836745de61") });
db.tb_1.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de62") });
db.tb_1.insertOne({ "name": "c", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de63") });
db.tb_1.insertOne({ "name": "d", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de64") });
