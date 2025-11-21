use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4a") });
db.tb_1.insertOne({ "name": "b", "age": "2" , "_id": ObjectId("65733a82fb2ce9836745de4b") });
db.tb_1.insertOne({ "name": "c", "age": "3" , "_id": ObjectId("65733a82fb2ce9836745de4c") });
db.tb_1.insertOne({ "name": "d", "age": "4" , "_id": ObjectId("65733a82fb2ce9836745de4d") });
db.tb_1.insertOne({ "name": "e", "age": "5" , "_id": ObjectId("65733a82fb2ce9836745de4e") });

db.tb_2.insertOne({ "name": "a", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4f") });
db.tb_2.insertOne({ "name": "b", "age": "2" , "_id": ObjectId("65733a82fb2ce9836745de40") });
db.tb_2.insertOne({ "name": "c", "age": "3" , "_id": ObjectId("65733a82fb2ce9836745de41") });
db.tb_2.insertOne({ "name": "d", "age": "4" , "_id": ObjectId("65733a82fb2ce9836745de42") });
db.tb_2.insertOne({ "name": "e", "age": "5" , "_id": ObjectId("65733a82fb2ce9836745de43") });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "1", "_id": ObjectId("65733a82fb2ce9836745de44") }); 
db.tb_1.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de45") });
db.tb_1.insertOne({ "name": "a", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de46") });
db.tb_1.insertOne({ "name": "b", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de47") }); 
db.tb_1.insertOne({ "name": "b", "age": "5", "_id": ObjectId("65733a82fb2ce9836745de48") }); 

db.tb_2.insertOne({ "name": "a", "age": "1", "_id": ObjectId("65733a82fb2ce9836745de49") }); 
db.tb_2.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de4a") });
db.tb_2.insertOne({ "name": "a", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de4b") });
db.tb_2.insertOne({ "name": "b", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de4c") }); 
db.tb_2.insertOne({ "name": "b", "age": "5", "_id": ObjectId("65733a82fb2ce9836745de4d") }); 