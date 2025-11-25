use dst_test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" , "_id": ObjectId("65733a82fb2ce9836745de4a") });
db.tb_1.insertOne({ "name": "b", "age": "2000" , "_id": ObjectId("65733a82fb2ce9836745de4b") });
db.tb_1.insertOne({ "name": "c", "age": "3000" , "_id": ObjectId("65733a82fb2ce9836745de4c") });

db.tb_2.insertOne({ "name": "c", "age": "3" , "_id": ObjectId("65733a82fb2ce9836745de41") });
db.tb_2.insertOne({ "name": "d", "age": "4000" , "_id": ObjectId("65733a82fb2ce9836745de42") });
db.tb_2.insertOne({ "name": "e", "age": "5000" , "_id": ObjectId("65733a82fb2ce9836745de43") });

use dst_test_db_2

db.dst_tb_1.insertOne({ "name": "a", "age": "1000", "_id": ObjectId("65733a82fb2ce9836745de44") }); 
db.dst_tb_1.insertOne({ "name": "b", "age": "2", "_id": ObjectId("65733a82fb2ce9836745de45") });
db.dst_tb_1.insertOne({ "name": "b", "age": "4", "_id": ObjectId("65733a82fb2ce9836745de47") }); 
db.dst_tb_1.insertOne({ "name": "b", "age": "5000", "_id": ObjectId("65733a82fb2ce9836745de48") }); 

use test_db_2
db.tb_2.insertOne({ "name": "a", "age": "3", "_id": ObjectId("65733a82fb2ce9836745de4b") });
db.tb_2.insertOne({ "name": "b", "age": "4000", "_id": ObjectId("65733a82fb2ce9836745de4c") }); 
db.tb_2.insertOne({ "name": "b", "age": "5000", "_id": ObjectId("65733a82fb2ce9836745de4d") }); 
