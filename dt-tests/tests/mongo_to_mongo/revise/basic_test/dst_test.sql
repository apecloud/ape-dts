use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" , "_id": "65733a82fb2ce9836745de4a" });
db.tb_1.insertOne({ "name": "b", "age": "2000" , "_id": "65733a82fb2ce9836745de4b" });
db.tb_1.insertOne({ "name": "c", "age": "3000" , "_id": "65733a82fb2ce9836745de4c" });

db.tb_2.insertOne({ "name": "c", "age": "3" , "_id": "65733a82fb2ce9836745de4h" });
db.tb_2.insertOne({ "name": "d", "age": "4000" , "_id": "65733a82fb2ce9836745de4i" });
db.tb_2.insertOne({ "name": "e", "age": "5000" , "_id": "65733a82fb2ce9836745de4j" });

use test_db_2

db.tb_1.insertOne({ "name": "a", "age": "1000", "_id": "65733a82fb2ce9836745de4k" }); 
db.tb_1.insertOne({ "name": "b", "age": "2", "_id": "65733a82fb2ce9836745de4l" });
db.tb_1.insertOne({ "name": "b", "age": "4", "_id": "65733a82fb2ce9836745de4n" }); 
db.tb_1.insertOne({ "name": "b", "age": "5000", "_id": "65733a82fb2ce9836745de4o" }); 