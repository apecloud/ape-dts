use test_db_1

db.finish_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb1"), "name" : "a", "age" : "1" });

db.resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb1"), "name" : "a", "age" : "1" });
db.resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb2"), "name" : "a", "age" : "1" });
db.resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb3"), "name" : "a", "age" : "1" });

db.non_resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb1"), "name" : "a", "age" : "1" });
db.non_resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb2"), "name" : "a", "age" : "1" });
db.non_resume_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb3"), "name" : "a", "age" : "1" });

db.finish_tb_in_log_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb1"), "name" : "a", "age" : "1" });

db.resume_tb_in_log_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb1"), "name" : "a", "age" : "1" });
db.resume_tb_in_log_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb2"), "name" : "a", "age" : "1" });
db.resume_tb_in_log_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcb3"), "name" : "a", "age" : "1" });
