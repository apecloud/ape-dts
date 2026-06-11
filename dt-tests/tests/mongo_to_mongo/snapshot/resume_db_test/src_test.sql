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

db.resume_string_tb_1.insertOne({ "_id": "a", "name" : "string_1" });
db.resume_string_tb_1.insertOne({ "_id": "b", "name" : "string_2" });
db.resume_string_tb_1.insertOne({ "_id": "c", "name" : "string_3" });

db.resume_int32_tb_1.insertOne({ "_id": { "$numberInt": "1" }, "name" : "int32_1" });
db.resume_int32_tb_1.insertOne({ "_id": { "$numberInt": "2" }, "name" : "int32_2" });
db.resume_int32_tb_1.insertOne({ "_id": { "$numberInt": "3" }, "name" : "int32_3" });

db.resume_int64_in_log_tb_1.insertOne({ "_id": { "$numberLong": "1" }, "name" : "int64_1" });
db.resume_int64_in_log_tb_1.insertOne({ "_id": { "$numberLong": "2" }, "name" : "int64_2" });
db.resume_int64_in_log_tb_1.insertOne({ "_id": { "$numberLong": "3" }, "name" : "int64_3" });

db.resume_datetime_in_log_tb_1.insertOne({ "_id": { "$date": "2024-01-01T00:00:00Z" }, "name" : "datetime_1" });
db.resume_datetime_in_log_tb_1.insertOne({ "_id": { "$date": "2024-01-02T00:00:00Z" }, "name" : "datetime_2" });
db.resume_datetime_in_log_tb_1.insertOne({ "_id": { "$date": "2024-01-03T00:00:00Z" }, "name" : "datetime_3" });

db.resume_binary_tb_1.insertOne({ "_id": { "$binary": { "base64": "AQ==", "subType": "00" } }, "name" : "binary_1" });
db.resume_binary_tb_1.insertOne({ "_id": { "$binary": { "base64": "Ag==", "subType": "00" } }, "name" : "binary_2" });
db.resume_binary_tb_1.insertOne({ "_id": { "$binary": { "base64": "Aw==", "subType": "00" } }, "name" : "binary_3" });

db.resume_decimal_tb_1.insertOne({ "_id": { "$numberDecimal": "1.1" }, "name" : "decimal_1" });
db.resume_decimal_tb_1.insertOne({ "_id": { "$numberDecimal": "2.2" }, "name" : "decimal_2" });
db.resume_decimal_tb_1.insertOne({ "_id": { "$numberDecimal": "3.3" }, "name" : "decimal_3" });

db.resume_document_tb_1.insertOne({ "_id": { "k": 1 }, "name" : "document_1" });
db.resume_document_tb_1.insertOne({ "_id": { "k": 2 }, "name" : "document_2" });
db.resume_document_tb_1.insertOne({ "_id": { "k": 3 }, "name" : "document_3" });

db.resume_minmax_in_log_tb_1.insertOne({ "_id": { "$minKey": 1 }, "name" : "min_key" });
db.resume_minmax_in_log_tb_1.insertOne({ "_id": ObjectId("648195af9aa9cadd41a9dcc2"), "name" : "middle_object_id" });
db.resume_minmax_in_log_tb_1.insertOne({ "_id": { "$maxKey": 1 }, "name" : "max_key" });
