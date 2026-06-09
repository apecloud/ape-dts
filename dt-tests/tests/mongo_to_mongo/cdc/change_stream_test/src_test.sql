use test_db_1

-- insert
db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });
db.tb_1.insertOne({ "name": "c", "age": "3" });
db.tb_1.insertOne({ "name": "d", "age": "4" });
db.tb_1.insertOne({ "name": "e", "age": "5" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });
db.tb_2.insertOne({ "name": "c", "age": "3" });
db.tb_2.insertOne({ "name": "d", "age": "4" });
db.tb_2.insertOne({ "name": "e", "age": "5" });

db.id_types_tb.insertOne({ "_id": { "$oid": "648195af9aa9cadd41a9dca1" }, "kind": "object_id", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": "string_id", "kind": "string", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$numberInt": "32" }, "kind": "int32", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$numberLong": "64" }, "kind": "int64", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$numberDouble": "3.5" }, "kind": "double", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$numberDecimal": "12.34" }, "kind": "decimal", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": false, "kind": "bool", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": null, "kind": "null", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$date": "2024-01-02T00:00:00Z" }, "kind": "datetime", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$timestamp": { "t": 1700000001, "i": 1 } }, "kind": "timestamp", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$binary": { "base64": "AQID", "subType": "00" } }, "kind": "binary", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "k": "w" }, "kind": "document", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$code": "return 2" }, "kind": "javascript_code", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$symbol": "sym_2" }, "kind": "symbol", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$minKey": 1 }, "kind": "min_key", "status": "inserted" });
db.id_types_tb.insertOne({ "_id": { "$maxKey": 1 }, "kind": "max_key", "status": "inserted" });

-- set, u
db.tb_1.updateOne({ "age" : "4" }, { "$set": { "name" : "d_1" } });
db.tb_1.updateOne({ "age" : "5" }, { "$set": { "name" : "e_1" } });

db.tb_2.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_2.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.id_types_tb.updateOne({ "_id": ObjectId("648195af9aa9cadd41a9dca1") }, { "$set": { "status": "updated_object_id" } });
db.id_types_tb.updateOne({ "_id": ObjectId("648195af9aa9cadd41a9dca1") }, { "$set": { "status": "updated_object_id_again", "version": 2 } });
db.id_types_tb.updateOne({ "_id": "string_id" }, { "$set": { "status": "updated_string" } });
db.id_types_tb.updateOne({ "_id": "string_id" }, { "$set": { "status": "updated_string_again", "version": 2 } });
db.id_types_tb.updateOne({ "_id": 32 }, { "$set": { "status": "updated_int32" } });
db.id_types_tb.updateOne({ "_id": { "k": "w" } }, { "$set": { "status": "updated_document" } });
db.id_types_tb.updateOne({ "_id": null }, { "$set": { "status": "updated_null" } });

-- set, i
db.tb_1.updateOne({ "age" : "4" }, { "$set": { "salary" : 100 } });
db.tb_1.updateOne({ "age" : "5" }, { "$set": { "salary" : 100 } });

db.tb_2.updateOne({ "age" : "1" }, { "$set": { "salary" : 100 } });
db.tb_2.updateOne({ "age" : "2" }, { "$set": { "salary" : 100 } });

-- unset, d
db.tb_1.updateOne({ "age" : "4" }, { "$unset": { "salary" : "" } });
db.tb_1.updateOne({ "age" : "5" }, { "$unset": { "salary" : "" } });

-- inc, u
db.tb_2.updateOne({ "age" : "1" }, { "$inc": { "salary" : 100 } });

-- delete
db.tb_1.deleteOne({ "name": "a", "age": "1" });
db.tb_1.deleteOne({ "name": "b", "age": "2" });

db.tb_2.deleteOne({ "name": "d", "age": "4" });
db.tb_2.deleteOne({ "name": "e", "age": "5" });

db.id_types_tb.deleteOne({ "_id": false });
db.id_types_tb.deleteOne({ "_id": { "$numberLong": "64" } });
db.id_types_tb.deleteOne({ "_id": { "$binary": { "base64": "AQID", "subType": "00" } } });
db.id_types_tb.deleteOne({ "_id": { "$maxKey": 1 } });

use test_db_2

-- insert records with custom defined _id and object_id
db.tb_1.insertMany([{ "name": "a", "age": "1", "_id": "1" }, { "name": "b", "age": "1", "_id": "2" }, { "name": "c", "age": "1" }]);

db.tb_1.updateMany({ "age": "1" },  { "$set": { "age" : "1000" } });

db.tb_1.deleteMany({ "age": "1000" });
