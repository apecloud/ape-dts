use test_db_1

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

use test_db_2

-- insert records with custom defined _id and object_id
db.tb_1.insertMany([{ "name": "a", "age": "1", "_id": "1" }, { "name": "b", "age": "1", "_id": "2" }, { "name": "c", "age": "1" }]);

db.tb_id_types.insertMany([
  { "_id": { "$oid": "65733a82fb2ce9836745de51" }, "name": "object_id" },
  { "_id": "1", "name": "string" },
  { "_id": { "$numberInt": "1" }, "name": "int32" },
  { "_id": { "$numberLong": "2" }, "name": "int64" },
  { "_id": { "$numberDouble": "3.5" }, "name": "double" },
  { "_id": { "$numberDecimal": "4.5" }, "name": "decimal128" },
  { "_id": false, "name": "bool" },
  { "_id": null, "name": "null" },
  { "_id": { "$date": "2024-02-03T04:05:06Z" }, "name": "datetime" },
  { "_id": { "$timestamp": { "t": 1700000001, "i": 1 } }, "name": "timestamp" },
  { "_id": { "$binary": { "base64": "BQYHCA==", "subType": "00" } }, "name": "binary" },
  { "_id": { "k": "w" }, "name": "document" },
  { "_id": { "$code": "function() { return 2; }" }, "name": "javascript_code" },
  { "_id": { "$symbol": "sym_2" }, "name": "symbol" },
  { "_id": { "$minKey": 1 }, "name": "min_key" },
  { "_id": { "$maxKey": 1 }, "name": "max_key" }
]);