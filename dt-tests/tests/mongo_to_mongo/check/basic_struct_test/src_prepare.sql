use mongo_struct_check_src;
db.dropDatabase();
db.createCollection("orders", {"validator":{"amount":{"$gte":0}}});
db.orders.createIndex({"a":1,"b":1}, {"name":"ordered"});
db.orders.createIndex({"amount":1}, {"name":"missing_index"});
db.createCollection("missing_empty");
db.createCollection("matching");
