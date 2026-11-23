use mongo_struct_check_dst;
db.dropDatabase();
db.createCollection("routed_orders", {"validator":{"amount":{"$gte":1}}});
db.routed_orders.createIndex({"b":1,"a":1}, {"name":"ordered"});
db.routed_orders.createIndex({"extra":1}, {"name":"extra_index"});
db.createCollection("matching", {"capped":false});
db.createCollection("target_only");
db.target_only.createIndex({"extra":1}, {"name":"ignored_index"});
