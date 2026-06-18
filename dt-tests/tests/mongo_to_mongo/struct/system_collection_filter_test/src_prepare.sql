use mongo_struct_system_filter_db;
db.dropDatabase();
db.createCollection("normal_accounts");
db.createCollection("systematic_logs");
db.runCommand({ "create": "v_normal", "viewOn": "normal_accounts", "pipeline": [] });
