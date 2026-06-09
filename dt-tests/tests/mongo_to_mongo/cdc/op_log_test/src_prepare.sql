use test_db_1

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");
db.createCollection("id_types_tb");

use test_db_2

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");
