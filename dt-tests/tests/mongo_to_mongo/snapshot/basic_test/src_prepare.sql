use test_db_1

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");
db.createCollection("tb_id_types");
db.createCollection("tb_batch_read");

use test_db_2

db.dropDatabase();

db.createCollection("tb_1");
db.createCollection("tb_2");
