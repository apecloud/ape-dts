use test_db_1

db.dropDatabase();

db.createCollection("finish_tb_1");
db.createCollection("resume_tb_1");
db.createCollection("non_resume_tb_1");
db.createCollection("finish_tb_in_log_1");
db.createCollection("resume_tb_in_log_1");
