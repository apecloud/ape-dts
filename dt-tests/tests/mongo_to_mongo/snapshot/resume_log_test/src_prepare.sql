use test_db_1

db.dropDatabase();

db.createCollection("finish_tb_1");
db.createCollection("resume_tb_1");
db.createCollection("non_resume_tb_1");
db.createCollection("finish_tb_in_log_1");
db.createCollection("resume_tb_in_log_1");
db.createCollection("resume_string_tb_1");
db.createCollection("resume_int32_tb_1");
db.createCollection("resume_int64_in_log_tb_1");
db.createCollection("resume_datetime_in_log_tb_1");
db.createCollection("resume_binary_tb_1");
db.createCollection("resume_decimal_tb_1");
db.createCollection("resume_document_tb_1");
db.createCollection("resume_minmax_in_log_tb_1");
