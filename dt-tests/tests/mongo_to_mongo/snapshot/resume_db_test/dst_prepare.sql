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

use apecloud_resumer_test

db.dropDatabase();

db.createCollection("ape_task_position");

db.ape_task_position.insertMany([
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotFinished",
    "position_key": "test_db_1-finish_tb_1",
    "position_data": "{\"type\":\"RdbSnapshotFinished\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"finish_tb_1\"}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotFinished",
    "position_key": "test_db_1-finish_tb_in_log_1",
    "position_data": "{\"type\":\"RdbSnapshotFinished\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"finish_tb_in_log_1\"}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"ObjectId\\\":\\\"648195af9aa9cadd41a9dcb2\\\"}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_tb_in_log_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_tb_in_log_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"ObjectId\\\":\\\"648195af9aa9cadd41a9dcb2\\\"}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_string_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_string_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"String\\\":\\\"b\\\"}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_int32_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_int32_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"Int32\\\":2}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_int64_in_log_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_int64_in_log_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"Int64\\\":2}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_datetime_in_log_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_datetime_in_log_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"CanonicalExtJson\\\":{\\\"$date\\\":{\\\"$numberLong\\\":\\\"1704153600000\\\"}}}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_binary_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_binary_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"CanonicalExtJson\\\":{\\\"$binary\\\":{\\\"base64\\\":\\\"Ag==\\\",\\\"subType\\\":\\\"00\\\"}}}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_decimal_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_decimal_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"CanonicalExtJson\\\":{\\\"$numberDecimal\\\":\\\"2.2\\\"}}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_document_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_document_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"CanonicalExtJson\\\":{\\\"k\\\":{\\\"$numberInt\\\":\\\"2\\\"}}}\"]}}"
  },
  {
    "task_id": "resume_db_test_1",
    "resumer_type": "SnapshotDoing",
    "position_key": "test_db_1-resume_minmax_in_log_tb_1",
    "position_data": "{\"type\":\"RdbSnapshot\",\"db_type\":\"mongo\",\"schema\":\"test_db_1\",\"tb\":\"resume_minmax_in_log_tb_1\",\"order_key\":{\"single\":[\"_id\",\"{\\\"ObjectId\\\":\\\"648195af9aa9cadd41a9dcc2\\\"}\"]}}"
  }
]);
