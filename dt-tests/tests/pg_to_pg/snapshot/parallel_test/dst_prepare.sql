DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;

-- id int, can be extracted parallelly, with empty data
CREATE TABLE test_db_1.tb_0 (id int NOT NULL, value int DEFAULT NULL, PRIMARY KEY (id)); 

-- id int, can be extracted parallelly 
CREATE TABLE test_db_1.tb_1 (id int NOT NULL, value int DEFAULT NULL, PRIMARY KEY (id)); 
CREATE TABLE test_db_1.tb_1_more (id int NOT NULL, value int DEFAULT NULL, PRIMARY KEY (id)); 

-- id varchar(255), can not be extracted parallelly
CREATE TABLE test_db_1.tb_2 (id varchar(255) NOT NULL, value int DEFAULT NULL, PRIMARY KEY (id)); 

-- no primary key, can be extracted parallelly
CREATE TABLE test_db_1.tb_3 (id int NOT NULL, value int DEFAULT NULL); 
CREATE TABLE test_db_1.tb_3_null (row_id int, id int, value int DEFAULT NULL); 

-- no unique key with multiple nulls, can be extracted parallelly
CREATE TABLE test_db_1.tb_4 (row_id int, id int, value int DEFAULT NULL, UNIQUE (id));

-- all null values
CREATE TABLE test_db_1.tb_all_null_1 (id varchar(255), value int DEFAULT NULL, UNIQUE (id)); 
CREATE TABLE test_db_1.tb_all_null_2 (id int, value int DEFAULT NULL, UNIQUE (id)); 

CREATE TABLE test_db_1.where_condition_1 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.where_condition_2 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 

-- fallback to extracting all in one fetch serially.
CREATE TABLE test_db_1.tb_fallback_1 (id int NOT NULL, value int DEFAULT NULL); 

-- fallback to extracting by batch size serially.
CREATE TABLE test_db_1.tb_fallback_2 (id int NOT NULL, value int DEFAULT NULL, PRIMARY KEY (id)); 

-- fallback to extracting by unevenly sized chunks.
CREATE TABLE test_db_1.tb_fallback_3 (id int NOT NULL, value int DEFAULT NULL); 

DROP SCHEMA IF EXISTS test_db_2 CASCADE;
CREATE SCHEMA test_db_2;

-- 1. Standard Int
CREATE TABLE IF NOT EXISTS test_db_2.tb_4 (
  row_id int,
  id int,
  value int DEFAULT NULL,
  UNIQUE (id)
);

-- 2. BigInt Signed
CREATE TABLE test_db_2.tb_bigint (
  row_id int,
  id bigint DEFAULT NULL,
  value varchar(50) DEFAULT NULL,
  UNIQUE (id)
);

-- 3. Varchar Unique
CREATE TABLE test_db_2.tb_varchar (
  row_id int,
  id varchar(64) DEFAULT NULL,
  value int DEFAULT NULL,
  UNIQUE (id)
);

-- 4. Char/UUID
CREATE TABLE test_db_2.tb_char (
  row_id int,
  id char(36) DEFAULT NULL,
  value int DEFAULT NULL,
  UNIQUE (id)
);

-- 5. DateTime (Timestamp)
CREATE TABLE test_db_2.tb_datetime (
  row_id int,
  id timestamp(3) DEFAULT NULL, -- Precision 3ms
  value int DEFAULT NULL,
  UNIQUE (id)
);

-- 6. Composite Key
CREATE TABLE test_db_2.tb_composite (
  row_id int,
  org_id int NOT NULL,
  user_code varchar(32) NOT NULL,
  value int DEFAULT NULL,
  UNIQUE (org_id, user_code)
);

-- 7. TinyInt (Mapped to SmallInt)
CREATE TABLE test_db_2.tb_tinyint (
  row_id int,
  id smallint DEFAULT NULL,
  value varchar(20),
  UNIQUE (id)
);

-- 8. BigInt Unsigned (Mapped to Numeric)
CREATE TABLE test_db_2.tb_bigint_unsigned (
  row_id int,
  id numeric(20,0) DEFAULT NULL, 
  value varchar(20),
  UNIQUE (id)
);

-- 9. Decimal & Float
CREATE TABLE test_db_2.tb_decimal (
  row_id int,
  id decimal(10,2) DEFAULT NULL,
  value varchar(20),
  UNIQUE (id)
);

-- 10. Date
CREATE TABLE test_db_2.tb_date (
  row_id int,
  id date DEFAULT NULL,
  value varchar(20),
  UNIQUE (id)
);

-- 11. VarBinary (Mapped to Bytea)
CREATE TABLE test_db_2.tb_varbinary (
  row_id int,
  id bytea DEFAULT NULL,
  value varchar(20),
  UNIQUE (id)
);

-- 12. Varchar Primary Key
CREATE TABLE test_db_2.tb_varchar_pk (
  row_id int,
  id varchar(255) NOT NULL, 
  value int DEFAULT NULL,
  PRIMARY KEY (id)
);

-- 13. Text with Prefix Index
CREATE TABLE test_db_2.tb_text_pk (
  row_id int,
  id TEXT, 
  value int DEFAULT NULL
);
-- Create unique index on prefix (first 64 chars)
CREATE UNIQUE INDEX idx_text_prefix ON test_db_2.tb_text_pk (substring(id, 1, 64));

-- 14. Blob with Prefix Index (Mapped to Bytea)
CREATE TABLE test_db_2.tb_blob_pk (
  row_id int,
  id BYTEA, 
  value int DEFAULT NULL
);
-- Create unique index on prefix (first 64 bytes)
CREATE UNIQUE INDEX idx_blob_prefix ON test_db_2.tb_blob_pk (substring(id, 1, 64));

-- 15. Float/Double (Scenario: Floating point as Unique Key)
CREATE TABLE test_db_2.tb_float (
  row_id int,
  id double precision DEFAULT NULL,
  value varchar(20),
  UNIQUE (id)
);