DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

-- `id` int(11), can be extracted parallelly, with empty data
CREATE TABLE test_db_1.tb_0 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- `id` int(11), can be extracted parallelly 
CREATE TABLE test_db_1.tb_1 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 
CREATE TABLE test_db_1.tb_1_more (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- `id` varchar(255), can be extracted parallelly
CREATE TABLE test_db_1.tb_2 (`id` varchar(255) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- no primary key, can not be extracted parallelly
CREATE TABLE test_db_1.tb_3 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL); 
CREATE TABLE test_db_1.tb_3_null (`row_id` int, `id` int(11), `value` int(11) DEFAULT NULL); 

-- no unique key with multiple nulls, can be extracted parallelly
CREATE TABLE test_db_1.tb_4 (`row_id` int, `id` int(11), `value` int(11) DEFAULT NULL, UNIQUE KEY (`id`)); 

CREATE TABLE test_db_1.where_condition_1 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.where_condition_2 ( f_0 int, f_1 int, PRIMARY KEY (f_0) ); 

-- fallback to extracting all in one fetch serially.
CREATE TABLE test_db_1.tb_fallback_1 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL); 

-- fallback to extracting by batch size serially.
CREATE TABLE test_db_1.tb_fallback_2 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL, PRIMARY KEY (`id`)); 

-- fallback to extracting by unevenly sized chunks.
CREATE TABLE test_db_1.tb_fallback_3 (`id` int(11) NOT NULL, `value` int(11) DEFAULT NULL); 

DROP DATABASE IF EXISTS test_db_2;
CREATE DATABASE test_db_2;

-- 1. Standard Int (Base Case)
CREATE TABLE IF NOT EXISTS test_db_2.tb_4 (
  `row_id` int,
  `id` int(11),
  `value` int(11) DEFAULT NULL,
  UNIQUE KEY (`id`)
);

-- 2. BigInt Signed (Scenario: Snowflake ID / High Concurrency)
CREATE TABLE test_db_2.tb_bigint (
  `row_id` int,
  `id` bigint DEFAULT NULL,
  `value` varchar(50) DEFAULT NULL,
  UNIQUE KEY (`id`)
);

-- 3. Varchar Unique (Scenario: Usernames / Codes)
CREATE TABLE test_db_2.tb_varchar (
  `row_id` int,
  `id` varchar(64) DEFAULT NULL,
  `value` int DEFAULT NULL,
  UNIQUE KEY (`id`)
);

-- 4. Char/UUID (Scenario: Fixed length, Random distribution)
CREATE TABLE test_db_2.tb_char (
  `row_id` int,
  `id` char(36) DEFAULT NULL,
  `value` int DEFAULT NULL,
  UNIQUE KEY (`id`)
);

-- 5. DateTime (Scenario: Time-series / Logging)
CREATE TABLE test_db_2.tb_datetime (
  `row_id` int,
  `id` datetime(3) DEFAULT NULL, -- With millisecond precision
  `value` int DEFAULT NULL,
  UNIQUE KEY (`id`)
);

-- 6. Composite Key (Scenario: Multi-tenant / Organization + User)
CREATE TABLE test_db_2.tb_composite (
  `row_id` int,
  `org_id` int NOT NULL,
  `user_code` varchar(32) NOT NULL,
  `value` int DEFAULT NULL,
  UNIQUE KEY (`org_id`, `user_code`)
);

-- 7. TinyInt Signed (Scenario: Status codes / Small range -128 to 127)
CREATE TABLE test_db_2.tb_tinyint (
  `row_id` int,
  `id` tinyint(4) DEFAULT NULL,
  `value` varchar(20),
  UNIQUE KEY (`id`)
);

-- 8. BigInt Unsigned (Scenario: Positive only, larger upper bound)
CREATE TABLE test_db_2.tb_bigint_unsigned (
  `row_id` int,
  `id` bigint(20) UNSIGNED DEFAULT NULL,
  `value` varchar(20),
  UNIQUE KEY (`id`)
);

-- 9. Decimal & Float (Scenario: Financial / Precision sensitive)
CREATE TABLE test_db_2.tb_decimal (
  `row_id` int,
  `id` decimal(10,2) DEFAULT NULL,
  `value` varchar(20),
  UNIQUE KEY (`id`)
);

-- 10. Date (Scenario: Reporting days / Range partitioning)
CREATE TABLE test_db_2.tb_date (
  `row_id` int,
  `id` date DEFAULT NULL,
  `value` varchar(20),
  UNIQUE KEY (`id`)
);

-- 11. VarBinary (Scenario: Case-sensitive binary strings / Hex)
CREATE TABLE test_db_2.tb_varbinary (
  `row_id` int,
  `id` varbinary(64) DEFAULT NULL,
  `value` varchar(20),
  UNIQUE KEY (`id`)
);

-- 12. Varchar Primary Key (Scenario: Common string PK, UTF8MB4)
CREATE TABLE test_db_2.tb_varchar_pk (
  `row_id` int,
  `id` varchar(255) NOT NULL, 
  `value` int DEFAULT NULL,
  PRIMARY KEY (`id`)
);

-- 13. Text with Prefix Index (Scenario: Long content as Unique Key)
-- Must specify prefix length (64) for indexing BLOB/TEXT
CREATE TABLE test_db_2.tb_text_pk (
  `row_id` int,
  `id` TEXT, 
  `value` int DEFAULT NULL,
  UNIQUE KEY `idx_text_prefix` (`id`(64)) 
);

-- 14. Blob with Prefix Index (Scenario: Binary data as Unique Key)
CREATE TABLE test_db_2.tb_blob_pk (
  `row_id` int,
  `id` BLOB, 
  `value` int DEFAULT NULL,
  UNIQUE KEY `idx_blob_prefix` (`id`(64))
);
