
INSERT INTO test_db_1.tb_1 VALUES (1,1);

-- add column
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_2 smallint DEFAULT NULL;
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_3 smallint DEFAULT NULL;

INSERT INTO test_db_1.tb_1 VALUES (2,2,2,2);

-- drop column
ALTER TABLE test_db_1.tb_1 DROP COLUMN f_2;

INSERT INTO test_db_1.tb_1 VALUES (3,3,3);

-- truncate table
TRUNCATE test_db_1.truncate_tb_1;
TRUNCATE TABLE test_db_1.truncate_tb_2;
-- truncate table in another database
TRUNCATE TABLE test_db_2.truncate_tb_1; 

-- rename table
ALTER TABLE test_db_1.rename_tb_1 RENAME test_db_1.dst_rename_tb_1;
RENAME TABLE test_db_1.rename_tb_2 TO test_db_1.dst_rename_tb_2, test_db_1.rename_tb_3 TO test_db_1.dst_rename_tb_3;

INSERT INTO test_db_1.dst_rename_tb_1 VALUES(1, 1);
INSERT INTO test_db_1.dst_rename_tb_2 VALUES(1, 1);
INSERT INTO test_db_1.dst_rename_tb_3 VALUES(1, 1);

-- drop table
DROP TABLE test_db_1.drop_tb_1;
DROP TABLE IF EXISTS test_db_1.drop_tb_2, test_db_1.drop_tb_3;

-- drop database 
DROP DATABASE test_db_3;
DROP DATABASE IF EXISTS test_db_3;

-- create database
CREATE DATABASE test_db_4;
CREATE DATABASE IF NOT EXISTS test_db_4;

-- create table
CREATE TABLE test_db_2.tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

INSERT INTO test_db_2.tb_1 VALUES (1,1,1);

-- add index
ALTER TABLE test_db_2.tb_1 ADD INDEX idx_f_1 (f_1);
CREATE INDEX idx_f_2 ON test_db_2.tb_1 (f_2);

-- drop index
ALTER TABLE test_db_2.tb_1 DROP INDEX idx_f_1;
DROP INDEX idx_f_2 ON test_db_2.tb_1;

-- create database with special character
CREATE DATABASE `中文database!@#$%^&*()_+`;

-- create table with chinese character
CREATE TABLE `中文database!@#$%^&*()_+`.`中文` ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

INSERT INTO `中文database!@#$%^&*()_+`.`中文` VALUES(1, 1, 1);

-- contain db system keywords
CREATE TABLE test_db_2.match (select_id INT PRIMARY KEY AUTO_INCREMENT, `table` VARCHAR(50) NOT NULL, `column` VARCHAR(100) NOT NULL, offset INT NOT NULL, unique_col VARCHAR(20), `match` INT, check_col INT, constraint_col INT);
CREATE INDEX idx_index_on_index ON test_db_2.match(offset);
CREATE INDEX idx_key_col ON test_db_2.match(`match`);
CREATE UNIQUE INDEX uniq_unique_col ON test_db_2.match(unique_col);

-- Keep CREATE and COMMIT in one runner statement so they use the same connection.
CREATE TABLE test_db_1.ddl_start_transaction (
    id INT PRIMARY KEY,
    name VARCHAR(32)
) START TRANSACTION; COMMIT;
-- Ordinary DML is only allowed after the internal DDL transaction commits.
INSERT INTO test_db_1.ddl_start_transaction VALUES (1, 'after_commit');
UPDATE test_db_1.ddl_start_transaction SET name = 'updated' WHERE id = 1;

-- The internal option can use lowercase and precede other table options.
CREATE TABLE test_db_1.ddl_start_transaction_options (
    id INT PRIMARY KEY
) start
transaction ENGINE=InnoDB COMMENT='START TRANSACTION'; COMMIT;

-- MySQL 8.0.21+ logs InnoDB CTAS as CREATE TABLE ... START TRANSACTION followed by row events.
CREATE TABLE test_db_1.ctas_source (
    id INT PRIMARY KEY,
    name VARCHAR(32),
    amount DECIMAL(10, 2)
) ENGINE=InnoDB;
INSERT INTO test_db_1.ctas_source VALUES
    (1, 'first', 10.50),
    (2, 'second', 20.00),
    (3, NULL, NULL),
    (4, '', 0.00),
    (5, 'fifth', -5.25),
    (6, 'sixth', 60.75);

-- Copy more rows than sinker.batch_size without inheriting the source primary key.
CREATE TABLE test_db_1.ctas_copy AS
    SELECT * FROM test_db_1.ctas_source ORDER BY id;

-- Explicit keys and subsequent DML must work after the copied rows are applied.
CREATE TABLE test_db_1.ctas_with_pk (PRIMARY KEY (id)) ENGINE=InnoDB AS
    SELECT * FROM test_db_1.ctas_source ORDER BY id;
UPDATE test_db_1.ctas_with_pk SET name = 'updated', amount = 99.99 WHERE id = 1;
DELETE FROM test_db_1.ctas_with_pk WHERE id = 2;
INSERT INTO test_db_1.ctas_with_pk VALUES (7, 'inserted', 70.00);

-- The AS keyword is optional, and only the selected result rows should be copied.
CREATE TABLE test_db_1.ctas_projection ENGINE=InnoDB
    SELECT id, CONCAT(name, '_copy') AS copied_name, amount + 1.00 AS copied_amount
    FROM test_db_1.ctas_source WHERE id <= 3 ORDER BY id;

-- An empty result must still create the destination table.
CREATE TABLE test_db_1.ctas_empty ENGINE=InnoDB AS
    SELECT * FROM test_db_1.ctas_source WHERE 1 = 0;

-- The new table and the SELECT source can belong to different databases.
CREATE TABLE `test_db_2`.`ctas_cross_db` ENGINE=InnoDB AS
    SELECT * FROM `test_db_1`.`ctas_source` WHERE id >= 4 ORDER BY id;

-- A keyword inside a SQL comment must not turn ordinary DDL into transactional DDL.
CREATE TABLE test_db_1.ddl_comment_only (
    id INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /* START TRANSACTION */;
INSERT INTO test_db_1.ddl_comment_only VALUES (1);

-- Ignore block comments before, within, and after the real internal option.
CREATE /* START TRANSACTION */ TABLE test_db_1.ddl_comment_block (
    id INT PRIMARY KEY,
    note VARCHAR(64) DEFAULT 'START TRANSACTION' COMMENT 'column START TRANSACTION'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
COMMENT='table START TRANSACTION' START /* between keywords */ TRANSACTION /* trailing START TRANSACTION */; COMMIT;
INSERT INTO test_db_1.ddl_comment_block (id) VALUES (1);

-- Raw blocks preserve line comments and keep CREATE and COMMIT on the same connection.
```
CREATE TABLE test_db_1.ddl_comment_line (
    id INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
START TRANSACTION -- trailing START TRANSACTION
;
COMMIT;
```
INSERT INTO test_db_1.ddl_comment_line VALUES (1);

```
CREATE TABLE test_db_1.ddl_comment_hash (
    id INT PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
START TRANSACTION # trailing START TRANSACTION
;
COMMIT;
```
INSERT INTO test_db_1.ddl_comment_hash VALUES (1);

-- Comment delimiters and quoted keywords inside literals are data, not SQL comments/options.
```
CREATE TABLE test_db_1.ddl_comment_literal (
    id INT PRIMARY KEY,
    block_note VARCHAR(64) DEFAULT '/* START TRANSACTION */' COMMENT 'keep /* START TRANSACTION */',
    line_note VARCHAR(64) DEFAULT 'text -- START TRANSACTION' COMMENT 'keep -- START TRANSACTION',
    quote_note VARCHAR(64) DEFAULT 'literal ''START TRANSACTION''' COMMENT 'keep # START TRANSACTION'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='table ''START TRANSACTION''';
```
INSERT INTO test_db_1.ddl_comment_literal (id) VALUES (1);
