INSERT INTO test_db_1.one_pk_multi_uk VALUES (1,1,1,1,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.one_pk_multi_uk VALUES (2,2,1,2,50,654321.4321,4321.23,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

-- PRIMARY KEY (`f_0`), 
-- UNIQUE KEY `uk_1` (`f_1`,`f_2`), 
-- UNIQUE KEY `uk_2` (`f_3`,`f_4`,`f_5`), 
-- UNIQUE KEY `uk_3` (`f_6`,`f_7`,`f_8`)

-- no pk/uk col changed
UPDATE test_db_1.one_pk_multi_uk SET f_27='d' WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_27='d' WHERE f_0=2;
UPDATE test_db_1.one_pk_multi_uk SET f_27='c' WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_27='c' WHERE f_0=2;

-- pk col changed
UPDATE test_db_1.one_pk_multi_uk SET f_0=10 WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_0=1 WHERE f_0=2;
UPDATE test_db_1.one_pk_multi_uk SET f_0=2 WHERE f_0=10;
UPDATE test_db_1.one_pk_multi_uk SET f_0=10 WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_0=1 WHERE f_0=2;
UPDATE test_db_1.one_pk_multi_uk SET f_0=2 WHERE f_0=10;

-- uk col changed
UPDATE test_db_1.one_pk_multi_uk SET f_1=10 WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_1=1 WHERE f_0=2;
UPDATE test_db_1.one_pk_multi_uk SET f_1=2 WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_1=10 WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_1=1 WHERE f_0=2;
UPDATE test_db_1.one_pk_multi_uk SET f_1=2 WHERE f_0=1;

DELETE FROM test_db_1.one_pk_multi_uk;