INSERT INTO test_db_1.no_pk_no_uk VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.no_pk_no_uk VALUES (2,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.no_pk_no_uk VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO test_db_1.one_pk_no_uk VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.one_pk_no_uk VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);
INSERT INTO test_db_1.one_pk_no_uk VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO test_db_1.no_pk_one_uk VALUES (1,1,1,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.no_pk_one_uk VALUES (2,2,1,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);
INSERT INTO test_db_1.no_pk_one_uk VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO test_db_1.no_pk_multi_uk VALUES (1,1,1,1,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.no_pk_multi_uk VALUES (2,2,1,2,50,654321.4321,4321.23,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);
INSERT INTO test_db_1.no_pk_multi_uk VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO test_db_1.one_pk_multi_uk VALUES (1,1,1,1,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.one_pk_multi_uk VALUES (2,2,1,2,50,654321.4321,4321.23,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);
INSERT INTO test_db_1.one_pk_multi_uk VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO test_db_1.col_has_special_character_table VALUES(1, 'col:1:value', 'col&2:value', 'col\3:value');
INSERT INTO test_db_1.col_has_special_character_table VALUES(2, NULL, NULL, NULL);

-- min for each col
INSERT INTO test_db_1.numeric_table VALUES(-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0);
-- max for each col
INSERT INTO test_db_1.numeric_table VALUES(127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615);
-- zero
INSERT INTO test_db_1.numeric_table VALUES(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

UPDATE test_db_1.no_pk_no_uk SET f_1=20, f_2=30, f_3=40, f_4=50, f_5=654321.4321, f_6=4321.21, f_7=54321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28=NULL WHERE f_0=1;

UPDATE test_db_1.one_pk_no_uk SET f_1=20, f_2=30, f_3=40, f_4=50, f_5=654321.4321, f_6=4321.21, f_7=54321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28=NULL WHERE f_0=1;
UPDATE test_db_1.one_pk_no_uk SET f_1=2, f_2=3, f_3=4, f_4=5, f_5=123456.1234, f_6=1234.12, f_7=12345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij', f_19='kl', f_20='mn', f_21='op', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28=NULL WHERE f_0=2;

UPDATE test_db_1.no_pk_one_uk SET f_1=20, f_2=300, f_3=400, f_4=50, f_5=654321.4321, f_6=4321.21, f_7=54321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28=NULL WHERE f_0=1;
UPDATE test_db_1.no_pk_one_uk SET f_1=2, f_2=30, f_3=40, f_4=5, f_5=123456.1234, f_6=1234.12, f_7=12345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij', f_19='kl', f_20='mn', f_21='op', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28=NULL WHERE f_0=2;

UPDATE test_db_1.no_pk_multi_uk SET f_1=200, f_2=300, f_3=400, f_4=500, f_5=54321.4321, f_6=321.21, f_7=4321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28=NULL WHERE f_0=1;
UPDATE test_db_1.no_pk_multi_uk SET f_1=20, f_2=30, f_3=40, f_4=50, f_5=23456.1234, f_6=234.12, f_7=2345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij', f_19='kl', f_20='mn', f_21='op', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28=NULL WHERE f_0=2;

UPDATE test_db_1.one_pk_multi_uk SET f_1=200, f_2=300, f_3=400, f_4=500, f_5=54321.4321, f_6=321.21, f_7=4321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28=NULL WHERE f_0=1;
UPDATE test_db_1.one_pk_multi_uk SET f_1=20, f_2=30, f_3=40, f_4=50, f_5=23456.1234, f_6=234.12, f_7=2345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij', f_19='kl', f_20='mn', f_21='op', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28=NULL WHERE f_0=2;

UPDATE test_db_1.col_has_special_character_table SET `col"1`=NULL, `col,2`=NULL, `col\3`=NULL WHERE `p:k`=1;
UPDATE test_db_1.col_has_special_character_table SET `col"1`='col:1:value', `col,2`='col&2:value', `col\3`='col\3:value' WHERE `p:k`=2;

UPDATE test_db_1.numeric_table SET f_1=255, f_2=32767, f_3=65535, f_4=8388607, f_5=16777215, f_6=2147483647, f_7=4294967295, f_8=9223372036854775807, f_9=18446744073709551615 WHERE f_0=-128;
UPDATE test_db_1.numeric_table SET f_1=0, f_2=0, f_3=0, f_4=0, f_5=0, f_6=0, f_7=0, f_8=0, f_9=0 WHERE f_0=127;
UPDATE test_db_1.numeric_table SET f_1=0, f_2=-32768, f_3=0, f_4=-8388608, f_5=0, f_6=-2147483648, f_7=0, f_8=-9223372036854775808, f_9=0 WHERE f_0=0;

DELETE FROM test_db_1.no_pk_no_uk;
DELETE FROM test_db_1.one_pk_no_uk;
DELETE FROM test_db_1.no_pk_one_uk;
DELETE FROM test_db_1.no_pk_multi_uk;
DELETE FROM test_db_1.one_pk_multi_uk;
DELETE FROM test_db_1.col_has_special_character_table;
DELETE FROM test_db_1.numeric_table;

-- charset tests
INSERT INTO test_db_1.tb_col_ansi VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_ansi VALUES(2, 'efg');
INSERT INTO test_db_1.tb_col_ansi VALUES(3, NULL);
UPDATE test_db_1.tb_col_ansi set value = 'efg' WHERE pk = 3;
UPDATE test_db_1.tb_col_ansi set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_ansi set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_ansi;

INSERT INTO test_db_1.tb_col_gbk VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gbk VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gbk VALUES(3, NULL);
UPDATE test_db_1.tb_col_gbk set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gbk set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gbk set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gbk;

INSERT INTO test_db_1.tb_col_gb18030 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gb18030 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gb18030 VALUES(3, NULL);
UPDATE test_db_1.tb_col_gb18030 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gb18030 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gb18030 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gb18030;

INSERT INTO test_db_1.tb_col_gb2312 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gb2312 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gb2312 VALUES(3, NULL);
UPDATE test_db_1.tb_col_gb2312 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gb2312 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gb2312 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gb2312;

INSERT INTO test_db_1.tb_col_big5 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_big5 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_big5 VALUES(3, NULL);
UPDATE test_db_1.tb_col_big5 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_big5 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_big5 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_big5;

INSERT INTO test_db_1.tb_col_ujis VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_ujis VALUES(2, 'わたし');
INSERT INTO test_db_1.tb_col_ujis VALUES(3, NULL);
UPDATE test_db_1.tb_col_ujis set value = 'わたし' WHERE pk = 3;
UPDATE test_db_1.tb_col_ujis set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_ujis set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_ujis;

INSERT INTO test_db_1.tb_col_euckr VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_euckr VALUES(2, '대한민국');
INSERT INTO test_db_1.tb_col_euckr VALUES(3, NULL);
UPDATE test_db_1.tb_col_euckr set value = '대한민국' WHERE pk = 3;
UPDATE test_db_1.tb_col_euckr set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_euckr set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_euckr;

INSERT INTO test_db_1.tb_col_utf8 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_utf8 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_utf8 VALUES(3, 'わたし');
INSERT INTO test_db_1.tb_col_utf8 VALUES(4, '대한민국');
INSERT INTO test_db_1.tb_col_utf8 VALUES(5, NULL);
UPDATE test_db_1.tb_col_utf8 set value = '대한민국' WHERE pk = 5;
UPDATE test_db_1.tb_col_utf8 set value = 'わたし' WHERE pk = 4;
UPDATE test_db_1.tb_col_utf8 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_utf8 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_utf8 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_utf8;

INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(3, '😀');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(4, NULL);
UPDATE test_db_1.tb_col_utf8mb4 set value = '😀' WHERE pk = 4;
UPDATE test_db_1.tb_col_utf8mb4 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_utf8mb4 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_utf8mb4 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_utf8mb4;

-- json
INSERT INTO test_db_1.json_test VALUES (1, '{"age":22,"addr":{"code":100,"detail":{"ab":"970785C8-C299"}},"name":"''Alice''"}');
UPDATE test_db_1.json_test SET f_1 = JSON_SET(f_1, '$.addr.detail.ab', '''970785C8''');
DELETE FROM test_db_1.json_test;

-- col value with '
INSERT INTO test_db_1.tb_col_value_has_special_char VALUES(1, '中文''''abc''');
UPDATE test_db_1.tb_col_value_has_special_char set value = 'abc''中文''''' WHERE pk = 1;
DELETE FROM test_db_1.tb_col_value_has_special_char;
