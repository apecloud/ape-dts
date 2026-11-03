INSERT INTO test_db_1.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_1.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

INSERT INTO test_db_2.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_2.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

INSERT INTO test_db_3.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_3.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

INSERT INTO test_db_4.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_4.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

INSERT INTO test_db_5.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO test_db_5.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

INSERT INTO other_test_db_1.one_pk_no_uk_1 VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij','kl','mn','op',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', NULL);
INSERT INTO other_test_db_1.one_pk_no_uk_2 VALUES (2,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', NULL);

-- Keep a final row so unexpected replication cannot pass as empty data.
INSERT INTO `wildchar_filter`.`star` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`star_many` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`prestar` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`question` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`question1` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`question12` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`regex_1` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`regex_12` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`regex_x` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`regex_12tail` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`exact_name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`exact_name_tail` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`plus+name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`plusname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`paren(name)` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`parenname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`bracket[12]` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`bracket1` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`brace{2}` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`bracee` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`caret^name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`caretname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`dollar$name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`dollarname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`pipe|name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`pipe_tail` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`quoted*name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`quotedXname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`quoted?name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`quotedYname` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_star` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_star_many` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skipXstar` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_question` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_question1` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_question12` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_regex_1` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip_regex_x` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skip+name` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`skipname` VALUES (1, 10);

INSERT INTO `wildchar_filter`.`plus+star` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`plus+star_many` VALUES (1, 10);
INSERT INTO `wildchar_filter`.`plusstar` VALUES (1, 10);
