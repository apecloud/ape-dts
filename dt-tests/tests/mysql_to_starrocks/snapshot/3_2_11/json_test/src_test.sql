-- basic json object
INSERT INTO test_db_1.json_test VALUES (NULL, '{"k.1":1,"k.0":0,"k.-1":-1,"k.true":true,"k.false":false,"k.null":null,"k.string":"string","k.true_false":[true,false],"k.32767":32767,"k.32768":32768,"k.-32768":-32768,"k.-32769":-32769,"k.2147483647":2147483647,"k.2147483648":2147483648,"k.-2147483648":-2147483648,"k.-2147483649":-2147483649,"k.18446744073709551615":18446744073709551615,"k.18446744073709551616":18446744073709551616,"k.3.14":3.14,"k.{}":{},"k.[]":[]}');

-- unicode support
INSERT INTO test_db_1.json_test VALUES (NULL, '{"key":"éééàààà"}');
INSERT INTO test_db_1.json_test VALUES (NULL, '{"中文":"😀"}');

-- multiple nested json object
INSERT INTO test_db_1.json_test VALUES (NULL, '{"literal1":true,"i16":4,"i32":2147483647,"int64":4294967295,"double":1.0001,"string":"abc","time":"2022-01-01 12:34:56.000000","array":[1,2,{"i16":4,"array":[false,true,"abcd"]}],"small_document":{"i16":4,"array":[false,true,3],"small_document":{"i16":4,"i32":2147483647,"int64":4294967295}}}'),(5, '[{"i16":4,"small_document":{"i16":4,"i32":2147483647,"int64":4294967295}},{"i16":4,"array":[false,true,"abcd"]},"abc",10,null,true,false]');

-- null
INSERT INTO test_db_1.json_test VALUES (NULL, null);

-- json with empty key
INSERT INTO test_db_1.json_test VALUES (NULL, '{"bitrate":{"":0}}');

-- json array
INSERT INTO test_db_1.json_test VALUES (NULL, '[-1,0,1,true,false,null,"string",[true,false],32767,32768,-32768,-32769,2147483647,2147483648,-2147483648,-2147483649,18446744073709551615,18446744073709551616,3.14,{},[]]');

-- json array nested
INSERT INTO test_db_1.json_test VALUES (NULL, '[-1,["b",["c"]],1]');

-- scalar string
INSERT INTO test_db_1.json_test VALUES (NULL, '"scalar string"'),(11, '"LONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONGLONG"');

-- scalar boolean: true
INSERT INTO test_db_1.json_test VALUES (NULL, 'true');

-- scalar boolean: false
INSERT INTO test_db_1.json_test VALUES (NULL, 'false');

-- scalar null
INSERT INTO test_db_1.json_test VALUES (NULL, 'null');

-- scalar negative integer
INSERT INTO test_db_1.json_test VALUES (NULL, '-1');

-- scalar positive integer
INSERT INTO test_db_1.json_test VALUES (NULL, '1');

-- scalar max positive int16
INSERT INTO test_db_1.json_test VALUES (NULL, '32767');

-- scalar int32
INSERT INTO test_db_1.json_test VALUES (NULL, '32768');

-- scalar min negative int16
INSERT INTO test_db_1.json_test VALUES (NULL, '-32768');

-- scalar negative int32
INSERT INTO test_db_1.json_test VALUES (NULL, '-32769');

-- scalar max_positive int32
INSERT INTO test_db_1.json_test VALUES (NULL, '2147483647');

-- scalar positive int64
INSERT INTO test_db_1.json_test VALUES (NULL, '2147483648');

-- scalar min negative int32
INSERT INTO test_db_1.json_test VALUES (NULL, '-2147483648');

-- scalar negative int64
INSERT INTO test_db_1.json_test VALUES (NULL, '-2147483649');

-- scalar uint64
INSERT INTO test_db_1.json_test VALUES (NULL, '18446744073709551615');

-- scalar uint64 overflow
INSERT INTO test_db_1.json_test VALUES (NULL, '18446744073709551616');

-- scalar float
INSERT INTO test_db_1.json_test VALUES (NULL, '3.14');

-- scalar datetime
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON));

-- scalar time
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST('23:24:25' AS TIME) AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON));

-- scalar timestamp
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, (CAST(UNIX_TIMESTAMP(CONVERT_TZ('2015-01-15 23:24:25','GMT',@@session.time_zone)) AS JSON)));

-- scalar geometry
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(ST_GeomFromText('POINT(1 1)') AS JSON));

-- scalar string with charset conversion
INSERT INTO test_db_1.json_test VALUES (NULL, CAST('[]' AS CHAR CHARACTER SET 'ascii'));

-- scalar binary as base64
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(x'cafe' AS JSON));
INSERT INTO test_db_1.json_test VALUES (NULL, CAST(x'cafebabe' AS JSON));

-- scalar decimal
-- TODO, decimal will lose precision when insert into target mysql as string
-- INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST("212765.700000000010000" AS DECIMAL(21,15)) AS JSON))
-- INSERT INTO test_db_1.json_test VALUES (NULL, CAST(CAST("111111.11111110000001" AS DECIMAL(24,17)) AS JSON))

-- empty object
INSERT INTO test_db_1.json_test VALUES (NULL, '{}');

-- empty array
INSERT INTO test_db_1.json_test VALUES (NULL, '[]');

-- set partial update with holes
INSERT INTO test_db_1.json_test VALUES (NULL, '{"age":22,"addr":{"code":100,"detail":{"ab":"970785C8-C299"}},"name":"Alice"}');
UPDATE test_db_1.json_test SET f_1 = JSON_SET(f_1, '$.addr.detail.ab', '970785C8');

-- remove partial update with holes
INSERT INTO test_db_1.json_test VALUES (NULL, '{"age":22,"addr":{"code":100,"detail":{"ab":"970785C8-C299"}},"name":"Alice"}');
UPDATE test_db_1.json_test SET f_1 = JSON_REMOVE(f_1, '$.addr.detail.ab');

-- remove partial update with holes and sparse keys
INSERT INTO test_db_1.json_test VALUES (NULL, '{"17fc9889474028063990914001f6854f6b8b5784":"test_field_for_remove_fields_behaviour_2","1f3a2ea5bc1f60258df20521bee9ac636df69a3a":{"currency":"USD"},"4f4d99a438f334d7dbf83a1816015b361b848b3b":{"currency":"USD"},"9021162291be72f5a8025480f44bf44d5d81d07c":"test_field_for_remove_fields_behaviour_3_will_be_removed","9b0ed11532efea688fdf12b28f142b9eb08a80c5":{"currency":"USD"},"e65ad0762c259b05b4866f7249eabecabadbe577":"test_field_for_remove_fields_behaviour_1_updated","ff2c07edcaa3e987c23fb5cc4fe860bb52becf00":{"currency":"USD"}}');
UPDATE test_db_1.json_test SET f_1 = JSON_REMOVE(f_1, '$."17fc9889474028063990914001f6854f6b8b5784"');

-- replace partial update with holes
INSERT INTO test_db_1.json_test VALUES (NULL, '{"age":22,"addr":{"code":100,"detail":{"ab":"970785C8-C299"}},"name":"Alice"}');
UPDATE test_db_1.json_test SET f_1 = JSON_REPLACE(f_1, '$.addr.detail.ab', '9707');

-- remove array value
INSERT INTO test_db_1.json_test VALUES (NULL, '["foo","bar","baz"]');
UPDATE test_db_1.json_test SET f_1 = JSON_REMOVE(f_1, '$[1]');