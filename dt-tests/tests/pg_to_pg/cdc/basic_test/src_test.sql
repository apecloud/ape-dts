INSERT INTO default_table VALUES (1, 30, '2019-02-10 11:34:58', '2019-02-10 11:35:00', '10:20:11', '10:20:12', '2019-02-01', '$20', B'101', 32766, 2147483646, 9223372036854775806, 3.14, true, 3.14768, 1234.56, 'Test', '(0,0),(1,1)', '<(0,0),1>', '01:02:03', '{0,1,2}', '((0,0),(1,1))', '((0,0),(0,1),(0,2))', '(1,1)', '((0,0),(0,1),(1,1))', 'a', 'hello world', '{"key": 123}', '<doc><item>abc</item></doc>', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', B'101', '192.168.1.100', '192.168.1', '08:00:2b:01:02:03');
INSERT INTO default_table VALUES (2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
UPDATE default_table SET val=30, created_at='2019-02-10 11:34:58', created_at_tz='2019-02-10 11:35:00', ctime='10:20:11', ctime_tz='10:20:12', cdate='2019-02-01', cmoney='$20', cbits=B'101', csmallint=32766, cinteger= 2147483646, cbigint=9223372036854775806, creal=3.14, cbool=true, cfloat8=3.14768, cnumeric=1234.56, cvarchar='Test', cbox='(0,0),(1,1)', ccircle='<(0,0),1>', cinterval='01:02:03', cline='{0,1,2}', clseg='((0,0),(1,1))', cpath='((0,0),(0,1),(0,2))', cpoint='(1,1)', cpolygon='((0,0),(0,1),(1,1))', cchar='a', ctext='hello world', cjson='{"key": 123}', cxml='<doc><item>abc</item></doc>', cuuid='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', cvarbit=B'101', cinet='192.168.1.100', ccidr='192.168.1', cmacaddr='08:00:2b:01:02:03' WHERE pk=2;
UPDATE default_table SET val=NULL, created_at=NULL, created_at_tz=NULL, ctime=NULL, ctime_tz=NULL, cdate=NULL, cmoney=NULL, cbits=NULL, csmallint=NULL, cinteger=NULL, cbigint=NULL, creal=NULL, cbool=NULL, cfloat8=NULL, cnumeric=NULL, cvarchar=NULL, cbox=NULL, ccircle=NULL, cinterval=NULL, cline=NULL, clseg=NULL, cpath=NULL, cpoint=NULL, cpolygon=NULL, cchar=NULL, ctext=NULL, cjson=NULL, cxml=NULL, cuuid=NULL, cvarbit=NULL, cinet=NULL, ccidr=NULL, cmacaddr=NULL WHERE pk=1;
DELETE FROM default_table;

INSERT INTO numeric_table (pk, si, i, bi, r, db, r_int, db_int, r_nan, db_nan, r_pinf, db_pinf, r_ninf, db_ninf, ss, bs, b, o) VALUES (1, 1, 123456, 1234567890123, 3.3, 4.44, 3, 4, 'NaN', 'NaN', 'Infinity', 'Infinity', '-Infinity', '-Infinity', 1, 123, true, 4000000000);
INSERT INTO numeric_table (pk, si, i, bi, r, db, r_int, db_int, r_nan, db_nan, r_pinf, db_pinf, r_ninf, db_ninf, ss, bs, b, o) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2, 321, NULL, NULL);
UPDATE numeric_table SET si=1, i=123456, bi=1234567890123, r=3.3, db=4.44, r_int=3, db_int=4, r_nan='NaN', db_nan='NaN', r_pinf='Infinity', db_pinf='Infinity', r_ninf='-Infinity', db_ninf='-Infinity', ss=1, bs=123, b=true, o=4000000000 WHERE pk=2;
UPDATE numeric_table SET si=NULL, i=NULL, bi=NULL, r=NULL, db=NULL, r_int=NULL, db_int=NULL, r_nan=NULL, db_nan=NULL, r_pinf=NULL, db_pinf=NULL, r_ninf=NULL, db_ninf=NULL, ss=2, bs=321, b=NULL, o=NULL WHERE pk=1;
DELETE FROM numeric_table;

INSERT INTO numeric_decimal_table (pk, d, dzs, dvs, d_nn, n, nzs, nvs, d_int, dzs_int, dvs_int, n_int, nzs_int, nvs_int, d_nan, dzs_nan, dvs_nan, n_nan, nzs_nan, nvs_nan) VALUES (1, 1.1, 10.11, 10.1111, 3.30, 22.22, 22.2, 22.2222, 1, 10, 10, 22, 22, 22, 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN');
INSERT INTO numeric_decimal_table (pk, d, dzs, dvs, d_nn, n, nzs, nvs, d_int, dzs_int, dvs_int, n_int, nzs_int, nvs_int, d_nan, dzs_nan, dvs_nan, n_nan, nzs_nan, nvs_nan) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE numeric_decimal_table SET d=1.1, dzs=10.11, dvs=10.1111, d_nn=3.30, n=22.22, nzs=22.2, nvs=22.2222, d_int=1, dzs_int=10, dvs_int=10, n_int=22, nzs_int=22, nvs_int=22, d_nan='NaN', dzs_nan='NaN', dvs_nan='NaN', n_nan='NaN', nzs_nan='NaN', nvs_nan='NaN' WHERE pk=2;
UPDATE numeric_decimal_table SET d=NULL, dzs=NULL, dvs=NULL, d_nn=NULL, n=NULL, nzs=NULL, nvs=NULL, d_int=NULL, dzs_int=NULL, dvs_int=NULL, n_int=NULL, nzs_int=NULL, nvs_int=NULL, d_nan=NULL, dzs_nan=NULL, dvs_nan=NULL, n_nan=NULL, nzs_nan=NULL, nvs_nan=NULL WHERE pk=1;
DELETE FROM numeric_decimal_table;

INSERT INTO string_table (pk, vc, vcv, ch, c, t, b, bnn, ct) VALUES (1, 'žš', 'bb', 'cdef', 'abc', 'some text', E'\\000\\001\\002'::bytea, E'\\003\\004\\005'::bytea, 'Hello World');
INSERT INTO string_table (pk, vc, vcv, ch, c, t, b, bnn, ct) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE string_table SET vc='žš', vcv='bb', ch='cdef', c='abc', t='some text', b=E'\\000\\001\\002'::bytea, bnn=E'\\003\\004\\005'::bytea, ct='Hello World' WHERE pk=2;
UPDATE string_table SET vc=NULL, vcv=NULL, ch=NULL, c=NULL, t=NULL, b=NULL, bnn=NULL, ct=NULL WHERE pk=1;
DELETE FROM string_table;

INSERT INTO network_address_table (pk, i) VALUES (1, '192.168.2.0/12');
INSERT INTO network_address_table (pk, i) VALUES (2, NULL);
UPDATE network_address_table SET i='192.168.2.0/12' WHERE pk=2;
UPDATE network_address_table SET i=NULL WHERE pk=1;
DELETE FROM network_address_table;

INSERT INTO cidr_network_address_table (pk, i) VALUES (1, '192.168.100.128/25');
INSERT INTO cidr_network_address_table (pk, i) VALUES (2, NULL);
UPDATE cidr_network_address_table SET i='192.168.100.128/25' WHERE pk=2;
UPDATE cidr_network_address_table SET i=NULL WHERE pk=1;
DELETE FROM cidr_network_address_table;

INSERT INTO macaddr_table (pk, m) VALUES (1, '08:00:2b:01:02:03');
INSERT INTO macaddr_table (pk, m) VALUES (2, NULL);
UPDATE macaddr_table SET m='08:00:2b:01:02:03' WHERE pk=2;
UPDATE macaddr_table SET m=NULL WHERE pk=1;
DELETE FROM macaddr_table;

INSERT INTO cash_table (pk, csh) VALUES (1, '$1234.11');
INSERT INTO cash_table (pk, csh) VALUES (2, '($1234.11)');
INSERT INTO cash_table (pk, csh) VALUES (3, NULL);
INSERT INTO cash_table (pk, csh) VALUES (4, NULL);
UPDATE cash_table SET csh='$1234.11' WHERE pk=3;
UPDATE cash_table SET csh='($1234.11)' WHERE pk=4;
UPDATE cash_table SET csh=NULL WHERE pk=1;
UPDATE cash_table SET csh=NULL WHERE pk=2;
DELETE FROM cash_table;

INSERT INTO bitbin_table (pk, ba, bol, bol2, bs, bs7, bv, bv2, bvl, bvunlimited1, bvunlimited2) VALUES (1, E'\\001\\002\\003'::bytea, '0'::bit(1), '1'::bit(1), '11'::bit(2), '1'::bit(7), '00'::bit(2), '000000110000001000000001'::bit(24),'1000000000000000000000000000000000000000000000000000000000000000'::bit(64), '101', '111011010001000110000001000000001');
INSERT INTO bitbin_table (pk, ba, bol, bol2, bs, bs7, bv, bv2, bvl, bvunlimited1, bvunlimited2) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE bitbin_table SET ba=E'\\001\\002\\003'::bytea, bol='0'::bit(1), bol2='1'::bit(1), bs='11'::bit(2), bs7='1'::bit(7), bv='00'::bit(2), bv2='000000110000001000000001'::bit(24), bvl= '1000000000000000000000000000000000000000000000000000000000000000'::bit(64), bvunlimited1='101', bvunlimited2='111011010001000110000001000000001' WHERE pk=2;
UPDATE bitbin_table SET ba=NULL, bol=NULL, bol2=NULL, bs=NULL, bs7=NULL, bv=NULL, bv2=NULL, bvl=NULL, bvunlimited1=NULL, bvunlimited2=NULL WHERE pk=1;
DELETE FROM bitbin_table;

INSERT INTO bytea_binmode_table (pk, ba) VALUES (1, E'\\001\\002\\003'::bytea);
INSERT INTO bytea_binmode_table (pk, ba) VALUES (2, NULL);
UPDATE bytea_binmode_table SET ba=E'\\001\\002\\003'::bytea WHERE pk=2;
UPDATE bytea_binmode_table SET ba=NULL WHERE pk=1;
DELETE FROM bytea_binmode_table;

INSERT INTO time_table(pk, ts, tsneg, ts_ms, ts_us, tz, date, date_pinf, date_ninf, ti, tip, ttf, ttz, tptz, it, ts_large, ts_large_us, ts_large_ms, tz_large, ts_max, ts_min, tz_max, tz_min, ts_pinf, ts_ninf, tz_pinf, tz_ninf) VALUES (1, '2016-11-04T13:51:30.123456'::TIMESTAMP, '1936-10-25T22:10:12.608'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456+02:00'::TIMESTAMPTZ, '2016-11-04'::DATE, 'infinity'::DATE, '-infinity'::DATE, '13:51:30'::TIME, '13:51:30.123'::TIME, '24:00:00'::TIME, '13:51:30.123789+02:00'::TIMETZ, '13:51:30.123+02:00'::TIMETZ, 'P1Y2M3DT4H5M6.78S'::INTERVAL,'21016-11-04T13:51:30.123456'::TIMESTAMP, '21016-11-04T13:51:30.123457'::TIMESTAMP, '21016-11-04T13:51:30.124'::TIMESTAMP,'21016-11-04T13:51:30.123456+07:00'::TIMESTAMPTZ,'294247-01-01T23:59:59.999999'::TIMESTAMP,'4713-12-31T23:59:59.999999 BC'::TIMESTAMP,'294247-01-01T23:59:59.999999+00:00'::TIMESTAMPTZ,'4714-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ,'infinity'::TIMESTAMP,'-infinity'::TIMESTAMP,'infinity'::TIMESTAMPTZ,'-infinity'::TIMESTAMPTZ);
INSERT INTO time_table(pk, ts, tsneg, ts_ms, ts_us, tz, date, date_pinf, date_ninf, ti, tip, ttf, ttz, tptz, it, ts_large, ts_large_us, ts_large_ms, tz_large, ts_max, ts_min, tz_max, tz_min, ts_pinf, ts_ninf, tz_pinf, tz_ninf) VALUES (2, '2016-11-04T13:51:30.123456'::TIMESTAMP, '1936-10-25T22:10:12.608'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123456+02:00'::TIMESTAMPTZ, '2016-11-04'::DATE, '2016-11-04'::DATE, '2016-11-04'::DATE, '13:51:30'::TIME, '13:51:30.123'::TIME, '24:00:00'::TIME, '13:51:30.123789+02:00'::TIMETZ, '13:51:30.123+02:00'::TIMETZ, 'P1Y2M3DT4H5M6.78S'::INTERVAL,'21016-11-04T13:51:30.123456'::TIMESTAMP, '2016-11-04T13:51:30.123457'::TIMESTAMP, '2016-11-04T13:51:30.124'::TIMESTAMP,'2016-11-04T13:51:30.123456+07:00'::TIMESTAMPTZ,'2016-01-01T23:59:59.999999'::TIMESTAMP,'2016-12-31T23:59:59.999999 BC'::TIMESTAMP,'2016-01-01T23:59:59.999999+00:00'::TIMESTAMPTZ,'2016-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ,'2016-12-31T23:59:59.999999Z BC'::TIMESTAMP,'2016-12-31T23:59:59.999999Z BC'::TIMESTAMP,'2016-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ,'2016-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ);
INSERT INTO time_table(pk, ts, tsneg, ts_ms, ts_us, tz, date, date_pinf, date_ninf, ti, tip, ttf, ttz, tptz, it, ts_large, ts_large_us, ts_large_ms, tz_large, ts_max, ts_min, tz_max, tz_min, ts_pinf, ts_ninf, tz_pinf, tz_ninf) VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO time_table(pk, ts, tsneg, ts_ms, ts_us, tz, date, date_pinf, date_ninf, ti, tip, ttf, ttz, tptz, it, ts_large, ts_large_us, ts_large_ms, tz_large, ts_max, ts_min, tz_max, tz_min, ts_pinf, ts_ninf, tz_pinf, tz_ninf) VALUES (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE time_table SET ts='2016-11-04T13:51:30.123456'::TIMESTAMP, tsneg='1936-10-25T22:10:12.608'::TIMESTAMP, ts_ms='2016-11-04T13:51:30.123456'::TIMESTAMP, ts_us='2016-11-04T13:51:30.123456'::TIMESTAMP, tz='2016-11-04T13:51:30.123456+02:00'::TIMESTAMPTZ, date='2016-11-04'::DATE, date_pinf='infinity'::DATE, date_ninf='-infinity'::DATE, ti='13:51:30'::TIME, tip='13:51:30.123'::TIME, ttf='24:00:00'::TIME, ttz='13:51:30.123789+02:00'::TIMETZ, tptz='13:51:30.123+02:00'::TIMETZ, it='P1Y2M3DT4H5M6.78S'::INTERVAL, ts_large='21016-11-04T13:51:30.123456'::TIMESTAMP, ts_large_us='21016-11-04T13:51:30.123457'::TIMESTAMP, ts_large_ms='21016-11-04T13:51:30.124'::TIMESTAMP, tz_large='21016-11-04T13:51:30.123456+07:00'::TIMESTAMPTZ, ts_max='294247-01-01T23:59:59.999999'::TIMESTAMP, ts_min='4713-12-31T23:59:59.999999 BC'::TIMESTAMP, tz_max='294247-01-01T23:59:59.999999+00:00'::TIMESTAMPTZ, tz_min='4714-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ, ts_pinf='infinity'::TIMESTAMP, ts_ninf='-infinity'::TIMESTAMP, tz_pinf='infinity'::TIMESTAMPTZ, tz_ninf='-infinity'::TIMESTAMPTZ WHERE pk=3;
UPDATE time_table SET ts='2016-11-04T13:51:30.123456'::TIMESTAMP, tsneg='1936-10-25T22:10:12.608'::TIMESTAMP, ts_ms='2016-11-04T13:51:30.123456'::TIMESTAMP, ts_us='2016-11-04T13:51:30.123456'::TIMESTAMP, tz='2016-11-04T13:51:30.123456+02:00'::TIMESTAMPTZ, date='2016-11-04'::DATE, date_pinf='infinity'::DATE, date_ninf='-infinity'::DATE, ti='13:51:30'::TIME, tip='13:51:30.123'::TIME, ttf='24:00:00'::TIME, ttz='13:51:30.123789+02:00'::TIMETZ, tptz='13:51:30.123+02:00'::TIMETZ, it='P1Y2M3DT4H5M6.78S'::INTERVAL, ts_large='21016-11-04T13:51:30.123456'::TIMESTAMP, ts_large_us='21016-11-04T13:51:30.123457'::TIMESTAMP, ts_large_ms='21016-11-04T13:51:30.124'::TIMESTAMP, tz_large='21016-11-04T13:51:30.123456+07:00'::TIMESTAMPTZ, ts_max='294247-01-01T23:59:59.999999'::TIMESTAMP, ts_min='4713-12-31T23:59:59.999999 BC'::TIMESTAMP, tz_max='294247-01-01T23:59:59.999999+00:00'::TIMESTAMPTZ, tz_min='4714-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ, ts_pinf='2016-12-31T23:59:59.999999Z BC'::TIMESTAMP, ts_ninf='2016-12-31T23:59:59.999999Z BC'::TIMESTAMP, tz_pinf='2016-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ, tz_ninf='2016-12-31T23:59:59.999999Z BC'::TIMESTAMPTZ WHERE pk=4;
UPDATE time_table SET ts=NULL, tsneg=NULL, ts_ms=NULL, ts_us=NULL, tz=NULL, date=NULL, date_pinf=NULL, date_ninf=NULL, ti=NULL, tip=NULL, ttf=NULL, ttz=NULL, tptz=NULL, it=NULL, ts_large=NULL, ts_large_us=NULL, ts_large_ms=NULL, tz_large=NULL, ts_max=NULL, ts_min=NULL, tz_max=NULL, tz_min=NULL, ts_pinf=NULL, ts_ninf=NULL, tz_pinf=NULL, tz_ninf=NULL WHERE pk=1;
UPDATE time_table SET ts=NULL, tsneg=NULL, ts_ms=NULL, ts_us=NULL, tz=NULL, date=NULL, date_pinf=NULL, date_ninf=NULL, ti=NULL, tip=NULL, ttf=NULL, ttz=NULL, tptz=NULL, it=NULL, ts_large=NULL, ts_large_us=NULL, ts_large_ms=NULL, tz_large=NULL, ts_max=NULL, ts_min=NULL, tz_max=NULL, tz_min=NULL, ts_pinf=NULL, ts_ninf=NULL, tz_pinf=NULL, tz_ninf=NULL WHERE pk=2;
DELETE FROM time_table;

INSERT INTO text_table(pk, j, jb, x, u) VALUES (1, '{"bar": "baz"}'::json, '{"bar": "baz"}'::jsonb, '<foo>bar</foo><foo>bar</foo>'::xml, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID);
INSERT INTO text_table(pk, j, jb, x, u) VALUES (2, NULL, NULL, NULL, NULL);
UPDATE text_table SET j='{"bar": "baz"}'::json, jb='{"bar": "baz"}'::jsonb, x='<foo>bar</foo><foo>bar</foo>'::xml, u='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID WHERE pk=2;
UPDATE text_table SET j=NULL, jb=NULL, x=NULL, u=NULL WHERE pk=1;
DELETE FROM text_table;

INSERT INTO geom_table(pk, p) VALUES (1, '(1,1)'::point);
INSERT INTO geom_table(pk, p) VALUES (2, NULL);
UPDATE geom_table SET p='(1,1)'::point WHERE pk=2;
UPDATE geom_table SET p=NULL WHERE pk=1;
DELETE FROM geom_table;

INSERT INTO range_table (pk, unbounded_exclusive_tsrange, bounded_inclusive_tsrange, unbounded_exclusive_tstzrange, bounded_inclusive_tstzrange, unbounded_exclusive_daterange, bounded_exclusive_daterange, int4_number_range, numerange, int8_number_range) VALUES (1, '[2019-03-31 15:30:00, infinity)', '[2019-03-31 15:30:00, 2019-04-30 15:30:00]', '[2017-06-05 11:29:12.549426+00,)', '[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]', '[2019-03-31, infinity)', '[2019-03-31, 2019-04-30)', '[1000,6000)', '[5.3,6.3)', '[1000000,6000000)');
INSERT INTO range_table (pk, unbounded_exclusive_tsrange, bounded_inclusive_tsrange, unbounded_exclusive_tstzrange, bounded_inclusive_tstzrange, unbounded_exclusive_daterange, bounded_exclusive_daterange, int4_number_range, numerange, int8_number_range) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE range_table SET unbounded_exclusive_tsrange='[2019-03-31 15:30:00, infinity)', bounded_inclusive_tsrange='[2019-03-31 15:30:00, 2019-04-30 15:30:00]', unbounded_exclusive_tstzrange='[2017-06-05 11:29:12.549426+00,)', bounded_inclusive_tstzrange='[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]',  unbounded_exclusive_daterange='[2019-03-31, infinity)', bounded_exclusive_daterange='[2019-03-31, 2019-04-30)', int4_number_range='[1000,6000)', numerange='[5.3,6.3)', int8_number_range='[1000000,6000000)' WHERE pk=2;
UPDATE range_table SET unbounded_exclusive_tsrange=NULL, bounded_inclusive_tsrange=NULL, unbounded_exclusive_tstzrange=NULL, bounded_inclusive_tstzrange=NULL, unbounded_exclusive_daterange=NULL, bounded_exclusive_daterange=NULL, int4_number_range=NULL, numerange=NULL, int8_number_range=NULL WHERE pk=1;
DELETE FROM range_table;

INSERT INTO array_table (pk, int_array, bigint_array, text_array, char_array, varchar_array, date_array, numeric_array, varnumeric_array, citext_array, inet_array, cidr_array, macaddr_array, tsrange_array, tstzrange_array, daterange_array, int4range_array, numerange_array, int8range_array, uuid_array, json_array, jsonb_array, oid_array) VALUES (1, '{1,2,3}', '{1550166368505037572}', '{"one","two","three"}', '{"cone","ctwo","cthree"}', '{"vcone","vctwo","vcthree"}', '{2016-11-04,2016-11-05,2016-11-06}', '{1.2,3.4,5.6}', '{1.1,2.22,3.333}', '{"four","five","six"}', '{"192.168.2.0/12","192.168.1.1","192.168.0.2/1"}', '{"192.168.100.128/25", "192.168.0.0/25", "192.168.1.0/24"}', '{"08:00:2b:01:02:03", "08-00-2b-01-02-03", "08002b:010203"}','{"[2019-03-31 15:30:00, infinity)", "[2019-03-31 15:30:00, 2019-04-30 15:30:00]"}', '{"[2017-06-05 11:29:12.549426+00,)", "[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]"}', '{"[2019-03-31, infinity)", "[2019-03-31, 2019-04-30)"}', '{"[1,6)", "[1,4)"}', '{"[5.3,6.3)", "[10.0,20.0)"}', '{"[1000000,6000000)", "[5000,9000)"}', '{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "f0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}',array['{"bar": "baz"}','{"foo": "qux"}']::json[], array['{"bar": "baz"}','{"foo": "qux"}']::jsonb[], '{3,4000000000}');
INSERT INTO array_table (pk, int_array, bigint_array, text_array, char_array, varchar_array, date_array, numeric_array, varnumeric_array, citext_array, inet_array, cidr_array, macaddr_array, tsrange_array, tstzrange_array, daterange_array, int4range_array, numerange_array, int8range_array, uuid_array, json_array, jsonb_array, oid_array) VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
UPDATE array_table SET int_array='{1,2,3}', bigint_array='{1550166368505037572}', text_array='{"one","two","three"}', char_array='{"cone","ctwo","cthree"}', varchar_array='{"vcone","vctwo","vcthree"}', date_array='{2016-11-04,2016-11-05,2016-11-06}', numeric_array='{1.2,3.4,5.6}', varnumeric_array='{1.1,2.22,3.333}', citext_array='{"four","five","six"}', inet_array='{"192.168.2.0/12","192.168.1.1","192.168.0.2/1"}', cidr_array='{"192.168.100.128/25", "192.168.0.0/25", "192.168.1.0/24"}', macaddr_array='{"08:00:2b:01:02:03", "08-00-2b-01-02-03", "08002b:010203"}', tsrange_array='{"[2019-03-31 15:30:00, infinity)", "[2019-03-31 15:30:00, 2019-04-30 15:30:00]"}',  tstzrange_array='{"[2017-06-05 11:29:12.549426+00,)", "[2017-06-05 11:29:12.549426+00, 2017-06-05 12:34:56.789012+00]"}',  daterange_array='{"[2019-03-31, infinity)", "[2019-03-31, 2019-04-30)"}', int4range_array='{"[1,6)", "[1,4)"}',  numerange_array='{"[5.3,6.3)", "[10.0,20.0)"}',  int8range_array='{"[1000000,6000000)", "[5000,9000)"}', uuid_array='{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "f0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}',  json_array=array['{"bar": "baz"}','{"foo": "qux"}']::json[], jsonb_array=array['{"bar": "baz"}','{"foo": "qux"}']::jsonb[], oid_array='{3,4000000000}' WHERE pk=2;
UPDATE array_table SET int_array=NULL, bigint_array=NULL, text_array=NULL, char_array=NULL, varchar_array=NULL, date_array=NULL, numeric_array=NULL, varnumeric_array=NULL, citext_array=NULL, inet_array=NULL, cidr_array=NULL, macaddr_array=NULL, tsrange_array=NULL, tstzrange_array=NULL, daterange_array=NULL, int4range_array=NULL, numerange_array=NULL, int8range_array=NULL, uuid_array=NULL, json_array=NULL, jsonb_array=NULL, oid_array=NULL WHERE pk=1;
DELETE FROM array_table;

INSERT INTO custom_table (pk, lt, i, n, lt_array) VALUES (1, 'Top.Collections.Pictures.Astronomy.Galaxies', '978-0-393-04002-9', 'some_text', '{"Ship.Frigate","Ship.Destroyer"}');
INSERT INTO custom_table (pk, lt, i, n, lt_array) VALUES (2, NULL, NULL, NULL, NULL);
UPDATE custom_table SET lt='Top.Collections.Pictures.Astronomy.Galaxies', i='978-0-393-04002-9', n='some_text', lt_array='{"Ship.Frigate","Ship.Destroyer"}' WHERE pk=2;
UPDATE custom_table SET lt=NULL, i=NULL, n=NULL, lt_array=NULL WHERE pk=1;
DELETE FROM custom_table;

INSERT INTO hstore_table (pk, hs) VALUES (1, '"key" => "val"'::hstore);
INSERT INTO hstore_table (pk, hs) VALUES (2, NULL);
UPDATE hstore_table SET hs='"key" => "val"'::hstore WHERE pk=2;
UPDATE hstore_table SET hs=NULL WHERE pk=1;
DELETE FROM hstore_table;

INSERT INTO hstore_table_mul (pk, hs, hsarr) VALUES (1, '"key1" => "val1","key2" => "val2","key3" => "val3"', array['"key4" => "val4","key5" => null'::hstore, '"key6" => "val6"']);
INSERT INTO hstore_table_mul (pk, hs, hsarr) VALUES (2, NULL, NULL);
UPDATE hstore_table_mul SET hs='"key1" => "val1","key2" => "val2","key3" => "val3"', hsarr=array['"key4" => "val4","key5" => null'::hstore, '"key6" => "val6"'] WHERE pk=2;
UPDATE hstore_table_mul SET hs=NULL, hsarr=NULL WHERE pk=1;
DELETE FROM hstore_table_mul;

INSERT INTO hstore_table_with_special (pk, hs) VALUES (1, '"key_#1" => "val 1","key 2" =>" ##123 78"');
INSERT INTO hstore_table_with_special (pk, hs) VALUES (2, NULL);
UPDATE hstore_table_with_special SET hs='"key_#1" => "val 1","key 2" =>" ##123 78"' WHERE pk=2;
UPDATE hstore_table_with_special SET hs=NULL WHERE pk=1;
DELETE FROM hstore_table_with_special;

INSERT INTO circle_table (pk, ccircle) VALUES (1, '((10, 20),10)'::circle);
INSERT INTO circle_table (pk, ccircle) VALUES (2, NULL);
UPDATE circle_table SET ccircle='((10, 20),10)'::circle WHERE pk=2;
UPDATE circle_table SET ccircle=NULL WHERE pk=1;
DELETE FROM circle_table;

INSERT INTO macaddr8_table (pk, m) VALUES (1, '08:00:2b:01:02:03:04:05');
INSERT INTO macaddr8_table (pk, m) VALUES (2, NULL);
UPDATE macaddr8_table SET m='08:00:2b:01:02:03:04:05' WHERE pk=2;
UPDATE macaddr8_table SET m=NULL WHERE pk=1;
DELETE FROM macaddr8_table;

INSERT INTO postgis_table (pk, p, ml) VALUES (1, 'SRID=3187;POINT(174.9479 -36.7208)'::geometry, 'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography);
INSERT INTO postgis_table (pk, p, ml) VALUES (2, NULL, NULL);
UPDATE postgis_table SET p='SRID=3187;POINT(174.9479 -36.7208)'::geometry, ml='MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography WHERE pk=2;
UPDATE postgis_table SET p=NULL, ml=NULL WHERE pk=1;
DELETE FROM postgis_table;

INSERT INTO postgis_array_table (pk, ga, gann) VALUES (1, ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::geometry], ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::geometry]);
INSERT INTO postgis_array_table (pk, ga, gann) VALUES (2, NULL, NULL);
UPDATE postgis_array_table SET ga=ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry], gann=ARRAY['GEOMETRYCOLLECTION EMPTY'::geometry, 'POLYGON((166.51 -46.64, 178.52 -46.64, 178.52 -34.45, 166.51 -34.45, 166.51 -46.64))'::geometry] WHERE pk=2;
UPDATE postgis_array_table SET ga=NULL, gann=NULL WHERE pk=1;
DELETE FROM postgis_array_table;

INSERT INTO timezone_table VALUES(1, now(), now(), now(), now());
INSERT INTO timezone_table VALUES(2, NULL, NULL, NULL, NULL);
UPDATE timezone_table SET t1=NULL, t2=NULL, t3=NULL, t4=NULL WHERE pk=1;
UPDATE timezone_table SET t1=now(), t2=now(), t3=now(), t4=now() WHERE pk=2;
DELETE FROM timezone_table;

INSERT INTO col_has_special_character_table VALUES(1, 'col:1:value', 'col&2:value', 'col\3:value');
INSERT INTO col_has_special_character_table VALUES(2, NULL, NULL, NULL);
UPDATE col_has_special_character_table SET "col`1"=NULL, "col,2"=NULL, "col\3"=NULL WHERE "p:k"=1;
UPDATE col_has_special_character_table SET "col`1"='col:1:value', "col,2"='col&2:value', "col\3"='col\3:value' WHERE "p:k"=2;
DELETE FROM col_has_special_character_table;

INSERT INTO ignore_cols_1 VALUES(1, 1, 1, 1),(2, 2, 2, 2);
UPDATE ignore_cols_1 SET f_1=5, f_2=5, f_3=5 WHERE f_0 > 0;
DELETE FROM ignore_cols_1;

INSERT INTO Upper_Case_DB.Upper_Case_TB VALUES(1, 1, 1, 1, 1),(2, 2, 2, 2, 2);
UPDATE Upper_Case_DB.Upper_Case_TB SET "FIELD_1"=5, field_2=5, Field_3=5, "Field_4"=5 WHERE "Id"=1;
UPDATE Upper_Case_DB.Upper_Case_TB SET "FIELD_1"=6, field_2=6, Field_3=6, "Field_4"=6 WHERE "Id"=2;
DELETE FROM Upper_Case_DB.Upper_Case_TB;

INSERT INTO "Upper_Case_DB"."Upper_Case_TB" VALUES(3, 3, 3, 3, 3),(4, 4, 4, 4, 4);
UPDATE "Upper_Case_DB"."Upper_Case_TB" SET "FIELD_1"=7, field_2=7, Field_3=7, "Field_4"=7 WHERE "Id"=3;
UPDATE "Upper_Case_DB"."Upper_Case_TB" SET "FIELD_1"=8, field_2=8, Field_3=8, "Field_4"=8 WHERE "Id"=4;
DELETE FROM "Upper_Case_DB"."Upper_Case_TB";

INSERT INTO pkuk.all_pks VALUES(1, 2, 3),(4, 5, 6),(7, 8, 9),(10, 11, 12);
UPDATE pkuk.all_pks SET pk3=13 WHERE pk1=1;
DELETE FROM pkuk.all_pks WHERE pk1=4;
INSERT INTO pkuk.tbl_1 VALUES(1, 'code1', 'name1'),(2, 'code2', 'name2'),(3, 'code3', 'name3');
UPDATE pkuk.tbl_1 SET name='name4' WHERE id=1;
DELETE FROM pkuk.tbl_1 WHERE id=2;
INSERT INTO pkuk.tbl_1 VALUES(4, 'code4', NULL);
UPDATE pkuk.tbl_1 SET name='name4' WHERE id=4 AND name is null;
INSERT INTO pkuk.tbl_2 VALUES('code1', 'name1'),('code1', 'name2'),('code3', 'name3');
UPDATE pkuk.tbl_2 SET code='code_updated' WHERE name='name1';
DELETE FROM pkuk.tbl_2 WHERE name='name2';
INSERT INTO pkuk.tbl_3 VALUES(1, 'code1', 'name1'),(2, 'code2', 'name2'),(3, 'code3', 'name3');
UPDATE pkuk.tbl_3 SET name='name4' WHERE id=1;
DELETE FROM pkuk.tbl_3 WHERE id=2;
INSERT INTO pkuk.tbl_4 VALUES('code1', 'name1'),('code2', 'name2'),('code3', 'name3');
UPDATE pkuk.tbl_4 SET name='name4' WHERE code='code1';
DELETE FROM pkuk.tbl_4 WHERE code='code3';
-- pkuk.tbl_5 no pk & uk, no `ALTER TABLE schema.tb_5 REPLICA IDENTITY FULL;`, cannot do update or delete
INSERT INTO pkuk.tbl_5 VALUES('code1', 'name1'),('code2', 'name2'),('code3', 'name3');