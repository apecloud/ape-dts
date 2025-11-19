drop schema if exists test_db_1 CASCADE;
drop schema if exists test_db_2 CASCADE;

create schema test_db_1;
create schema test_db_2;

CREATE TABLE test_db_1.full_column_type (
    id serial PRIMARY KEY, 

    -- char
    char_col char,
    char_col_2 char(255),
    character_col character,
    character_col_2 character(255),

    -- varchar
    varchar_col varchar, 
    varchar_col_2 varchar(255), 
    character_varying_col character varying,
    character_varying_col_2 character varying(255),

    bpchar_col bpchar,
    bpchar_col_2 bpchar(10),

    text_col text, 

    -- float
    real_col real, 
    float4_col float4,

    -- double
    double_precision_col double precision, 
    float8_col float8,

    -- decimal
    numeric_col numeric, 
    numeric_col_2 numeric(10, 2), 
    decimal_col decimal, 
    decimal_col_2 decimal(10, 2), 

    -- small int
    smallint_col smallint, 
    int2_col int2,
    smallserial_col smallserial,
    serial2_col smallserial,

    -- int
    integer_col integer,
    int_col int,
    int4_col int,
    serial_col serial,
    serial4_col serial4,

    -- bigint
    bigint_col bigint, 
    int8_col int8, 
    bigserial_col bigserial,
    serial8_col serial8,

    -- bit
    bit_col bit,
    bit_col_2 bit(10),
    bit_varying_col bit varying,
    bit_varying_col_2 bit varying(10),
    varbit_col varbit,
    varbit_col_2 varbit(10),

    -- time
    time_col time, 
    time_col_2 time(6),
    time_col_3 time without time zone,
    time_col_4 time(6) without time zone,

    -- timez
    timez_col timetz,
    timez_col_2 timetz(6),
    timez_col_3 time with time zone,
    timez_col_4 time(6) with time zone,

    -- timestamp
    timestamp_col timestamp, 
    timestamp_col_2 timestamp(6),
    timestamp_col_3 timestamp without time zone,
    timestamp_col_4 timestamp(6) without time zone,

    -- timestampz
    timestampz_col timestamptz,
    timestampz_col_2 timestamptz(6),
    timestampz_col_3 timestamp with time zone,
    timestampz_col_4 timestamp(6) with time zone,

    date_col date, 
    
    bytea_col bytea, 

    -- bool
    boolean_col boolean, 
    bool_col bool,

    -- json
    json_col json, 
    jsonb_col jsonb, 

    -- interval
    interval_col interval, 
    interval_col_2 interval(3), 

    -- array
    array_float4_col float4[],
    array_float8_col float8[],

    array_int2_col int2[],
    array_int4_col int4[],
    array_int8_col bigint[],
    array_int8_col_2 int8[],

    array_text_col text[],

    array_boolean_col boolean[],
    array_boolean_col_2 bool[],

    array_date_col date[],

    array_timestamp_col timestamp[],
    array_timestamp_col_2 timestamp(6)[],
    array_timestamptz_col timestamptz[],
    array_timestamptz_col_2 timestamptz(6)[],

    -- others
    box_col box, 
    cidr_col cidr,
    circle_col circle,
    inet_col inet,

    line_col line,
    lseg_col lseg, 
    macaddr_col macaddr,
    macaddr8_col macaddr8,
    money_col money,

    path_col path, 
    pg_lsn_col pg_lsn,
    pg_snapshot_col pg_snapshot,
    polygon_col polygon, 
    point_col point, 

    tsquery_col tsquery,
    tsvector_col tsvector,
    txid_snapshot_col txid_snapshot,

    uuid_col uuid, 
    xml_col xml
);

CREATE TABLE test_db_1.check_pk_cols_order (
  col_1 INT,
  col_2 INT,
  col_3 INT,
  pk_3 INT,
  pk_1 INT, 
  col_4 INT,
  pk_2 INT,
  col_5 INT,
  PRIMARY KEY(pk_1, pk_2, pk_3)
);


CREATE TABLE test_db_2.router_test_1 (
  pk INT,
  col_1 INT,
  PRIMARY KEY(pk)
);

CREATE TABLE test_db_2.router_test_2 (
  pk INT,
  col_1 INT,
  PRIMARY KEY(pk)
);