drop schema if exists struct_it_pg2pg_1 CASCADE;

create schema struct_it_pg2pg_1;

-- all basic column types:
```
CREATE TABLE struct_it_pg2pg_1.full_column_type (
  id SERIAL PRIMARY KEY, 
  varchar_col VARCHAR(255) NOT NULL, 
  char_col CHAR(10), 
  text_col TEXT, 
  boolean_col BOOLEAN, 
  smallint_col SMALLINT, 
  integer_col INTEGER, 
  bigint_col BIGINT, 
  decimal_col DECIMAL(10, 2), 
  numeric_col NUMERIC(10, 2), 
  real_col REAL, 
  double_precision_col DOUBLE PRECISION, 
  date_col DATE, 
  time_col TIME, 
  timestamp_col TIMESTAMP, 
  interval_col INTERVAL, 
  bytea_col BYTEA, 
  uuid_col UUID, 
  xml_col XML, 
  json_col JSON, 
  jsonb_col JSONB, 
  point_col POINT, 
  line_col LINE, 
  lseg_col LSEG, 
  box_col BOX, 
  path_col PATH, 
  polygon_col POLYGON, 
  circle_col CIRCLE
);
```

-- array column types:
-- CREATE TABLE struct_it_pg2pg_1.array_table (pk SERIAL, int_array INT[], bigint_array BIGINT[], text_array TEXT[], char_array CHAR(10)[], varchar_array VARCHAR(10)[], date_array DATE[], numeric_array NUMERIC(10, 2)[], varnumeric_array NUMERIC[3], citext_array CITEXT[], inet_array INET[], cidr_array CIDR[], macaddr_array MACADDR[], tsrange_array TSRANGE[], tstzrange_array TSTZRANGE[], daterange_array DATERANGE[], int4range_array INT4RANGE[],numerange_array NUMRANGE[], int8range_array INT8RANGE[], uuid_array UUID[], json_array json[], jsonb_array jsonb[], oid_array OID[], PRIMARY KEY(pk));

-- postgres 12, without: CITEXT[]
```
CREATE TABLE struct_it_pg2pg_1.array_table (
  pk SERIAL, 
  int_array INT[], 
  bigint_array BIGINT[], 
  text_array TEXT[], 
  char_array CHAR(10) [], 
  varchar_array VARCHAR(10) [], 
  date_array DATE[], 
  numeric_array NUMERIC(10, 2) [], 
  varnumeric_array NUMERIC[3], 
  inet_array INET[], 
  cidr_array CIDR[], 
  macaddr_array MACADDR[], 
  tsrange_array TSRANGE[], 
  tstzrange_array TSTZRANGE[], 
  daterange_array DATERANGE[], 
  int4range_array INT4RANGE[], 
  numerange_array NUMRANGE[], 
  int8range_array INT8RANGE[], 
  uuid_array UUID[], 
  json_array json[], 
  jsonb_array jsonb[], 
  oid_array OID[], 
  PRIMARY KEY(pk)
);
```

-- all check types(without fk and exclude):
```
CREATE TABLE struct_it_pg2pg_1.full_constraint_type (
  id SERIAL PRIMARY KEY, 
  varchar_col VARCHAR(255) NOT NULL, 
  unique_col VARCHAR(255) UNIQUE, 
  not_null_col VARCHAR(255) NOT NULL, 
  check_col VARCHAR(255) CHECK (char_length(check_col) > 3)
);
```

-- all index types:
```
CREATE TABLE struct_it_pg2pg_1.full_index_type (
  id SERIAL PRIMARY KEY, 
  unique_col VARCHAR(255) NOT NULL, 
  index_col VARCHAR(255), 
  fulltext_col TSVECTOR, 
  spatial_col POINT NOT NULL, 
  simple_index_col VARCHAR(255), 
  composite_index_col1 VARCHAR(255), 
  composite_index_col2 VARCHAR(255), 
  composite_index_col3 VARCHAR(255)
);
```

CREATE UNIQUE INDEX unique_index ON struct_it_pg2pg_1.full_index_type (unique_col);

CREATE INDEX index_index ON struct_it_pg2pg_1.full_index_type (index_col);

CREATE INDEX fulltext_index ON struct_it_pg2pg_1.full_index_type USING gin(fulltext_col);

CREATE INDEX spatial_index ON struct_it_pg2pg_1.full_index_type USING gist(spatial_col);

CREATE INDEX simple_index ON struct_it_pg2pg_1.full_index_type (simple_index_col);

```
CREATE INDEX composite_index ON struct_it_pg2pg_1.full_index_type (
  composite_index_col1, composite_index_col2, 
  composite_index_col3
);
```

-- table comments:
COMMENT ON TABLE struct_it_pg2pg_1.full_column_type IS 'Comment on full_column_type.';
COMMENT ON TABLE struct_it_pg2pg_1.full_index_type IS 'Comment on full_index_type.';

-- column comments:
COMMENT ON COLUMN struct_it_pg2pg_1.full_column_type.id IS 'Comment on full_column_type.id.';
COMMENT ON COLUMN struct_it_pg2pg_1.full_index_type.id IS 'Comment on full_index_type.id.';

-- sequences

-- case 1: sequeces created automatically when creating table
CREATE TABLE struct_it_pg2pg_1.sequence_test_1 (seq_1 SERIAL, seq_2 BIGSERIAL, seq_3 SMALLSERIAL);

-- case 2: create independent sequences, then alter their owners
CREATE SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_1;
CREATE SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_2;
CREATE SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_3;

CREATE TABLE struct_it_pg2pg_1.sequence_test_2 (seq_1 INTEGER, seq_2 BIGINT, seq_3 SMALLINT);

-- in postgres, sequence must be in same schema as table it is linked to
-- actually, postgres allows mutiple sequences owned by the same table.column, here we just ignore
ALTER SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_1 OWNED BY struct_it_pg2pg_1.sequence_test_2.seq_1;
ALTER SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_2 OWNED BY struct_it_pg2pg_1.sequence_test_2.seq_2;
ALTER SEQUENCE struct_it_pg2pg_1.sequence_test_2_seq_3 OWNED BY struct_it_pg2pg_1.sequence_test_2.seq_3;

-- case 3: create independent sequences, use them in column defaults without ownership
-- we should migrate these sequences
CREATE SEQUENCE struct_it_pg2pg_1.sequence_test_3_seq_2;
CREATE SEQUENCE struct_it_pg2pg_1."sequence_test_3_seq.\d@_3";

```
CREATE TABLE struct_it_pg2pg_1.sequence_test_3 (
  seq_1 SERIAL, 
  seq_2 BIGINT DEFAULT nextval('struct_it_pg2pg_1.sequence_test_3_seq_2'), 
  seq_3 SMALLINT DEFAULT nextval('struct_it_pg2pg_1."sequence_test_3_seq.\d@_3"')
);
```

-- case 4: create independent sequences and never used by any tables
-- we should not migrate them
CREATE SEQUENCE struct_it_pg2pg_1.sequence_test_4_seq_1;

-- for case 1 & 2, the sequence ownership can be got by below sql

-- SELECT seq.relname,
--     tab.relname AS table_name,
--     attr.attname AS column_name,
--     ns.nspname
-- FROM pg_class AS seq
-- JOIN pg_namespace ns
--     ON (seq.relnamespace = ns.oid)
-- JOIN pg_depend AS dep
--     ON (seq.relfilenode = dep.objid)
-- JOIN pg_class AS tab
--     ON (dep.refobjid = tab.relfilenode)
-- JOIN pg_attribute AS attr
--     ON (attr.attnum = dep.refobjsubid AND attr.attrelid = dep.refobjid)
-- WHERE dep.deptype='a'
--     AND seq.relkind='S'
--     AND ns.nspname = 'struct_it_pg2pg_1';

-- test view filtered
CREATE VIEW struct_it_pg2pg_1.full_column_type_view AS SELECT * FROM struct_it_pg2pg_1.full_column_type;

-- special character
```
CREATE TABLE struct_it_pg2pg_1."special_character_$1#@*_table" (
  id SERIAL PRIMARY KEY, 
  varchar_col VARCHAR(255) NOT NULL, 
  unique_col VARCHAR(255) UNIQUE, 
  not_null_col VARCHAR(255) NOT NULL, 
  check_col VARCHAR(255) CHECK (char_length(check_col) > 3)
);
```