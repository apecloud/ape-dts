-- Use a DO block and a FOR loop to create the schemas and their objects dynamically.
```
DO $$
DECLARE
    i INT;
    schema_name TEXT;
BEGIN

    FOR i IN 1..10 LOOP
        schema_name := 'struct_it_pg2pg_' || i;

        -- Drop and Create the Schema
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name);
        EXECUTE format('CREATE SCHEMA %I', schema_name);

        -- all basic column types:
        EXECUTE format('
            CREATE TABLE %I.full_column_type (
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
            )', schema_name);

        -- array column types:
        EXECUTE format('
            CREATE TABLE %I.array_table (
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
            )', schema_name);

        -- all check types(without fk and exclude):
        EXECUTE format('
            CREATE TABLE %I.full_constraint_type (
                id SERIAL PRIMARY KEY,
                varchar_col VARCHAR(255) NOT NULL,
                unique_col VARCHAR(255) UNIQUE,
                not_null_col VARCHAR(255) NOT NULL,
                check_col VARCHAR(255) CHECK (char_length(check_col) > 3)
            )', schema_name);

        -- all index types:
        EXECUTE format('
            CREATE TABLE %I.full_index_type (
                id SERIAL PRIMARY KEY,
                unique_col VARCHAR(255) NOT NULL,
                index_col VARCHAR(255),
                fulltext_col TSVECTOR,
                spatial_col POINT NOT NULL,
                simple_index_col VARCHAR(255),
                composite_index_col1 VARCHAR(255),
                composite_index_col2 VARCHAR(255),
                composite_index_col3 VARCHAR(255)
            )', schema_name);

        EXECUTE format('CREATE UNIQUE INDEX unique_index ON %I.full_index_type (unique_col)', schema_name);
        EXECUTE format('CREATE INDEX index_index ON %I.full_index_type (index_col)', schema_name);
        EXECUTE format('CREATE INDEX fulltext_index ON %I.full_index_type USING gin(fulltext_col)', schema_name);
        EXECUTE format('CREATE INDEX spatial_index ON %I.full_index_type USING gist(spatial_col)', schema_name);
        EXECUTE format('CREATE INDEX simple_index ON %I.full_index_type (simple_index_col)', schema_name);
        EXECUTE format('CREATE INDEX composite_index ON %I.full_index_type (composite_index_col1, composite_index_col2, composite_index_col3)', schema_name);

        -- table comments:
        EXECUTE format('COMMENT ON TABLE %I.full_column_type IS %L', schema_name, 'Comment on full_column_type.');
        EXECUTE format('COMMENT ON TABLE %I.full_index_type IS %L', schema_name, 'Comment on full_index_type.');

        -- column comments:
        EXECUTE format('COMMENT ON COLUMN %I.full_column_type.id IS %L', schema_name, 'Comment on full_column_type.id.');
        EXECUTE format('COMMENT ON COLUMN %I.full_index_type.id IS %L', schema_name, 'Comment on full_index_type.id.');

        -- sequences

        -- case 1: sequences created automatically when creating table
        EXECUTE format('CREATE TABLE %I.sequence_test_1 (seq_1 SERIAL, seq_2 BIGSERIAL, seq_3 SMALLSERIAL)', schema_name);

        -- case 2: create independent sequences, then alter their owners
        EXECUTE format('CREATE SEQUENCE %I.sequence_test_2_seq_1', schema_name);
        EXECUTE format('CREATE SEQUENCE %I.sequence_test_2_seq_2', schema_name);
        EXECUTE format('CREATE SEQUENCE %I.sequence_test_2_seq_3', schema_name);
        EXECUTE format('CREATE TABLE %I.sequence_test_2 (seq_1 INTEGER, seq_2 BIGINT, seq_3 SMALLINT)', schema_name);
        EXECUTE format('ALTER SEQUENCE %I.sequence_test_2_seq_1 OWNED BY %I.sequence_test_2.seq_1', schema_name, schema_name);
        EXECUTE format('ALTER SEQUENCE %I.sequence_test_2_seq_2 OWNED BY %I.sequence_test_2.seq_2', schema_name, schema_name);
        EXECUTE format('ALTER SEQUENCE %I.sequence_test_2_seq_3 OWNED BY %I.sequence_test_2.seq_3', schema_name, schema_name);

        -- case 3: create independent sequences, use them in column defaults without ownership
        EXECUTE format('CREATE SEQUENCE %I.sequence_test_3_seq_2', schema_name);
        EXECUTE format('CREATE SEQUENCE %I."sequence_test_3_seq.\d@_3"', schema_name);
        EXECUTE format('
            CREATE TABLE %I.sequence_test_3 (
                seq_1 SERIAL,
                seq_2 BIGINT DEFAULT nextval(%L),
                seq_3 SMALLINT DEFAULT nextval(%L)
            )', schema_name, schema_name || '.sequence_test_3_seq_2', schema_name || '."sequence_test_3_seq.\d@_3"');

        -- case 4: create independent sequences and never used by any tables
        EXECUTE format('CREATE SEQUENCE %I.sequence_test_4_seq_1', schema_name);

        -- test view filtered
        EXECUTE format('CREATE VIEW %I.full_column_type_view AS SELECT * FROM %I.full_column_type', schema_name, schema_name);

        -- special character
        EXECUTE format('
            CREATE TABLE %I."special_character_$1#@*_table" (
                id SERIAL PRIMARY KEY,
                varchar_col VARCHAR(255) NOT NULL,
                unique_col VARCHAR(255) UNIQUE,
                not_null_col VARCHAR(255) NOT NULL,
                check_col VARCHAR(255) CHECK (char_length(check_col) > 3)
            )', schema_name);

    END LOOP;
END $$;
```