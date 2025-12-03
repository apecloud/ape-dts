-- Use a DO block and a FOR loop to create the schemas and their objects dynamically.
DO $$
DECLARE
    i INT;
    schema_name TEXT;
BEGIN

    FOR i IN 1..100 LOOP
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

    END LOOP;
END $$;