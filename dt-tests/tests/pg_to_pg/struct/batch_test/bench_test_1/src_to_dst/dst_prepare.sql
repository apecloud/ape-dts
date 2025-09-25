-- Use a DO block and a FOR loop to create the schemas and their objects dynamically.
```
DO $$
DECLARE
    i INT;
    schema_name TEXT;
BEGIN
    FOR i IN 1..10 LOOP
        schema_name := 'struct_it_pg2pg_' || i;

        -- Drop Schemas
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_name);
    END LOOP;
END $$;
```
