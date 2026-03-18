-- prepare user-defined functions for struct copy test
DROP SCHEMA IF EXISTS struct_it_pg2pg_1 CASCADE;
CREATE SCHEMA struct_it_pg2pg_1;

-- 1. base SQL function
CREATE OR REPLACE FUNCTION struct_it_pg2pg_1.add_int(a int, b int)
RETURNS int
LANGUAGE sql
AS $$
    SELECT a + b;
$$;

-- 2. PL/pgSQL function depending on add_int (func2 depends on func1)
CREATE OR REPLACE FUNCTION struct_it_pg2pg_1.add_three(a int, b int, c int)
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN struct_it_pg2pg_1.add_int(
               struct_it_pg2pg_1.add_int(a, b),
               c
           );
END;
$$;

-- 3. PL/pgSQL factorial function (self-recursive)
CREATE OR REPLACE FUNCTION struct_it_pg2pg_1.factorial(n int)
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    res int := 1;
    i   int;
BEGIN
    IF n IS NULL OR n <= 1 THEN
        RETURN 1;
    END IF;

    FOR i IN 2..n LOOP
        res := res * i;
    END LOOP;

    RETURN res;
END;
$$;
