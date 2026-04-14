-- Use anonymous block to generate 1000 independent INSERT events for CDC
DO $$
BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO public.check_large_test (id, name, value, data)
        VALUES (i, 'user_' || i::text, i * 100, 'data_' || i::text);
    END LOOP;
END
$$;
