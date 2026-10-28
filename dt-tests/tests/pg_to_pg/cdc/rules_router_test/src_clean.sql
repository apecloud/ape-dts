DROP EVENT TRIGGER IF EXISTS ape_dts_intercept_ddl;
DROP FUNCTION IF EXISTS public.ape_dts_capture_ddl() CASCADE;
DROP TABLE IF EXISTS public.ape_dts_ddl_command;
DROP SCHEMA IF EXISTS "rules_shard_a" CASCADE;
DROP SCHEMA IF EXISTS "rules_shard_b" CASCADE;
