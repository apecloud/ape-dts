DROP EVENT TRIGGER IF EXISTS ape_dts_intercept_ddl;
DROP FUNCTION IF EXISTS public.ape_dts_capture_ddl() CASCADE;
DROP TABLE IF EXISTS public.ape_dts_ddl_command;
CREATE TABLE public.ape_dts_ddl_command (id BIGSERIAL PRIMARY KEY, ddl_text TEXT, tag TEXT, schema TEXT);
CREATE FUNCTION public.ape_dts_capture_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $BODY$
BEGIN
  INSERT INTO public.ape_dts_ddl_command(ddl_text, tag, schema) VALUES (current_query(), TG_TAG, current_schema());
END;
$BODY$;
CREATE EVENT TRIGGER ape_dts_intercept_ddl ON ddl_command_end EXECUTE PROCEDURE public.ape_dts_capture_ddl();
ALTER EVENT TRIGGER ape_dts_intercept_ddl ENABLE ALWAYS;
DROP SCHEMA IF EXISTS "rules_inventory" CASCADE;
CREATE SCHEMA "rules_inventory";
DROP SCHEMA IF EXISTS "rules_archive" CASCADE;
CREATE SCHEMA "rules_archive";
DROP SCHEMA IF EXISTS "rules_plain" CASCADE;
CREATE SCHEMA "rules_plain";
CREATE TABLE "rules_inventory"."orders" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE TABLE "rules_inventory"."audit" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE TABLE "rules_archive"."unchained" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE TABLE "rules_plain"."identity" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE TABLE "rules_inventory"."order,log:1" ("id" INT PRIMARY KEY, "note" VARCHAR(80), "secret" VARCHAR(80));
CREATE TABLE "rules_inventory"."literal*" (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
CREATE TABLE "rules_inventory"."literal_other" (id INT PRIMARY KEY, note VARCHAR(80), secret VARCHAR(80));
