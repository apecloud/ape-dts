DROP EVENT TRIGGER IF EXISTS ape_dts_intercept_ddl;

DROP FUNCTION IF EXISTS public.ape_dts_capture_ddl() CASCADE;

DROP TABLE IF EXISTS public.ape_dts_ddl_command;

CREATE TABLE public.ape_dts_ddl_command
(
  ddl_text text COLLATE pg_catalog."default",
  id bigserial primary key,
  event text COLLATE pg_catalog."default",
  tag text COLLATE pg_catalog."default",
  username character varying COLLATE pg_catalog."default",
  database character varying COLLATE pg_catalog."default",
  schema character varying COLLATE pg_catalog."default",
  object_type character varying COLLATE pg_catalog."default",
  object_name character varying COLLATE pg_catalog."default",
  client_address character varying COLLATE pg_catalog."default",
  client_port integer,
  event_time timestamp with time zone,
  txid_current character varying(128) COLLATE pg_catalog."default",
  message text COLLATE pg_catalog."default"
);

CREATE FUNCTION public.ape_dts_capture_ddl()
  RETURNS event_trigger
  LANGUAGE 'plpgsql'
  COST 100
  VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
  declare ddl_text text;
  declare max_rows int := 10000;
  declare current_rows int;
  declare pg_version_95 int := 90500;
  declare pg_version_10 int := 100000;
  declare current_version int;
  declare object_id varchar;
  declare record_object record;
  declare message text;
  declare pub RECORD;
begin

  select current_query() into ddl_text;

  if TG_TAG = 'CREATE TABLE' then
    show server_version_num into current_version;
    if current_version >= pg_version_95 then
      for record_object in (select * from pg_event_trigger_ddl_commands()) loop
        if record_object.command_tag = 'CREATE TABLE' then
          object_id := record_object.object_identity;
        end if;
      end loop;
    end if;
    if object_id = '' or object_id is null then
      message := 'CREATE TABLE, but ddl_text=' || ddl_text || ', current_query=' || current_query();
    end if;
    if current_version >= pg_version_10 then
      for pub in (select * from pg_publication where pubname like 'ape_dts_%') loop
        BEGIN
          execute 'alter publication ' || pub.pubname || ' add table ' || object_id;
        EXCEPTION WHEN OTHERS THEN
        END;
      end loop;
    end if;
  end if;

  insert into public.ape_dts_ddl_command(id,event,tag,username,database,schema,object_type,object_name,client_address,client_port,event_time,ddl_text,txid_current,message)
  values (default,TG_EVENT,TG_TAG,current_user,current_database(),current_schema,'','',inet_client_addr(),inet_client_port(),current_timestamp,ddl_text,cast(TXID_CURRENT() as varchar(16)),message);

  select count(id) into current_rows from public.ape_dts_ddl_command;
  if current_rows > max_rows then
    delete from public.ape_dts_ddl_command where id in (select min(id) from public.ape_dts_ddl_command);
  end if;
end
$BODY$;

ALTER FUNCTION public.ape_dts_capture_ddl() OWNER TO postgres;

CREATE EVENT TRIGGER ape_dts_intercept_ddl ON ddl_command_end
EXECUTE PROCEDURE public.ape_dts_capture_ddl();

ALTER EVENT TRIGGER ape_dts_intercept_ddl ENABLE ALWAYS;

DROP SCHEMA IF EXISTS test_db_1 CASCADE;
CREATE SCHEMA test_db_1;
