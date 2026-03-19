# Migrate data from Postgres to Postgres

# Prerequisites

- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/pg_to_pg.md) and [common configs](/docs/en/config.md) for more details.

# Prepare Postgres instances

## Source

```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d "$POSTGRES_IMAGE"
```

- set wal_level to logical

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

ALTER SYSTEM SET wal_level = logical;

-- restart container
docker restart some-postgres-1
```

## Target

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d "$POSTGRES_IMAGE"
```

# Migrate structures

## Prepare data

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE SCHEMA test_db;
CREATE TABLE test_db.tb_1(id int, value int, primary key(id));
```

## Start task

```
rm -rf /tmp/ape_dts
mkdir -p /tmp/ape_dts

cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
extract_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
sink_type=struct
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db

[parallelizer]
parallel_type=serial

[pipeline]
buffer_size=100
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Check results

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SET search_path TO test_db;
\d
```

```
         List of relations
 Schema  | Name | Type  |  Owner
---------+------+-------+----------
 test_db | tb_1 | table | postgres
```

# Migrate snapshot data

## Prepare data

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

INSERT INTO test_db.tb_1 VALUES(1,1),(2,2),(3,3),(4,4);
```

## Start task

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=snapshot
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Check results

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
 id | value
----+-------
  1 |     1
  2 |     2
  3 |     3
  4 |     4
```

# Check data

- check the differences between target data and source data

## Prepare data

- change target table records

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=1 WHERE id=2;
```

## Start task

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=snapshot
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s

[checker]
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_dbs=test_db
do_events=insert

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/:/logs/" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Check results

- cat /tmp/ape_dts/check_data_task_log/check/miss.log

```
{"schema":"test_db","tb":"tb_1","id_col_values":{"id":"1"},"diff_col_values":{}}
```

- cat /tmp/ape_dts/check_data_task_log/check/diff.log

```
{"schema":"test_db","tb":"tb_1","id_col_values":{"id":"2"},"diff_col_values":{"value":{"src":"2","dst":"1"}}}
```

# Revise data

- revise target data based on "check data" task results

## Start task

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./check_data_task_log

[sinker]
db_type=pg
sink_type=write
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Check results

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
 id | value
----+-------
  1 |     1
  2 |     2
  3 |     3
  4 |     4
```

# Review data

- check if target data revised based on "check data" task results

## Start task

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=check_log
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
check_log_dir=./check_data_task_log

[checker]
db_type=pg
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[filter]
do_events=*

[parallelizer]
parallel_type=rdb_check
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
-v "/tmp/ape_dts/check_data_task_log/check/:/check_data_task_log/" \
-v "/tmp/ape_dts/review_data_task_log/:/logs/" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Check results

- /tmp/ape_dts/review_data_task_log/check/miss.log and /tmp/ape_dts/review_data_task_log/check/diff.log should be empty

# CDC task

## Drop replication slot if exists

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

SELECT pg_drop_replication_slot('ape_test') FROM pg_replication_slots WHERE slot_name = 'ape_test';
```

## Start task

- this will create slot if not exists.

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=pg
sink_type=write
batch_size=200
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Change source data

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

DELETE FROM test_db.tb_1 WHERE id=1;
UPDATE test_db.tb_1 SET value=2000000 WHERE id=2;
INSERT INTO test_db.tb_1 VALUES(5,5);
```

## Check results

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_1 ORDER BY id;
```

```
 id |  value
----+---------
  2 | 2000000
  3 |       3
  4 |       4
  5 |       5
```

# CDC task with ddl capture

## Enable ddl capture in source

- Create a meta table to store ddl info

```
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
```

- Create a function to capture ddl and record it into ddl meta table

```
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
  declare alter_table varchar;
  declare record_object record;
  declare message text;
  declare pub RECORD;
begin

  select current_query() into ddl_text;

  if TG_TAG = 'CREATE TABLE' then -- ALTER TABLE schema.TABLE REPLICA IDENTITY FULL;
    show server_version_num into current_version;
    if current_version >= pg_version_95 then
      for record_object in (select * from pg_event_trigger_ddl_commands()) loop
        if record_object.command_tag = 'CREATE TABLE' then
          object_id := record_object.object_identity;
        end if;
      end loop;
    else
      select btrim(substring(ddl_text from '[ \t\r\n\v\f]*[c|C][r|R][e|E][a|A][t|T][e|E][ \t\r\n\v\f]*.*[ \t\r\n\v\f]*[t|T][a|A][b|B][l|L][e|E][ \t\r\n\v\f]+(.*)\(.*'),' \t\r\n\v\f') into object_id;
    end if;
    if object_id = '' or object_id is null then
      message := 'CREATE TABLE, but ddl_text=' || ddl_text || ', current_query=' || current_query();
    end if;
    if current_version >= pg_version_10 then
      for pub in (select * from pg_publication where pubname like 'ape_dts_%') loop
        raise notice 'pubname=%',pub.pubname;
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
```

- Alter the function owner to your account

```
ALTER FUNCTION public.ape_dts_capture_ddl() OWNER TO postgres;
```

- Create an event trigger on ddl_command_end and execute the capture function

```
CREATE EVENT TRIGGER ape_dts_intercept_ddl ON ddl_command_end
EXECUTE PROCEDURE public.ape_dts_capture_ddl();
```

## Start task

```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=pg
extract_type=cdc
url=postgres://postgres:postgres@127.0.0.1:5433/postgres?options[statement_timeout]=10s
slot_name=ape_test
ddl_meta_tb=public.ape_dts_ddl_command

[filter]
do_dbs=test_db
do_events=insert,update,delete
do_ddls=create_schema,drop_schema,alter_schema,create_table,alter_table,drop_table,create_index,drop_index,truncate_table,rename_table

[sinker]
db_type=pg
sink_type=write
batch_size=200
url=postgres://postgres:postgres@127.0.0.1:5434/postgres?options[statement_timeout]=10s

[parallelizer]
parallel_type=rdb_merge
parallel_size=8

[pipeline]
buffer_size=16000
checkpoint_interval_secs=1
EOL
```

```
docker run --rm --network host \
-v "/tmp/ape_dts/task_config.ini:/task_config.ini" \
"$APE_DTS_IMAGE" /task_config.ini
```

## Do ddls in source

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5433 -W

CREATE TABLE test_db.tb_2(id int, value int, primary key(id));
INSERT INTO test_db.tb_2 VALUES(1,1);
```

## Check results

```
psql -h 127.0.0.1 -U postgres -d postgres -p 5434 -W

SELECT * FROM test_db.tb_2 ORDER BY id;
```

```
 id | value
----+-------
  1 |     1
```
