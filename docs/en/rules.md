# Filtering and routing configuration

Use `[source.*]` sections in the task INI file to choose databases, tables and columns, and to set their destination names. Connection and task settings are covered in the [configuration reference](config.md). For existing `[filter]` / `[router]` files and migration instructions, see [Legacy config](legacy_config.md).

## Configure a MySQL → MySQL snapshot task

This example copies `inventory` to `archive`, excluding tables whose names start with `tmp`. It writes `orders` to `archive.order_history`, selects rows with `id > 100`, excludes columns starting with `secret`, and renames `id` to `order_id`.

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://user:password@source:3306

[sinker]
db_type=mysql
sink_type=write
url=mysql://user:password@target:3306

[source.db]
include=inventory
route=inventory:archive

[source.table]
exclude=inventory.tmp*
where_conditions=inventory.orders:"id > 100"
route=inventory.orders:archive.order_history

[source.column]
exclude=inventory.orders.secret*
route=inventory.orders.id:archive.order_history.order_id

[source.event]
include=insert

[parallelizer]
parallel_type=snapshot
parallel_size=4

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10
```

Replace the connection details and prepare the corresponding destination tables. A data snapshot task does not create tables; see [structure migration](structure/migration.md). Destination columns that are not written must allow NULL or have defaults.

## Sections and object names

Separate levels in an object name with `.`, rules with `,`, and mappings with `source:destination`.

| Database type | Sections | Example table, collection or database number |
| --- | --- | --- |
| MySQL, TiDB, ClickHouse, StarRocks, Doris | `source.db`, `source.table`, `source.column` | `inventory.orders`, `inventory.orders.id` |
| PostgreSQL | `source.schema`, `source.table`, `source.column` | `inventory.orders`, `inventory.orders.id` |
| SQL Server | `source.db`, `source.schema`, `source.table`, `source.column` | `market.inventory.orders`, `market.inventory.orders.id` |
| MongoDB | `source.db`, `source.collection` | `inventory.orders` |
| Redis | `source.db` | `0`, `1` |

For PostgreSQL, the URL selects the connection database and table paths start with the schema. SQL Server table paths contain database, schema and table. `[source.database]` is an alias of `[source.db]`; use one spelling. Resource types unsupported by the database are ignored.

| Setting | Purpose | Example |
| --- | --- | --- |
| `include` | Include objects | `inventory.orders,inventory.event*` |
| `exclude` | Exclude objects | `inventory.tmp*` |
| `where_conditions` | Query conditions for snapshot migration and snapshot checking | `inventory.orders:"id > 100"` |
| `route` | Set complete destination names | `inventory.orders:archive.order_history` |
| `to_<kind>` | Dispatch mapping: choose the destination resource receiving the data; `to_topic` is the topic case | `to_topic=inventory.*:events,*.*:default_events` |

Include at least one resource `include` rule. Configuring only `route` or `exclude` does not select all tables. Omit `route` to retain source names.

## Include and exclude objects

```ini
[source.db]
include=inventory,tenant_*
exclude=tenant_private*

[source.table]
include=other.orders*,tenant_private.public_orders
exclude=inventory.tmp*,other.orders_archive*
```

This selects `inventory` and databases starting with `tenant_`, excluding those starting with `tenant_private`. The database exclusion also removes `tenant_private.public_orders`, despite its explicit table inclusion. Tables matching `inventory.tmp*` are excluded. `other.orders_archive` matches both table rules and is excluded.

All applicable `exclude` rules are checked first; any match excludes the object. Otherwise, any matching `include` rule selects it. Database/schema and table includes form a union, as do exclusions. Column rules only project columns of selected tables and cannot reselect an excluded table.

### Wildcards and regular expressions

Use these patterns in resource `include` / `exclude` settings. Each level of the name is matched separately.

| Pattern | Matches | Does not match |
| --- | --- | --- |
| `inventory.order*` | `inventory.order`, `inventory.orders`, `inventory.order2026` | `inventory.preorder` |
| `inventory.*log` | `inventory.log`, `inventory.audit_log` | `inventory.log_backup` |
| `inventory.order*log` | `inventory.orderlog`, `inventory.order_audit_log` | `inventory.orderlog_backup` |
| `inventory.*audit*` | `inventory.audit`, `inventory.daily_audit_log` | `inventory.orders` |
| `inventory.*` | Every table in `inventory` | Tables in other databases |
| `tenant_*.orders` | `tenant_.orders`, `tenant_eu.orders` | `other.orders` |
| `*.*` | Tables in all databases/schemas | — |
| `inventory.order?` | `inventory.order`, `inventory.order1` | `inventory.order12` |
| `inventory.r#order[0-9]+#` | `inventory.order1`, `inventory.order12` | `inventory.order`, `inventory.order1_backup` |

`*` matches zero or more characters; `?` matches zero or one. Put regular expressions between `r#` and `#`. For alternatives, use a group such as `inventory.r#(?:orders|events)[0-9]+#`.

### Names containing special characters

For names containing `*`, `?`, `.`, `,`, `:`, `+`, parentheses or other special characters, quote each name with the source database's identifier quotes to match it exactly.

| Database | Exact table named `order*` | A name containing a quote |
| --- | --- | --- |
| MySQL | `` `inventory`.`order*` `` | ``` `inventory`.`a``b` ``` represents ``a`b`` |
| PostgreSQL | `"inventory"."order*"` | `"inventory"."a""b"` represents `a"b` |
| SQL Server | `[market].[inventory].[order*]` | `[market].[inventory].[a]]b]` represents `a]b` |

```ini
[source.table]
include=`a.b,c:d`.`order*`,`inventory`.`r#literal#`
```

This selects the table `order*` in database `a.b,c:d`, and the table literally named `r#literal#` in `inventory`. Stars and `r#…#` inside identifier quotes are literal text.

### Select columns

```ini
[source.table]
include=inventory.orders,inventory.events

[source.column]
include=inventory.orders.id,inventory.orders.amount*,inventory.orders.secret
exclude=inventory.orders.secret,inventory.events.debug*
```

`orders` includes only `id` and columns starting with `amount`. Although `secret` is included by `include`, it is excluded by `exclude`. `events` has no column inclusion rule, so all columns except those starting with `debug` are included. Column selection applies to data migration and checking; it does not change destination table definitions.

## Select rows with WHERE conditions

```ini
[source.table]
include=inventory.orders,inventory.events
; Outer double quotes delimit a configuration string; escape inner quotes as \" and backslashes as \\.
where_conditions=inventory.orders:"id > 100 AND note IN ('keep\"quote','other')",inventory.events:"kind IN ('sale','refund')"
```

The outer double quotes belong to the configuration, not to SQL. Escaping follows JSON strings: `\"` represents a double quote and `\\` a backslash; `\n`, `\t` and `\uXXXX` are also supported. Inside the condition, use the source database's SQL syntax. A quoted PostgreSQL column can be written as `inventory.orders:"\"OrderId\" > 100"`.

Table names are exact, without wildcards or regex. If the same table appears more than once, the first condition applies. Conditions are not inherited: a condition on a database/schema does not automatically apply to its tables.

WHERE conditions apply to snapshot migration and snapshot data checking, not to individual CDC changes. Put these query conditions in `[source.table]`.

## Events, DDL, DCL and structure types

These sections accept exact type names in `include` / `exclude`. A standalone `include=*` selects all types; `include=` selects none. Do not mix `*` with individual type names. `exclude=*` does not exclude every type.

| Section | Default when `include` is absent | Common values |
| --- | --- | --- |
| `[source.event]` | All | `insert,update,delete` |
| `[source.ddl]` | None | `create_database,drop_database,alter_database,create_schema,drop_schema,alter_schema,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index` |
| `[source.dcl]` | None | `create_user,alter_user,create_role,drop_user,drop_role,grant,revoke,set_role` |
| `[source.structure]` | All | `database,table,constraint,sequence,comment,index,udf,udt,rbac,collection,shardkey` |
| `[source.cmd]` | All | Lowercase Redis command names, such as `set,del,flushall,flushdb` |

```ini
; Copy inserts and updates, excluding deletes.
[source.event]
include=*
exclude=delete

; Create and alter tables, excluding truncation.
[source.ddl]
include=create_table,alter_table,truncate_table
exclude=truncate_table

; Create users and grant permissions, excluding user deletion.
[source.dcl]
include=create_user,grant,drop_user
exclude=drop_user

; Migrate databases, tables and constraints, excluding ordinary standalone indexes.
[source.structure]
include=database,table,constraint,index
exclude=index

; Skip these commands in Redis CDC.
[source.cmd]
exclude=flushall,flushdb
```

Configure the sections needed by your task. DDL/DCL availability depends on the source, destination and task type. PostgreSQL CDC DDL also requires `ddl_meta_tb` and a capture trigger; see the [PostgreSQL template](../templates/pg_to_pg.md).

Primary keys and unique indexes migrate with tables; `index` controls ordinary standalone indexes. PostgreSQL sequences and comments migrate with table definitions; excluding `sequence` or `comment` separately does not remove them from those definitions. See [structure migration](structure/migration.md) for MongoDB structure types. MongoDB DDL names are `mongo_create_collection`, `mongo_drop_collection`, `mongo_rename_collection`, `mongo_drop_database`, `mongo_create_index`, `mongo_drop_index`, `mongo_coll_mod`, `mongo_shard_collection`, `mongo_reshard_collection`, and `mongo_refine_collection_shard_key`.

## Change destination names

`route` matches exact names and does not support wildcard routing. Databases/schemas, tables and columns can be renamed:

```ini
[source.db]
include=inventory,archive
route=inventory:archive,archive:history

[source.table]
route=inventory.orders:special.order_history

[source.column]
route=inventory.orders.id:special.order_history.order_id
```

| Source object | Destination object | Reason |
| --- | --- | --- |
| `inventory.orders` | `special.order_history` | Table rule takes precedence |
| `inventory.orders.id` | `special.order_history.order_id` | Column mapping |
| `inventory.audit` | `archive.audit` | Database rule |
| `archive.audit` | `history.audit` | Another database rule |

Each object is mapped once: `inventory.audit` does not continue on to `history.audit`. Unmapped names remain unchanged. A column mapping's destination database/table must agree with the table's route. Multiple source columns cannot map to the same destination column.

For PostgreSQL, use schema rules:

```ini
[source.schema]
include=inventory
route=inventory:archive
[source.table]
route=inventory.orders:archive.order_history
[source.column]
route=inventory.orders.id:archive.order_history.order_id
```

For SQL Server, include database, schema and table:

```ini
[source.db]
include=market
[source.schema]
route=market.inventory:warehouse.sales
[source.table]
route=market.inventory.orders:warehouse.sales.order_history
[source.column]
route=market.inventory.orders.id:warehouse.sales.order_history.order_id
```

Use `[source.collection]` for MongoDB collections, for example `route=inventory.orders:archive.order_history`. Use `[source.db]` for Redis database numbers, for example `include=0,2`.

Forward routing allows multiple source databases/schemas to share a destination. Snapshot sink monitoring uses the routed destination table name when the source is ambiguous. Checking and Kafka dispatch still require accurate source names; configure complete, distinguishable table routes for these features. Ambiguous reverse routes return an error.

## Dispatch mappings

**`to_<kind>` defines a dispatch mapping**, selecting the destination resource that receives the data. `kind` names that resource type; `to_topic` is the specific case where `kind=topic`. `route` changes resource names carried by the data, while `to_<kind>` selects where the data is sent. Both can be configured together.

Place the dispatch mapping in the source resource's `[source.<source_kind>]` section using `to_<kind>=source_path:destination_path`. The source engine determines `source_kind`; the destination engine determines `kind`. The kinds can differ: `[source.table] to_topic=inventory.orders:events.orders` sends data from the source table `inventory.orders` to the destination topic `events.orders`.

| Destination resource kind `<kind>` | INI key | Destination path example |
| --- | --- | --- |
| `db` (alias `database`) | `to_db` (or `to_database`) | `archive` |
| `schema` | `to_schema` | PostgreSQL: `sales`; SQL Server: `warehouse.sales` |
| `table` | `to_table` | MySQL / PostgreSQL: `archive.orders`; SQL Server: `warehouse.sales.orders` |
| `column` | `to_column` | MySQL / PostgreSQL: `archive.orders.id` |
| `collection` | `to_collection` | MongoDB: `archive.orders` |
| `topic` | `to_topic` | Kafka: `events.orders`, with dots belonging to the topic name |

Write the destination path according to the destination engine and resource kind. Known kinds unsupported by that destination engine are ignored.

Dispatch mappings follow these matching rules:

- Always use source names on the left, including when `route` is configured. Multiple source resources can share one destination resource.
- Match exact names first, then patterns consisting of complete trailing `*` segments. For a two-segment path, precedence is `inventory.orders`, `inventory.*`, then `*.*`, regardless of rule order.
- Patterns such as `order*`, `?` and regex are unsupported; only whole trailing `*` segments act as wildcards. A fallback should cover every trailing segment, such as `*.*` for tables.

The table describes the general configuration naming and path rules. Use also depends on the task's destination write support. **The built-in write flows currently use Kafka `to_topic`**; other `to_<kind>` mappings are not yet connected to destination writes. Use `route` for ordinary database, table and column renaming.

### Topology example: MySQL → Kafka → MySQL

This example copies data from a source MySQL instance through Kafka into another MySQL instance. The producer uses `route` to rename databases, tables and columns inside messages, and `to_topic` to dispatch those messages to three topics. Consumers write to destination tables using the names carried by the messages. Here `kind=topic` refers to Kafka, the immediate destination of DTS 1; the final MySQL database/table names travel inside the messages.

```mermaid
flowchart LR
    S["Source MySQL"] --> P["DTS 1: MySQL → Kafka"]
    P --> T1["Kafka: events.orders"]
    P --> T2["Kafka: events.inventory"]
    P --> T3["Kafka: events.default"]
    T1 --> C1["DTS 2: Kafka → MySQL"]
    T2 --> C2["DTS 3: Kafka → MySQL"]
    T3 --> C3["DTS 4: Kafka → MySQL"]
    C1 --> D["Destination MySQL"]
    C2 --> D
    C3 --> D
```

Create `events.orders`, `events.inventory` and `events.default` with **one partition each**. Each Kafka consumer task reads one partition of one topic, so this example runs **four DTS tasks: one producer and three consumers**. Sending everything to one single-partition topic instead requires one producer and one consumer.

**DTS 1: MySQL → Kafka snapshot producer.** Save this as `mysql_to_kafka.ini` and replace the connection settings:

```ini
[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://user:password@source:3306

[sinker]
db_type=kafka
sink_type=write
url=kafka-host:9092
with_field_defs=true

[source.db]
include=inventory,other
route=inventory:archive

[source.table]
route=inventory.orders:archive.order_history
to_topic=*.*:events.default,inventory.*:events.inventory,inventory.orders:events.orders

[source.column]
route=inventory.orders.id:archive.order_history.order_id

[parallelizer]
parallel_type=snapshot
parallel_size=4

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs/mysql_to_kafka
log_level=info
```

| Source table | Dispatch rule matched | Kafka topic | Consumer | Message table / final destination table |
| --- | --- | --- | --- | --- |
| `inventory.orders` | Exact table | `events.orders` | DTS 2 | `archive.order_history` |
| `inventory.audit` | `inventory.*` | `events.inventory` | DTS 3 | `archive.audit` |
| `other.orders` | `*.*` | `events.default` | DTS 4 | `other.orders` |

Create the corresponding tables in the destination MySQL instance beforehand. `inventory.orders.id` is named `order_id` in both the message and the destination table; unmapped columns retain their source names. The topic selects which consumer reads the message; the database and table names inside the message determine the final destination table.

This `to_topic` example follows the general dispatch rules above: source table names are matched exactly first, followed by database/schema wildcards and finally `*.*`. Although the message table has been renamed to `archive.order_history`, its original name `inventory.orders` still selects `events.orders`.

**DTS 2: Kafka → MySQL, consuming `events.orders`.** Save this as `orders_to_mysql.ini`:

```ini
[extractor]
db_type=kafka
extract_type=cdc
url=kafka-host:9092
group=orders_to_mysql
topic=events.orders
partition=0
offset=0
ack_interval_secs=1

[sinker]
db_type=mysql
sink_type=write
url=mysql://user:password@target:3306
batch_size=200

[parallelizer]
parallel_type=serial
parallel_size=1

[pipeline]
buffer_size=16000
checkpoint_interval_secs=10

[runtime]
log_dir=./logs/orders_to_mysql
log_level=info
```

Copy this file for the other two consumer tasks, changing only the fields below. All three consumers connect to the same destination MySQL instance:

| Task / configuration file | `[extractor] topic` | `[extractor] group` | `[runtime] log_dir` |
| --- | --- | --- | --- |
| DTS 2 / `orders_to_mysql.ini` | `events.orders` | `orders_to_mysql` | `./logs/orders_to_mysql` |
| DTS 3 / `inventory_to_mysql.ini` | `events.inventory` | `inventory_to_mysql` | `./logs/inventory_to_mysql` |
| DTS 4 / `default_to_mysql.ini` | `events.default` | `default_to_mysql` | `./logs/default_to_mysql` |

Start the three consumers before the producer. `partition=0` selects each topic's only partition, and `offset=0` reads from the start of the newly created topic. For existing topics, set the offset to the required recovery position. Configure separate consumer tasks for additional partitions.

The consumers use `extract_type=cdc` to keep reading Kafka messages; the producer remains a MySQL snapshot task. This example configures filtering, renaming and dispatch in the producer. Consumers write using the database, table and column names carried by the messages. Kafka subscriptions use `[extractor]` settings `topic`, `partition` and `offset`.

PostgreSQL → Kafka → PostgreSQL uses the same task topology. Use `[source.schema]` for producer namespace rules, and PostgreSQL engine types, connection settings and destination table definitions.
