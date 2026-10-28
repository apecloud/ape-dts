# Legacy config

Use this reference to maintain existing `[filter]` / `[router]` files. For task configuration, see the [configuration reference](config.md) and [filtering and routing](rules.md).

These sections are read only when the file contains **no `[source.*]` section**. Adding any `[source.*]` section, including an empty one, disables both `[filter]` and `[router]` in full. Migrate resource selection, event types, WHERE conditions and routing together.

## [filter]

| Config           | Description                                                          | Example                                                                                                                              | Default |
| ---------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| do_dbs           | databases to be synced                      | db_1,db_2*,db*&#                                                                                                                     | -       |
| ignore_dbs       | databases to be filtered                | db_1,db_2*,db*&#                                                                                                                     | -       |
| do_tbs           | tables to be synced                         | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_tbs       | tables to be filtered                   | db_1.tb_1,db_2*.tb_2*,db*&#.tb*&#                                                                                                    | -       |
| ignore_cols      | table columns to be filtered                                         | json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]                         | -       |
| do_events        | events to be synced                                                  | insert,update,delete                                                                                                                 | \*      |
| do_ddls          | ddls to be synced, for mysql cdc tasks                               | create_database,drop_database,alter_database,create_table,drop_table,truncate_table,rename_table,alter_table,create_index,drop_index | -       |
| do_dcls          | DCL statements to be synced, for supported structure tasks           | create_user,grant                                                                                                                    | -       |
| do_structures    | structures to be migrated in structure migration tasks               | mysql/pg: database,table,constraint,sequence,comment,index; mongo: collection,shardkey                                               | \*      |
| ignore_cmds      | commands to be filtered, for redis cdc tasks                         | flushall,flushdb                                                                                                                     | -       |
| where_conditions | where conditions for the source SELECT SQL during snapshot migration | json:[{"db":"db_1","tb":"tb_1","condition":"f_0 > 1"},{"db":"db_2","tb":"tb_2","condition":"f_0 > 1 AND f_1 < 9"}]                   | -       |

### Values

- All configurations support multiple items, which are separated by ",". Example: do_dbs=db_1,db_2.
- Set to `*` to match all. Example: `do_dbs=*`.
- Empty resource lists select no entries. Missing or empty `do_events` and `do_structures` retain their default of all types.
- `ignore_cols` and `where_conditions` are in JSON format and must start with `json:`.
- do_events takes one or more values from **insert**, **update**, and **delete**.
- do_dcls takes one or more values from **create_user**, **alter_user**, **create_role**,
  **drop_user**, **drop_role**, **grant**, **revoke**, and **set_role**.
- `do_structures` takes structure object types. For MySQL/PostgreSQL, common values include
  **database**, **table**, **constraint**, **sequence**, **comment**, and **index**. For MongoDB,
  supported values are **collection**, **shardkey**. MongoDB does not use a separate
  **database** structure type; databases are created implicitly by creating collections. **shardkey**
  copies source sharding definitions for sharded collections and runs only when the target is
  connected through `mongos`.

### Priority

- `ignore_dbs` and `ignore_tbs` form a union and override all `do_dbs` and `do_tbs` rules.
- Otherwise, any matching database/schema or table inclusion selects the object.

### Wildcard

| Wildcard | Description                 |
| -------- | --------------------------- |
| \*       | Matches zero or more characters |
| ?        | Matches 0 or 1 characters   |

Used in: do_dbs, ignore_dbs, do_tbs, and ignore_tbs.

### Escapes

| Database | Before      | After               |
| -------- | ----------- | ------------------- |
| mysql    | db\*&#      | \`db\*&#\`          |
| mysql    | db*&#.tb*$# | \`db*&#\`.\`tb*$#\` |
| pg       | db\*&#      | "db\*&#"            |
| pg       | db*&#.tb*$# | "db\*&#"."tb\*$#"   |

Names should be enclosed in escape characters if there are special characters.

Used in: do_dbs, ignore_dbs, do_tbs and ignore_tbs.

## [router]

| Config    | Description                                                         | Example                                                                      | Default |
| --------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------- |
| db_map    | database mapping                                                    | db_1:dst_db_1,db_2:dst_db_2                                                  | -       |
| tb_map    | table mapping                                                       | db_1.tb_1:dst_db_1.dst_tb_1,db_1.tb_2:dst_db_1.dst_tb_2                      | -       |
| col_map   | column mapping                                                      | json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}] | -       |
| topic_map | table -> kafka topic mapping, for mysql/pg -> kafka tasks. required | \*.\*:default_topic,test_db_2.\*:topic2,test_db_2.tb_1:topic3                | -       |

### Values

- A mapping rule consists of the source and target, which are separated by ":".
- All configurations support multiple items, which are separated by ",". Example: db_map=db_1:dst_db_1,db_2:dst_db_2.
- col_map value is in JSON format and must start with `json:`.
- If not set, data will be routed to the same databases/tables/columns with the source database.

### Priority

- tb_map > db_map.
- col_map only works for column mapping. If a table needs database + table + column mapping, tb_map/db_map must be set.
- topic_map: test_db_2.tb_1:topic3 > test_db_2.\*:topic2 > \*.\*:default_topic.

### Wildcard

Database, table and column mappings use exact names. `topic_map` supports whole trailing `*` segments such as `db.*` and `*.*`.

### Escapes

Use the same identifier quoting as `[filter]`.


## Legacy migration

| Legacy field | Configuration |
| --- | --- |
| filter do_dbs / ignore_dbs | `include / exclude` in the top-level resource section (`source.db` for MySQL, `source.schema` for PG) |
| do_tbs / ignore_tbs | `source.table` include / exclude |
| ignore_cols JSON | `source.column` exclude with full column paths |
| do_events / do_ddls / do_dcls / do_structures / ignore_cmds | common section include / exclude |
| where_conditions JSON | `source.table` where_conditions with quoted SQL strings |
| router db_map / tb_map / col_map | corresponding resource route |
| router topic_map | `to_topic` in `[source.table]`, the topic case of general `to_<kind>` dispatch mappings |

Include the schema when migrating MSSQL's old two-segment table names. Do not keep legacy filter settings while adding only new routing sections: the entire configuration mode changes.
