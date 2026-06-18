# Migrate structures

- Database: MySQL, PG, MongoDB.
- Migrated Objects:
  - MySQL/PostgreSQL: database(mysql), schema(pg), table, comment, index, sequence(pg), constraints.
  - MongoDB: collection, index, shardkey.

# Example: MySQL -> MySQL / Mongo -> Mongo

Refer to [MySQL task templates](../../templates/mysql_to_mysql.md), [MySQL tutorial](../tutorial/mysql_to_mysql.md),
[Mongo task templates](../../templates/mongo_to_mongo.md), and [Mongo tutorial](../tutorial/mongo_to_mongo.md).

## Note

Structure migration is executed serially in a single thread. Notice the following configurations:

```
[extractor]
extract_type=struct

[sinker]
sink_type=struct
batch_size=1

[parallelizer]
parallel_type=serial
parallel_size=1
```

Failure strategy: interrupt(default), ignore.

- interrupt: If a particular migration fails, the entire task will be terminated immediately.

- ignore: If a migration fails, it will not affect the migration of other schemas, and the process will continue. However, the failure will be logged as an error.

```
[sinker]
conflict_policy=interrupt
```

# Phased migration

In a complete data migration process that includes both structure migration and data migration, the task will be divided into three stages in order to accelerate data migration:

1. Migrate table structures + primary/unique keys ( necessities for data migration);
2. Data migration;
3. Migrate indexes + constraints.

Thus, we offer 2 types of filtering:

## Migrate table structures + primary/unique keys

```
[filter]
do_structures=database,table
```

## Migrate indexes and constraints

```
[filter]
do_structures=constraint,index
```

## MongoDB structure types

```
[filter]
do_structures=collection,shardkey
```

MongoDB does not use a separate `database` structure type. `collection` creates selected
collections and copies collection options, `shardkey` copies source
sharding definitions. For sharded targets, connect through `mongos`; DTS runs `enableSharding`
before `shardCollection` when needed. If the target is not `mongos`, shard key statements are
ignored.
