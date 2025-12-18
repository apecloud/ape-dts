# Migrate data from Mongo to Mongo

# Prerequisites
- [prerequisites](./prerequisites.md)

- This article is for quick start, refer to [templates](/docs/templates/mongo_to_mongo.md) and [common configs](/docs/en/config.md) for more details.

# Prepare Mongo instances

## Source

```
docker run -d --name src-mongo \
    -p 27017:27017 \
    "$MONGO_IMAGE" --replSet rs0

-- enable and check oplog 
docker exec -it src-mongo mongosh --quiet --eval "rs.initiate()"
```

## Target

```
docker run -d --name dst-mongo \
	-e MONGO_INITDB_ROOT_USERNAME=ape_dts \
	-e MONGO_INITDB_ROOT_PASSWORD=123456 \
    -p 27018:27017 \
	"$MONGO_IMAGE"
```

# Migrate snapshot data
## Prepare data
```
docker exec -it src-mongo mongosh --quiet

use test_db;
db.tb_1.insertOne({ "name": "c", "age": "1", "_id": "1" });
db.tb_1.insertOne({ "name": "d", "age": "2", "_id": "2" });
db.tb_1.insertOne({ "name": "a", "age": "3" });
db.tb_1.insertOne({ "name": "b", "age": "4" });

db.tb_1.find();
```

```
[
  { _id: '1', name: 'c', age: '1' },
  { _id: '2', name: 'd', age: '2' },
  { _id: ObjectId("670cc7d95bace351d307453b"), name: 'a', age: '3' },
  { _id: ObjectId("670cc7d95bace351d307453c"), name: 'b', age: '4' }
]
```

## Start task
```
rm -rf /tmp/ape_dts
mkdir -p /tmp/ape_dts

cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://127.0.0.1:27017

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018

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
docker exec -it dst-mongo mongosh \
--host localhost --port 27017 --authenticationDatabase admin -u ape_dts -p 123456 \
--eval "db = db.getSiblingDB('test_db'); db.tb_1.find()"
```

```
[
  { _id: '1', name: 'c', age: '1' },
  { _id: ObjectId("670cc7d95bace351d307453b"), name: 'a', age: '3' },
  { _id: ObjectId("670cc7d95bace351d307453c"), name: 'b', age: '4' },
  { _id: '2', name: 'd', age: '2' }
]
```

# Check data
- check the differences between target data and source data

## Prepare data
- change target table records
```
docker exec -it dst-mongo mongosh \
--host localhost --port 27017 --authenticationDatabase admin -u ape_dts -p 123456 

use test_db;
db.tb_1.deleteOne({ "_id": "1" });
db.tb_1.updateOne({ "_id" : "2" }, { "$set": { "age" : 200000 } });
```

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mongo
extract_type=snapshot
url=mongodb://127.0.0.1:27017

[sinker]
db_type=mongo
sink_type=check
url=mongodb://ape_dts:123456@127.0.0.1:27018

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
{"schema":"test_db","tb":"tb_1","id_col_values":{"_id":"\"1\""}}
```
- cat /tmp/ape_dts/check_data_task_log/check/diff.log
```
{"schema":"test_db","tb":"tb_1","id_col_values":{"_id":"\"2\""},"diff_col_values":{"age":{"src":"2","dst":"200000"}}}
```

# Revise data
- revise target data based on "check data" task results

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://127.0.0.1:27017
check_log_dir=./check_data_task_log

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018

[filter]
do_events=*

[parallelizer]
parallel_type=mongo
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
docker exec -it dst-mongo mongosh \
--host localhost --port 27017 --authenticationDatabase admin -u ape_dts -p 123456 \
--eval "db = db.getSiblingDB('test_db'); db.tb_1.find()"
```

```
[
  { _id: ObjectId("670cc7d95bace351d307453b"), name: 'a', age: '3' },
  { _id: ObjectId("670cc7d95bace351d307453c"), name: 'b', age: '4' },
  { _id: '2', name: 'd', age: '2' },
  { _id: '1', name: 'c', age: '1' }
]
```

# Review data
- check if target data revised based on "check data" task results

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mongo
extract_type=check_log
url=mongodb://127.0.0.1:27017
check_log_dir=./check_data_task_log

[sinker]
db_type=mongo
sink_type=check
url=mongodb://ape_dts:123456@127.0.0.1:27018

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

# Cdc task

## Start task
```
cat <<EOL > /tmp/ape_dts/task_config.ini
[extractor]
db_type=mongo
extract_type=cdc
url=mongodb://127.0.0.1:27017
source=op_log

[filter]
do_dbs=test_db
do_events=insert,update,delete

[sinker]
db_type=mongo
sink_type=write
url=mongodb://ape_dts:123456@127.0.0.1:27018

[parallelizer]
parallel_type=mongo
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
docker exec -it src-mongo mongosh --quiet

use test_db;
db.tb_1.deleteOne({ "_id": "1" });
db.tb_1.updateOne({ "_id" : "2" }, { "$set": { "age" : 200000 } });
db.tb_1.insertOne({ "name": "b", "age": "5" });
```

## Check results
```
docker exec -it dst-mongo mongosh \
--host localhost --port 27017 --authenticationDatabase admin -u ape_dts -p 123456 \
--eval "db = db.getSiblingDB('test_db'); db.tb_1.find()"
```

```
[
  { _id: '2', name: 'd', age: 200000 },
  { _id: ObjectId("670cc7d95bace351d307453b"), name: 'a', age: '3' },
  { _id: ObjectId("670cc7d95bace351d307453c"), name: 'b', age: '4' },
  { _id: ObjectId("670ccb84b6456ba2539bb75a"), name: 'b', age: '5' }
]
```