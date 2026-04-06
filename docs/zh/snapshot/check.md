# 数据校验

数据迁移完成后，需要对源数据和目标数据进行逐行逐列比对。如果数据量过大，可以进行抽样校验。请确保需要校验的表具有主键/唯一键。

支持对 MySQL、PostgreSQL、MongoDB 进行比对。

目前 `sample_interval` 采样仅支持 MySQL / PostgreSQL 全量校验。

数据校验当前按三种形态进行文档说明：

## 校验形态

### Standalone snapshot check

- 使用 `extract_type=snapshot`。
- 设置 `sink_type=dummy`，或直接省略 `[sinker]`。
- 在 `[checker]` 中显式配置校验目标。
- 使用 `parallel_type=rdb_check`。

```text
源端数据
   |
   v
[extractor snapshot]
   |
   v
[checker] ---- 查询目标端 ----> [checker target]
   |
   +---- 一致 -----------> 下一批
   |
   `---- 不一致 ---------> retry / miss.log / diff.log
```

### Inline snapshot check

- 使用 `extract_type=snapshot` 且 `[sinker] sink_type=write`。
- checker 会在写入后执行，并直接复用 `[sinker]` 已解析的目标端配置。
- `[checker]` 不接受 `db_type`、`url`、`username`、`password`。
- 当前仅支持 `[sinker].db_type` 为 `mysql`、`pg`、`mongo`。
- 并发策略沿用 snapshot 链路（通常为 `parallel_type=snapshot`）。

```text
源端数据
   |
   v
[extractor snapshot]
   |
   v
[sinker 写入一批] -------> [目标端]
   |
   v
[checker 校验同一批、同一目标]
   |
   +---- 一致 -----------> 下一批
   |
   `---- 不一致 ---------> retry ---------> 用尽后写 miss.log / diff.log
```

- 更像“写后校验 + 短期收敛等待”。
- 先 retry，只有 retry 用尽后才会落 miss/diff。
- 不维护长期不一致 store。

### Inline cdc check

- 使用 `extract_type=cdc` 且 `[sinker] sink_type=write`。
- checker 会在变更写入目标端后校验 CDC 已落库数据。
- checker 直接复用 `[sinker]` 已解析的目标端配置。
- `[checker]` 不接受 `db_type`、`url`、`username`、`password`。
- 必须通过 `[resumer] resume_type=from_target` 或 `from_db` 持久化 checker 状态。
- 当前仅支持 `[sinker].db_type` 为 `mysql` 或 `pg`。
- 使用 `parallel_type=rdb_check`。

```text
源端 CDC events
      |
      v
[extractor cdc]
      |
      v
[sinker 写入一批 event] --> [目标端]
      |
      v
[checker 校验同一批、同一目标]
      |
      +---- 一致 -----------> 下一批 / checkpoint
      |
      `---- 不一致 ---------> checker state/store
                                   |
                                   +--> 后续 event 可能抵消旧 miss/diff
                                   `--> 与 resumer / checkpoint 状态一起持久化
```

- 更像“持续对账”。
- 不一致会进入 checker state/store，而不是只走短 retry。
- checkpoint / state store 与 checker 生命周期耦合更深。
- 运行时错误按单次操作处理：记录错误日志，不影响主写入链路，并继续处理后续 checker 消息。
- checkpoint / 元数据刷新这类控制信号与 checker DML 积压解耦，不会因为排队批次过多而阻塞主链路。

#### Inline cdc check 的配置约束

会直接报配置错误（`ConfigError`）的情况：

- 未启用 `[checker]`，但 `[parallelizer] parallel_type=rdb_check`。
- `[pipeline] pipeline_type` 不是 `basic`。
- `[extractor] extract_type=cdc`，但 `[sinker] sink_type` 不是 `write`。
- `[sinker].db_type` 不是 `mysql` 或 `pg`。
- 在 `[checker]` 中配置了 `db_type`、`url`、`username`、`password`。
- 未配置 `[resumer] resume_type=from_target` 或 `from_db`，导致 checker 无法持久化状态。

会继续生效或被强制改写的配置：

- `[checker].batch_size`：继续生效，且不会 fallback 到 `[sinker].batch_size`。
- `[checker].queue_size`：按待处理 checker DML 批次数计数；队列满时会丢弃最旧的待校验批次并记录
  warning 日志。checkpoint、`refresh_meta` 等控制信号会绕过这条队列。
- `[checker].max_retries` 与 `[checker].retry_interval_secs`：inline cdc check 下强制按 0 处理。

## 示例: MySQL -> MySQL

参考 [任务模版](../../templates/mysql_to_mysql.md) 和 [教程](../../en/tutorial/mysql_to_mysql.md)。模板中已将 standalone snapshot check、inline snapshot check、inline cdc check 分开列出。

### 抽样校验（仅 MySQL / PostgreSQL 全量校验）

对 MySQL / PostgreSQL 的全量校验，可在 `[extractor]` 中添加 `sample_interval` 配置。例如设置 `sample_interval=3` 表示每 3 条记录采样 1 次。
```
[extractor]
sample_interval=3
```

## 限制

- 数据校验为源端驱动（仅验证 Source ∈ Target），无法发现目标端多余数据（幽灵数据）。
  如需检测目标端多余数据，可通过 [反向校验](#反向校验) 交换源/目标角色。
- 对于 MongoDB，`_id` 应为可哈希类型（例如 ObjectId/String/Int32/Int64）。若某行 `_id` 无法参与哈希计算，该行会被跳过并计入 `summary.log.skip_count`；若拉取到的目标端行含有不可哈希的 `_id`，校验会失败。

## DELETE 事件校验（inline cdc check）

在 inline cdc check 中，checker 会校验 DELETE 事件：通过主键在目标端查询，若目标端仍
存在该行则判定为不一致，记录到 `diff.log`（`diff_col_values` 为空）。开启
`output_revise_sql=true` 时，会自动生成对应的 `DELETE` 修复语句写入 `sql.log`。

# 校验结果

`diff.log`、`miss.log`、`summary.log` 以 JSON 格式写入；`sql.log` 保存生成的修复 SQL。默认写入 `runtime.log_dir/check`；若配置了 `[checker].check_log_dir`，则写入该目录。

## 差异日志（diff.log）

差异日志包括库（schema）、表（tb）、主键/唯一键（id_col_values）、差异列的源值和目标值（diff_col_values）。

```json
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"5"},"diff_col_values":{"f_1":{"src":"5","dst":"5000"},"f_2":{"src":"ok","dst":"after manual update"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"4"},"diff_col_values":{"f_1":{"src":"2","dst":"1"}}}
{"schema":"test_db_1","tb":"one_pk_no_uk","id_col_values":{"f_0":"6"},"diff_col_values":{"f_1":{"src":null,"dst":"1","src_type":"None","dst_type":"Short"}}}
```

当源端与目标端的类型不同（如 Int32 对 Int64，或 None 对 Short）时，`src_type`/`dst_type` 会出现在对应列下，明确标出类型不一致。MongoDB 也适用这一规则，差异日志会输出 BSON 类型名称。

只有在路由对 schema 或 table 进行重命名时，日志才会补充 `target_schema`/`target_tb` 来标识目的端真实库表。`schema`、`tb` 依旧表示源端，方便排查。

## 缺失日志（miss.log）

缺失日志包括库（schema）、表（tb）和主键/唯一键（id_col_values）。由于缺失记录不存在差异列，因此不会输出 `diff_col_values`。

```json
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":"8","f_2":"1"}}
{"schema":"test_db_1","tb":"no_pk_one_uk","id_col_values":{"f_1":null,"f_2":null}}
{"schema":"test_db_1","tb":"one_pk_multi_uk","id_col_values":{"f_0":"7"}}
```

## 输出完整行

当需要完整行内容用于排查问题时，可在 `[checker]` 中开启全行日志。对于 standalone
snapshot check，需要在 `[checker]` 中显式配置校验目标；对于 inline snapshot check 与
inline cdc check，checker 会直接复用 `[sinker]` 已解析的目标端配置：

```
[checker]
output_full_row=true
```

开启后，所有 `diff.log` 条目都会追加 `src_row` 与 `dst_row`，所有 `miss.log` 条目都会追加 `src_row`（当前支持 MySQL、PostgreSQL、MongoDB）。示例：

```json
{
  "schema": "test_db_1",
  "tb": "one_pk_multi_uk",
  "id_col_values": {
    "f_0": "5"
  },
  "diff_col_values": {
    "f_1": {
      "src": "5",
      "dst": "5000"
    },
    "f_2": {
      "src": "ok",
      "dst": "after manual update"
    }
  },
  "src_row": {
    "f_0": 5,
    "f_1": 5,
    "f_2": "ok"
  },
  "dst_row": {
    "f_0": 5,
    "f_1": 5000,
    "f_2": "after manual update"
  }
}
```

## 输出修复 SQL

如需人工修复差异数据，可在 `[checker]` 中开启 SQL 输出。对于 standalone snapshot
check，需要在 `[checker]` 中显式配置校验目标；对于 inline snapshot check 与 inline cdc
check，checker 会直接复用 `[sinker]` 已解析的目标端配置：

```
[checker]
output_revise_sql=true
# 可选：强制使用全字段匹配 WHERE 条件
revise_match_full_row=true
```

开启后，缺失记录的 `INSERT` 语句与差异记录的 `UPDATE` 语句会被写入 `sql.log`。

当 `revise_match_full_row=true` 时，即使表存在主键也会使用整行数据生成 WHERE 条件，以便通过完整行值定位目标数据。

若路由没有对 schema 或 table 改名，则不会输出 `target_schema`/`target_tb`。这两个字段仅在路由改名时用于确定 SQL 应执行的目标表。

生成的 SQL 直接使用真正的目的端 schema/table，可以直接在目标端执行。路由改名时可参考 `target_schema`/`target_tb` 判断最终目标对象。

示例：

```json
{
  "schema": "test_db_1",
  "tb": "one_pk_no_uk",
  "target_schema": "target_db",
  "target_tb": "target_tb",
  "id_col_values": {"f_0": "4"},
  "diff_col_values": {"f_1": {"src": "2", "dst": "1"}}
}
```

`sql.log` 示例：

```sql
UPDATE `target_db`.`target_tb` SET `f_1`='2' WHERE `f_0` = 4;
```

缺失记录日志示例：

```json
{
  "schema": "test_db_1",
  "tb": "test_table",
  "id_col_values": {"id": "3"}
}
```

`sql.log` 示例：

```sql
INSERT INTO `test_db_1`.`test_table`(`id`,`name`,`age`,`email`) VALUES(3,'Charlie',35,'charlie@example.com');
```

## 概览日志（summary.log）

概览日志包含校验的总体结果，如 start_time、end_time、is_consistent，以及 miss、diff、跳过行数（`skip_count`）和生成修复 SQL 数量（`sql_count`）。

`skip_count` 用于记录被 checker 跳过的行，例如行主键/唯一键无法参与哈希计算时。若没有跳过任何行，则该字段不会出现在日志中。

在 inline cdc check 中，`summary.log` 还会包含 `tables` 字段，用于记录每张表的
miss/diff 计数；非 CDC 任务不会输出该字段。

```json
{"start_time": "2023-09-01T12:00:00+08:00", "end_time": "2023-09-01T12:00:01+08:00", "is_consistent": false, "miss_count": 1, "diff_count": 2, "skip_count": 1, "sql_count": 3}
```

Inline cdc check 示例：

```json
{"start_time":"2023-09-01T12:00:00+08:00","end_time":"2023-09-01T12:05:00+08:00","is_consistent":false,"miss_count":1,"diff_count":2,"skip_count":1,"tables":{"test_db_1.test_tb":{"miss_count":1,"diff_count":2}}}
```

# 反向校验

数据校验为源端驱动，只验证源端数据是否存在于目标端。若需检测目标端中多余的数据
（源端不存在），可通过交换源/目标角色，执行一组 standalone snapshot check：

```
# 原始：源端=A，目标端=B
# 反向：源端=B，目标端=A
[extractor]
db_type=<原 checker 的 db_type>
url=<原 checker 的 url>

[checker]
db_type=<原 extractor 的 db_type>
url=<原 extractor 的 url>
```

# 配置

`[checker]` 的完整配置与目标选择规则请参考 [config.md](../config.md)。

## 重试机制

当 `max_retries > 0` 时，checker 会在检测到不一致时自动重试：
- 重试期间不记录日志，避免噪音
- 仅在最后一次检查时记录详细的 miss/diff 日志
- 适用于目标端数据尚未完全同步的场景

> **注意：** inline cdc check 下不支持重试。CDC 事件是流式到达的，后续的 DELETE
> 事件可能会移除已正确写入的数据，导致重试队列中出现误报。即使配置了 `max_retries`
> 和 `retry_interval_secs`，CDC 模式下也会被强制忽略（设为 0），并输出警告日志。

## 路由

数据校验支持 `[router]` 配置，详情请参考 [config.md](../config.md)。

## 集成测试参考

参考各类型集成测试的 `task_config.ini`：
- dt-tests/tests/mysql_to_mysql/check
- dt-tests/tests/pg_to_pg/check
- dt-tests/tests/mongo_to_mongo/check
