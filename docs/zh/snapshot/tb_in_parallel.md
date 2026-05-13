# 全量任务多表并行

如果全量任务涉及多张表，默认情况下，ape-dts 会按照 **先库名后表名** 的方式排序并逐表迁移，同一时刻只有一张表处于迁移中。

如果你的资源（内存，CPU）充足，可以开启多表并行以加快全局速度。

## 配置
- 添加以下配置后，ape-dts 会最多同时运行 4 个表级 worker；当某张表完成后，会从剩余表中继续补上一张，维持最多 4 张表并行。

```
[extractor]
parallel_type=table
parallel_size=4
```

`parallel_type=table` 表示把全量并发度分配给多张表；`parallel_type=chunk` 表示把并发度分配给单表内的 chunk 切分。这两种模式下，真正控制全量并发度的都是 `[extractor].parallel_size`。

废弃配置说明：`[runtime] tb_parallel_size` 已废弃，不应再用于新配置。当前仅为兼容旧版本配置而保留，且只会在未设置 `[extractor] parallel_size` 时作为 fallback 生效。

旧配置示例：

```ini
[runtime]
tb_parallel_size=4
```

## 和 [parallelizer] parallel_size 的区别
- 全量任务中，`[extractor].parallel_size` 控制源端全量拉取并发度。
- `[parallelizer].parallel_size` 控制数据拉取完成后的目标端下游并发写入。

```
[parallelizer]
parallel_type=snapshot
parallel_size=8
```

## 适用范围
- 全量迁移（源端：MySQL, Postgres, MongoDB）
- 全量校验（源端：MySQL, Postgres, MongoDB）
