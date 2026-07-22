# 错误码

Ape-DTS 任务运行时错误使用稳定的五位条件码。错误码用于说明发生了什么；错误发生
的位置通过独立的阶段（stage）记录。在 v1 中，`dt-main` 和 `dtscli` 都通过相同的
面向用户的错误报告边界输出失败信息。

例如：

```text
ERROR [MD001]: A required database object was not found
TASK: sync-orders-01
AFFECTED: destination postgres sales.orders
HINT: Check object routing and create the required object or enable structure initialization.
```

`MD001` 是该错误唯一的身份标识。阶段是独立的诊断元数据。应用逻辑必须比较类型化的
`ErrorCode`，禁止解析错误消息，也禁止将错误码和阶段组合成新的标识。

## 兼容性规则

- 错误码由五个大写 ASCII 字母或数字组成。
- 错误码只允许追加，禁止将已有错误码复用于其他错误条件。
- 以 `000` 结尾的类别码为保留码，不得实际输出。
- 阶段不属于稳定的错误身份。
- stage 和 endpoint 由执行边界显式挂载；报告层禁止根据错误码、provider 或模块名推导。
- 重试、跳过、降级和 DLQ 行为属于策略层，不属于错误码契约。

错误码目录在 `dt-common::error` 中集中声明一次，并由该声明生成枚举、序列化错误码
映射、默认消息、默认建议和完整错误码列表。新增错误码必须加入该声明，禁止通过多个
并行的 `match` 分支分别维护。

## 错误码目录

| 错误码 | 条件 | 默认含义 |
|---|---|---|
| `CF001` | `MissingConfig` | 缺少必要的配置文件 |
| `CF002` | `InvalidConfig` | 配置无效 |
| `CF003` | `MissingConfigItem` | 缺少必要的配置项 |
| `CN001` | `ConnectionFailed` | 无法建立端点连接，或者连接已经中断 |
| `CN002` | `ConnectionTimeout` | 获取连接池连接超时 |
| `CN003` | `TlsFailed` | 无法建立安全的数据库连接 |
| `AU001` | `AuthenticationFailed` | 凭据被端点拒绝 |
| `AU002` | `PermissionDenied` | 已认证账号缺少必要权限 |
| `PR001` | `PrerequisiteNotMet` | 必要的版本、CDC 配置、slot 或能力不可用 |
| `PR002` | `UnsupportedDatabaseVersion` | 数据库版本不受支持 |
| `PR003` | `CdcNotEnabled` | 源数据库未启用 CDC 所需配置 |
| `PR004` | `ReplicationCapacityExhausted` | 没有可用的复制 slot 或 sender 容量 |
| `PR005` | `UnsupportedTableStructure` | 表结构不受支持 |
| `MD001` | `ObjectNotFound` | 必要的表、topic 或其他端点对象不存在 |
| `MD002` | `DatabaseNotFound` | 数据库不存在 |
| `MD099` | `MetadataReadFailed` | 无法读取迁移所需的数据库系统信息 |
| `DB001` | `StatementFailed` | 源端或目标端操作失败 |
| `IC001` | `IntegrityViolation` | 数据违反约束 |
| `ST001` | `CheckpointReadFailed` | 无法读取 checkpoint 状态 |
| `IO001` | `IoFailed` | I/O 操作失败 |
| `RT001` | `WorkerFailed` | 任务 worker 异常终止 |
| `RT002` | `OperationInterrupted` | 请求的操作被中断 |
| `IN001` | `InvariantViolated` | 内部不变量被破坏 |
| `IN999` | `Unclassified` | 暂时无法进行稳定分类 |

## 诊断阶段目录

| 阶段 | 含义 |
|---|---|
| `bootstrap` | 加载任务配置 |
| `precheck` | 检查迁移前置条件 |
| `extractor` | 从源端读取数据 |
| `parallelizer` | 准备迁移任务 |
| `pipeline` | 处理迁移数据 |
| `sinker` | 向目标端写入数据 |
| `checker` | 校验迁移数据 |
| `resumer` | 恢复已保存的任务进度 |
| `task` | 运行迁移任务 |
| `unknown` | 没有可用的结构化阶段信息 |

## 用户视图和诊断视图

`ErrorReport` JSON 是带版本的机器接口，当前 `schema_version` 为 `1`。新增可选字段属于
兼容变更；删除、重命名用户字段，或者改变已有用户字段的含义时，必须提升 schema
版本。CLI 文本格式不是稳定的机器接口。

`ErrorReport` JSON 视图只包含面向用户的字段：稳定错误码、用户消息、详细信息、处理
建议、任务 ID、端点角色和受影响对象。文本视图先输出这些字段，然后输出诊断部分。

内存中的报告还包含 `stage`、`origin`、脱敏后的 `error_chain`、`context_count`，以及
可选的已捕获 `backtrace`。这些字段不会写入 JSON。
`ErrorReport` 的文本输出始终在用户信息后输出全部可用诊断字段。API 使用方必须使用
带版本的 JSON 视图，不得解析 CLI 文本。

组件提供的 `message` 和 `hint` 保持自由文本。`detail` 由 `ErrorReport` 按从外到内的
顺序组合普通 `anyhow::Context` 和具体 cause，并去除重复文本。报告边界使用
`rtb-redact` 对凭据、带认证信息的 URL、authorization 值、provider token、JWT、长随机
token 和私钥进行脱敏，之后这些内容才能进入用户视图。

CLI 错误默认包含诊断部分：

```text
DIAGNOSTIC [MD001]
STAGE: sinker
ENDPOINT: destination
ORIGIN: postgres/42P01
CONTEXT 1: starting task
CAUSE 1: relation missing
BACKTRACE:
...
```

诊断信息可能包含 SQL、行数据、对象名、provider 消息和 `anyhow::Context`。符合凭据
特征的值和带认证信息的 URL 会被脱敏，但 stderr 和采集后的日志仍应作为敏感数据处理。
进程不会自行开启 backtrace。`RUST_LIB_BACKTRACE=0` 会关闭错误 backtrace，设置为任意
其他值时开启。该变量未设置时，按相同规则读取 `RUST_BACKTRACE`。当前错误报告始终使用
简短格式输出已捕获的 backtrace。没有捕获到 backtrace 时，结构化诊断和错误链仍会
完整输出，只省略 `BACKTRACE` 区块。

## 结构化错误

`anyhow::Error` 是唯一的错误传输容器。`DtErrorContext` 是类型化 metadata frame，
其中的错误码、消息、处理建议、阶段、任务 ID、端点角色、数据库对象和 provider
origin 都是可选字段。它实现 `Display`，但刻意不实现
`std::error::Error`。端点角色包括 `source`、`destination` 和 `metadata`。

错误链最内层始终是真正实现 `Error` 的 cause。Provider 失败直接保留原始 provider
错误；应用主动产生的失败使用基于 `thiserror` 的 `DtError` 枚举。

项目主动失败先创建根因，再通过 `DtErrorContextExt` 的 fluent 方法逐层挂载 metadata，
例如 `DtError::ConfigError(detail).with_code(ErrorCode::InvalidConfig)`。同一 extension trait
也为 `anyhow::Error` 和已支持的 provider 错误类型实现。未知的具体错误只允许在组件边界
通过 `DtErrorContext::attach` 挂载。

`DtError` variant 只描述项目自有的具体根因，不隐含也不分类 `ErrorCode`。同一个 variant
可能对应多个错误码，因此失败点必须显式挂载业务错误码；禁止增加根据 variant 猜测
错误码的 `DtError` classifier。

Metadata 在错误跨越所有权边界时逐层挂载。叶子 frame 可以提供业务错误码、受影响对象
和 provider origin。stage、endpoint 和 task ID 在仍然明确知道其含义的最高层边界显式
挂载：例如 extractor worker 挂载 `extractor/source`，统一 sinker adapter 挂载
`sinker/destination`，recovery 初始化边界挂载 `resumer/metadata`，precheck 入口挂载
`precheck` 和 task ID，而 precheck builder 在各 checker 调用外层挂载 `source` 或
`destination`。普通任务的 task ID 只在任务入口挂载。报告阶段不做推导。

Frame 挂载后不再修改。`ErrorReport` 从最外层 frame 开始递归读取 metadata 父链；每个
字段使用第一个非空的外层值，外层为空时才继承内层值。这是无条件的 outer-wins 语义，
不是 fill-if-absent 修改：例如外层挂载的 `sinker/destination` 会在最终报告中覆盖内层的
stage 或 endpoint。需要保留内层 scope 的边界不得再挂载竞争字段。`DtErrorContext` 不包含
detail 字段；报告构建时从已脱敏的普通 context 和具体 cause 生成 detail。项目自有 `DtError`
会保留 variant 对应的完整 `Display` 文本和类型化 payload，与 provider error 保留自身
`Display`、source chain 和具体类型的方式一致。

组件边界先借用原始 provider 错误完成分类，再通过 `DtErrorContext::attach` 挂载。
得到的 `anyhow::Error` 既能 downcast 到 `DtErrorContext`，也能 downcast 到原始
provider 错误类型。provider 错误一旦擦除为 `anyhow::Error` 就不得再次分类；分类必须
发生在仍持有具体 provider 类型的第一个边界。

`ErrorReport` 是面向用户的边界表示。其文本格式使用 `ERROR`、`TASK`、`AFFECTED`、
`DETAIL`、`HINT` 和 `DIAGNOSTIC` 等行。

Provider 分类器统一位于 `dt-common::error::provider`。分类器根据 provider 原始错误码、
类型化错误种类和 Rust 错误变体进行判断，禁止解析 provider 错误消息。
`ProviderErrorClassification::into_context` 生成包含可选精确错误码、provider origin 和
受影响对象的 frame。组件边界先挂载当前操作的默认错误码，再挂载 provider frame；
因此精确的 provider 分类会覆盖默认码，未识别的 provider 错误会继承默认码。stage 和
endpoint 位于这两层之外，原始 provider 错误保持不变。

Provider 相关元数据应放在组件边界的小型 wrapper 中。组件边界负责设置操作默认错误
码。Provider classifier 不接收业务默认码；无法识别时返回空 code 并继承边界默认码。
stage、endpoint 和 task ID 在 provider 分类之外、仍明确知道其语义的最高层执行边界
显式挂载。

每个 crate 必须将组件 wrapper 统一放在唯一的 `src/error_boundary.rs` 中，并通过
`extractor_error`、`sinker_error` 或 provider 等内部模块区分所有权。禁止继续在业务模块旁新增
组件错误 helper 文件。`error_boundary` 表示将底层失败分类或补充为 Ape-DTS 错误的
边界，与 `dt-common::error` 中的公共错误契约相互独立。公共 provider 分类器仍统一
位于 `dt-common::error::provider`。

`ErrorReport` 只读取类型化 context 和错误链，不再尝试 provider 分类。没有
`DtErrorContext` 的错误到达报告层时稳定输出 `IN999`。遗漏的分类位置通过失败路径测试
和代码评审暴露。

`MD099 MetadataReadFailed` 只作为读取或解析端点 catalog、控制面元数据失败时的
默认码。Schema/结构本身是迁移对象，并不代表所有结构错误都是元数据读取错误。对象
不存在使用 `MD001`/`MD002`，结构不受支持使用 `PR005`，版本或拓扑前置条件不满足
使用 `PR001`/`PR002`，目标端拒绝 DDL 使用 `DB001`。

错误分类必须依据用户能够检查或修改什么，而不是依据发现错误的内部模块或数据结构。
找不到 schema、table 或 column 时使用 `MD001`，并填写受影响对象；CDC row event 缺少
对应的 table/relation definition 时使用 `DB001`，并提示如何从更早的日志位置重启；
Redis slot map 不完整时使用 `PR001`，并提示先稳定集群拓扑。只有不存在合理的端点、
配置或迁移侧处理方式时才使用 `IN001`。cache entry、OID map 或 Rust type 等内部术语
不得成为唯一解释；用户消息和处理建议必须说明受影响的端点行为与下一步动作。
`DETAIL` 可以保留 table ID、relation ID 等有助于关联 provider 日志的标识。

## Provider 原始错误保留

Provider 错误码属于诊断信息，不是 Ape-DTS 错误码。只要能够获取，就应保存在
`origin` 中。首批 SQLx 映射如下：

| Provider 错误 | Ape-DTS 错误码 |
|---|---|
| PostgreSQL `42P01`/`42703`/`42704`，MySQL `1054`/`1146` | `MD001` |
| PostgreSQL `3D000`，MySQL `1049` | `MD002` |
| PostgreSQL SQLSTATE 类别 `28`，MySQL `1045` | `AU001` |
| PostgreSQL `42501`，MySQL `1044`/`1142`/`1143`/`1227`/`1370` | `AU002` |
| PostgreSQL SQLSTATE 类别 `08` | `CN001` |
| PostgreSQL SQLSTATE 类别 `23` | `IC001` |
| MySQL `2002`/`2003`/`2006`/`2013` | `CN001` |
| SQLx 连接配置错误 | `CF002` |
| SQLx I/O、协议错误或连接池已关闭 | `CN001` |
| SQLx 连接池超时 | `CN002` |
| SQLx TLS 错误 | `CN003` |
| SQLx worker 崩溃 | `RT001` |
| SQLx 完整性错误种类 | `IC001` |

每个 SQLx 边界还会提供与操作相关的默认错误码，例如 `CN001`、`DB001` 或 `ST001`。
精确的 provider 分类会覆盖该默认码，无法识别时则继承默认码；两种情况下都必须保留
provider 原始错误码、constraint/table 元数据和源错误链。

`tokio-postgres` 复制适配器使用相同的 PostgreSQL SQLSTATE 规则。URL 解析错误使用
`CF002`，连接建立失败使用 `CN001`，复制命令被拒绝时使用该操作的边界默认错误码。

其他首批 provider 映射如下：

| Provider 错误 | Ape-DTS 错误码 |
|---|---|
| Redis 认证错误 / `NOAUTH` / `WRONGPASS` | `AU001` |
| Redis `NOPERM` / `READONLY` | `AU002` |
| Redis 客户端配置无效 | `CF002` |
| Redis 超时 | `CN002` |
| Redis I/O、cluster down、master down 或缺少集群连接 | `CN001` |
| MongoDB 认证错误或命令错误码 `18` | `AU001` |
| MongoDB 命令错误码 `13` | `AU002` |
| MongoDB 命令错误码 `26` | `MD001` |
| MongoDB 重复键错误码 `11000`/`11001`/`12582` | `IC001` |
| MongoDB 客户端选项无效 / TLS 配置无效 | 边界默认码 `CF002` / `CN003` |
| MongoDB DNS、I/O、连接池清空、server selection 或 shutdown | `CN001`；I/O 超时为 `CN002` |
| Kafka 认证 / 授权错误 | `AU001` / `AU002` |
| Kafka topic 或 partition 不存在 | `MD001` |
| Kafka 超时 / broker 传输失败 | `CN002` / `CN001` |
| HTTP 请求超时 / 连接失败 | `CN002` / `CN001` |
| HTTP 非成功响应或响应体无效 | `DB001` |
| MySQL binlog I/O 超时 / 其他传输失败 | `CN002` / `CN001` |
| MySQL binlog GTID 无效 | `CF002` |
| MySQL binlog 错误 `1236`（请求的 binlog 已不可用） | `ST001`，origin `mysql/1236` |
| 其他 MySQL binlog 解码失败 | `DB001` |

Worker join 失败使用 `RT001`。在组件边界分类的 URL 和 YAML 解析错误使用 `CF002`；
未分类原始错误到达 `ErrorReport` 时使用 `IN999`。本地文件系统 `std::io::Error` 使用
`IO001`；网络 I/O 必须在 provider 边界分类为
`CN001` 或 `CN002`。用户主动中断的 CLI 操作使用 `RT002`。

`mysql-binlog-connector-rust v0.3.4` 会丢弃 MySQL 错误包中的数值错误码，并将错误
`1236` 暴露为 `ConnectError(String)`。在 connector 保留类型化错误码之前，MySQL
extractor 边界只识别“请求的 binlog 已不可用”这一组已知消息，并恢复 origin code
`1236`。该逻辑是窄范围兼容补偿，不属于通用 provider classifier。

数据库连接 URL 格式错误属于配置错误，使用 `CF002`。只有格式正确的端点无法访问，
或者已经建立的连接中断时，才使用 `CN001`。这一区分对应不同的用户处理方式：
`CF002` 需要修正配置；`CN001` 需要检查服务状态和网络连通性。
