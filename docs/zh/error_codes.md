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

项目主动失败选择语义化的 `DtError` variant，并只挂载当前调用点真正掌握的 metadata。
例如 `DtError::InvalidConfig(detail).with_stage(Stage::Bootstrap)` 创建根因并补充 stage，
不再重复书写 `ErrorCode::InvalidConfig`。`ClassifyError` 实现将每个 `DtError` variant 映射
为 `DtErrorContext`，其中包含稳定的默认错误码和该 variant 固有的 metadata；新增 variant
时必须增加对应的 match 分支，无法静默落入兜底。
`DtError::Unclassified` 只用于项目主动产生、但暂时没有稳定分类的失败。

同一 extension trait 也为 `anyhow::Error` 和已支持的 provider 错误类型实现。显式的
`DtErrorContext.code` 优先于项目错误和 provider 分类器。仅当操作本身定义了错误条件，
或者动态聚合结果无法用 `DtError` variant 表达时，才显式设置 code。未知的具体错误可以
通过 `DtErrorContext::attach` 保留其 source 类型。
如果项目语义分类同时存在更底层 source，则将语义化 `DtError` 作为 typed context 放在
source 之上。这样 `DtError` 和原始 source 都可 downcast，报告可通过项目分类器取码，
同时不会丢失底层诊断信息。

Metadata 在错误跨越所有权边界时逐层挂载。叶子 frame 可以提供业务错误码、受影响对象
和 provider origin。stage 和 endpoint 在拥有当前操作的最窄组件边界挂载，task ID 则在
仍然明确知道它的最高层边界挂载：例如 extractor worker 挂载 `extractor/source`，统一
sinker adapter 挂载 `sinker/destination`，recovery 初始化边界挂载 `resumer/metadata`，
precheck 入口挂载 `precheck` 和 task ID，而 precheck builder 在各 checker 调用外层挂载 `source` 或
`destination`。普通任务的 task ID 只在任务入口挂载。报告阶段不做推导。

Frame 挂载后不再修改。`ErrorReport` 从最外层 frame 开始递归读取内层 metadata 链。
scope 和根因字段使用最接近根因的内层值：错误经过其他组件传播时，stage、endpoint 和
provider origin 不会仅因为外层再次设置而被替换。错误对象按字段合并，保留内层已有值，
只从外层补充缺失字段。code、用户 message、hint 和 task ID 仍然采用显式的外层优先级。
因此 sinker 错误经过 parallelizer 和 pipeline 传播后仍报告为 `sinker/destination`，任务入口
仍可补充 task ID 和面向用户的上下文。`DtErrorContext` 不包含 detail 字段；报告构建时从
已脱敏的普通 context 和具体 cause 生成 detail。项目自有 `DtError`
会保留 variant 对应的完整 `Display` 文本和类型化 payload，与 provider error 保留自身
`Display`、source chain 和具体类型的方式一致。

已支持的 provider 错误通常直接通过 `?` 或普通 `anyhow::Context` 传播。构建 report 时，
共享 raw-cause registry 会按保留的具体类型分类，恢复 provider 固有的 code、origin 和
object。只有 provider 无法知道的业务语义或执行 scope 才显式挂载 `DtErrorContext`。
两条路径都不会在报告阶段推断 stage、endpoint 或 task ID。

`ErrorReport` 是面向用户的边界表示。其文本格式使用 `ERROR`、`TASK`、`AFFECTED`、
`DETAIL`、`HINT` 和 `DIAGNOSTIC` 等行。

Provider 分类器实现统一位于 `dt-common::error::provider`。它们通过 `ClassifyError` 根据
provider 原始错误码、类型化错误种类和 Rust 错误变体进行判断，禁止解析 provider 错误
消息。每个实现直接返回包含可选精确错误码、provider origin 和受影响对象的
`DtErrorContext`。一个小型显式 registry 对 `anyhow::Error` 中保留的 provider cause 调用
同一 trait。明确知道数据库类型的 SQLx 调用点继续传入 `SqlxProvider`，保证 transport 错误的
origin 精确；raw fallback 只使用 infer 分类。`SqlxErrorExt::with_sqlx_provider` 挂载 provider
frame，同时保留原始 SQLx error。操作同时提供 code 时，调用点先使用 `.with_code(...)`，
再使用 `.with_sqlx_provider(...)`。已识别的 provider 条件因而成为更外层、更精确的 frame；
无法识别时则保留操作 code。stage、endpoint 和 task ID 在仍明确知道其语义的最高层执行
位置单独挂载。

Provider 分类统一位于 `dt-common::error::provider`，不再放入各 crate 的 wrapper。
业务模块使用 `DtError` 表达项目主动失败，使用普通 `anyhow::Context` 添加诊断文字，只在
需要显式 metadata 时调用 context extension。禁止新增仅转发这些共享机制参数的 crate
专用 helper 模块。

`ErrorReport` 读取类型化 context 和错误链。它先使用最外层显式 context code；没有时，
从 cause chain downcast 到 `DtError` 并调用 `ClassifyError`；最后调用 registry 中的 raw
provider 分类器。两条路径都返回 `DtErrorContext`，内部 resolver 使用相同优先级规则合并 metadata。
显式业务 metadata 因此仍然具有最高优先级，已知 provider 错误也不会仅因为调用者
使用普通 `?` 而变成 `IN999`。不受支持或无法识别的 raw 错误仍输出 `IN999`。失败路径测试
和代码评审仍需覆盖 raw provider 无法提供的显式操作 code 及调用点 metadata。

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

SQLx 调用点可以显式提供与操作相关的错误码，例如 `CN001`、`DB001` 或 `ST001`。
精确的 provider 分类优先，无法识别时保留操作 code；两种情况下都保留 provider 原始
错误码、constraint/table 元数据和源错误链。

`tokio-postgres` 复制适配器使用相同的 PostgreSQL SQLSTATE 规则。URL 解析错误使用
`CF002`，连接建立失败使用 `CN001`，复制命令被拒绝时使用该操作显式设置的错误码。

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
| MongoDB 客户端选项无效 / TLS 配置无效 | 调用点显式 code `CF002` / `CN003` |
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

Worker join 失败使用 `RT001`。由调用者显式分类的 URL 和 YAML 解析错误使用 `CF002`；
`DtError::Unclassified` 和不受支持或无法识别的原始错误到达 `ErrorReport` 时都使用 `IN999`。本地文件
系统 `std::io::Error` 使用 `IO001`；网络 I/O 应保留 provider 错误类型，以分类为 `CN001` 或
`CN002`。用户主动中断的 CLI 操作使用 `RT002`。

`mysql-binlog-connector-rust v0.3.4` 会丢弃 MySQL 错误包中的数值错误码，并将错误
`1236` 暴露为 `ConnectError(String)`。在 connector 保留类型化错误码之前，MySQL
binlog 分类器只识别“请求的 binlog 已不可用”这一组已知消息，并恢复 origin code
`1236`。这是针对 provider 库丢失类型信息的窄范围消息匹配例外；其他 provider 分类仍
基于类型和原始错误码。

数据库连接 URL 格式错误属于配置错误，使用 `CF002`。只有格式正确的端点无法访问，
或者已经建立的连接中断时，才使用 `CN001`。这一区分对应不同的用户处理方式：
`CF002` 需要修正配置；`CN001` 需要检查服务状态和网络连通性。
