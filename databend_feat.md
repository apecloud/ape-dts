

# Databend Sinker Feature 实现计划

## 概述

本 feature 的目标是为 databend 实现一个 sinker，使得数据能够从 MySQL 抽取后转换并写入 databend 目标。整个系统主要分为 Extractor 与 Sinker 两个部分，Extractor 负责数据抽取入队，而 Sinker 则消费数据并将数据转换后写入目标数据库。鉴于已有的各类 sinker（如 MysqlSinker、StarRocksSinker 等）实现了类似的逻辑，本次只需要新增一个符合 Sinker 接口的 databend sinker，其他业务逻辑无需修改。databend 官方的 Rust SDK 可参考：[BendSQL driver](https://github.com/databendlabs/BendSQL/tree/main/driver)。

---

## 架构与设计

- **整体架构**  
  - 系统主要由 Extractor、Sinker、Task Factory（dt-task）及 Parallelizer（dt-parallelizer）构成。  
  - Sinker 负责转换数据为目标数据源的 SQL 或直接调用第三方 SDK 实现写入。

- **现有实现参考**  
  - 可以参考 starrocks sinker（如 dt-connector/src/sinker/starrocks/starrocks_sinker.rs）以及 mysql sinker 的实现，了解如何处理 row_data、DML 语句以及批量/串行处理的实现模式。
  - dt-task 通过将各个 sinker 注册到工厂中（例如 dt-task/src/sinker_util.rs 中），来创建各个 sinker 实例，因此需要在此处新增 Databend sinker 的处理逻辑和注册入口。

---

## 分步实现计划

### 第一步：需求分析与环境准备

- **需求分析**  
  - 实现一个新的 Sinker，其核心接口为 Sinker trait（包括 sink_dml、sink_ddl、sink_raw、sink_struct 等方法）。
  - 本次实现只需支持 MySQL → Databend 的数据同步，Extractor 部分保持不变。

- **环境准备**  
  - 克隆最新版代码，理清代码结构（dt-connector、dt-task、dt-tests 下的测试示例）。
  - 下载 databend 官方 Rust SDK，并熟悉其基本用法。

---

### 第二步：实现 Databend Sinker

- **新建文件与结构设计**  
  - 在 dt-connector/src/sinker/ 下新建文件，例如：`databend_sinker.rs`。  
  - 定义结构体 `DatabendSinker`，包含必要的属性，比如：databend 连接信息、批次参数、监控对象等。
  
- **实现 Sinker Trait**  
  - 为 `DatabendSinker` 实现 Sinker trait 接口。  
  - 在 sink_dml 方法中，需要将 RowData 数据转换为 databend 可接受的格式（比如转换成对应的 SQL 或调用 SDK 的数据写入 API）。
  
  ```rust:dt-connector/src/sinker/databend_sinker.rs
  // Define the DatabendSinker struct with required fields.
  pub struct DatabendSinker {
      // Connection pool or client from BendSQL driver
      pub client: DatabendClient, // from BendSQL driver
      // Batch size, monitor and other parameters
      pub batch_size: usize,
      pub monitor: Arc<Mutex<Monitor>>,
      // Other configuration parameters
  }
  
  #[async_trait]
  impl Sinker for DatabendSinker {
      async fn sink_dml(&mut self, data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
          // Convert row data to databend SQL or SDK write call
          // Example comment: // Convert each row to SQL query and execute using client
          Ok(())
      }
  
      async fn close(&mut self) -> anyhow::Result<()> {
          // Close connections if needed
          Ok(())
      }
  
      // Other trait method implementations...
  }
  ```
  
  *注意：代码中的注释保持英文。*

- **集成到 Sinker 工厂**  
  - 修改 dt-task/src/sinker_util.rs，在 create_sinkers 方法中为新建的 Databend sinker 分支增加对应逻辑。  
  - 例如，可以新增一个 SinkerConfig::Databend 枚举分支，读取任务配置中的 databend 相关字段，然后通过新建 DatabendSinker 实例添加到 sinker 列表中。

---

### 第三步：编写配置文件和测试用例

- **编写测试任务配置文件**  
  - 参考 dt-tests/tests/mysql_to_starrocks/ 下已有的任务测试配置文件，在 dt-tests 下新建一个例如 `mysql_to_databend` 的目录。  
  - 编写任务配置文件（例如 task_config.ini 或 struct_task_config.ini），配置的 extractor 依旧为 mysql，sinker 则为 databend。  
  - 配置文件中需要指定 databend 的 URL、批处理参数、是否使用 SDK 进行写入等参数。

- **实现最小化测试用例**  
  - 新增一个集成测试用例，通过手动启动任务，观察任务日志并验证 databend 中数据是否与预期一致。  
  - 可以参考已有的测试用例目录结构，写出类似测试文件，帮助开发者在本地手动验证。

---

### 第四步：测试与验证

- **功能验证方法**  
  - 启动一个 mysql 到 databend 的同步任务。  
  - 人工检查 databend 中是否正确创建目标表、数据是否正确同步。  
  - 同时观察任务日志，检查是否有异常或错误提示。

- **验证要点**  
  - 数据转换逻辑：确保 RowData 正确转换成 databend 可接受的格式。  
  - 错误处理：确保 SDK 调用异常能正确上报，并且不影响后续任务处理。  
  - 性能监控：通过 monitor 统计数据是否能正确记录同步速度和批次情况。

- **后续集成优化**  
  - 根据手动测试情况，可进一步补充单元测试或集成测试覆盖率，确保功能稳健。

---

## 里程碑规划

1. **需求调研与设计确认**  
   - 理解现有 Sinker 相关代码、接口及测试用例的实现方式  
   - 确认 databend SDK 的使用方法及所需参数

2. **DatabendSinker 实现**  
   - 新建 databend_sinker.rs，实现 Sinker trait 的相关接口  
   - 集成到 dt-task 的工厂方法中，支持通过任务配置创建实例

3. **编写测试用例与任务配置**  
   - 参照 dt-tests/tests/mysql_to_starrocks/ 目录，建立对应的 mysql_to_databend 目录  
   - 编写任务配置文件，配置好数据同步参数

4. **手动验证及日志检查**  
   - 启动任务后，检查 databend 实例中数据是否正确写入  
   - 分析任务日志，确认错误处理及数据转换无异常

5. **后续完善与优化**  
   - 根据测试反馈，调整数据转换及连接异常处理逻辑  
   - 补充必要的单元测试，确保功能鲁棒

---

## 小结

本次 feature 实现只需新增 databend sinker，无需修改现有 extractor 或其他逻辑。实现过程中主要工作包含：  
- 实现符合 Sinker trait 的 DatabendSinker  
- 集成到 dt-task 工厂和任务配置中  
- 编写配置文件并利用现有测试用例体系验证任务同步

完成上述工作后，即可手动启动 mysql → databend 的数据同步任务，并通过日志与实际数据验证功能是否正确实现。

--- 

以上就是 databend sinker feature 的完整实现计划。
