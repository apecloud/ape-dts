use anyhow::{bail, Context};
use async_trait::async_trait;
use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    error::{DtErrorContextExt, ErrorCode},
    log_error, log_info,
    meta::{
        mssql::mssql_connection_pool::{MssqlClient, MssqlConnectionPool},
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    rdb_filter::RdbFilter,
    utils::limit_queue::LimitedQueue,
};
use tokio::time::Instant;

use crate::{rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

#[derive(Clone)]
pub struct MssqlStructSinker {
    pub connection_pool: MssqlConnectionPool,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: Option<RdbRouter>,
    pub base_sinker: BaseSinker,
}

#[async_trait]
impl Sinker for MssqlStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut rts = LimitedQueue::new(data.len().clamp(1, 100));
        let mut struct_count = 0_u64;

        for mut struct_data in data {
            let requires_autocommit = Self::requires_autocommit(&struct_data.statement);
            let sqls = struct_data
                .statement
                .to_sqls(&self.filter)?
                .into_iter()
                .map(|(_, sql)| sql)
                .collect::<Vec<_>>();
            if sqls.is_empty() {
                continue;
            }
            struct_count += 1;

            match self.conflict_policy {
                ConflictPolicyEnum::Interrupt => {
                    let start = Instant::now();
                    if requires_autocommit {
                        self.execute_with_autocommit(&sqls).await?;
                    } else {
                        self.execute_atomically(&sqls).await?;
                    }
                    rts.push((start.elapsed().as_millis() as u64, sqls.len() as u64));
                }
                ConflictPolicyEnum::Ignore => {
                    for sql in sqls {
                        let start = Instant::now();
                        let result = if requires_autocommit {
                            self.execute_with_autocommit(std::slice::from_ref(&sql))
                                .await
                        } else {
                            self.execute_ignoring_conflict(&sql).await
                        };
                        if let Err(error) = result {
                            log_error!("ddl ignored, error: {}", error);
                        }
                        rts.push((start.elapsed().as_millis() as u64, 1));
                    }
                }
            }
        }

        if struct_count > 0 {
            self.base_sinker
                .update_serial_monitor(struct_count, 0)
                .await?;
            self.base_sinker.update_monitor_rt(&rts).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl MssqlStructSinker {
    fn requires_autocommit(statement: &StructStatement) -> bool {
        matches!(statement, StructStatement::MssqlCreateDatabase(_))
    }

    async fn execute_with_autocommit(&self, sqls: &[String]) -> anyhow::Result<()> {
        let mut connection = self.connection_pool.get().await?;
        for sql in sqls {
            log_info!("ddl begin: {}", sql);
            Self::execute(connection.client_mut(), sql).await?;
            log_info!("ddl succeed");
        }
        Ok(())
    }

    async fn execute_atomically(&self, sqls: &[String]) -> anyhow::Result<()> {
        let mut transaction = self.connection_pool.begin().await?;
        for sql in sqls {
            log_info!("ddl begin: {}", sql);
            if let Err(error) = Self::execute(transaction.client_mut(), sql).await {
                let rollback_result = transaction.rollback().await;
                if let Err(rollback_error) = rollback_result {
                    return Err(error).context(format!(
                        "failed to roll back MSSQL struct transaction: {rollback_error}"
                    ));
                }
                bail!(error);
            }
            log_info!("ddl succeed");
        }
        transaction
            .commit()
            .await
            .context("commit MSSQL struct transaction")
    }

    async fn execute_ignoring_conflict(&self, sql: &str) -> anyhow::Result<()> {
        let mut transaction = self.connection_pool.begin().await?;
        log_info!("ddl begin: {}", sql);
        match Self::execute(transaction.client_mut(), sql).await {
            Ok(()) => {
                transaction
                    .commit()
                    .await
                    .context("commit MSSQL struct transaction")?;
                log_info!("ddl succeed");
                Ok(())
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .context("rollback ignored MSSQL struct statement")?;
                Err(error)
            }
        }
    }

    async fn execute(client: &mut MssqlClient, sql: &str) -> anyhow::Result<()> {
        client
            .simple_query(sql)
            .await
            .map_err(|error| error.code(ErrorCode::StatementFailed))?
            .into_results()
            .await
            .map_err(|error| error.code(ErrorCode::StatementFailed))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dt_common::meta::struct_meta::{
        statement::{
            mssql_create_database_statement::MssqlCreateDatabaseStatement,
            struct_statement::StructStatement,
        },
        structure::database::Database,
    };

    use super::MssqlStructSinker;

    #[test]
    fn create_database_requires_autocommit() {
        let statement = StructStatement::MssqlCreateDatabase(MssqlCreateDatabaseStatement {
            database: Database {
                name: "test_db".to_string(),
                ..Default::default()
            },
        });

        assert!(MssqlStructSinker::requires_autocommit(&statement));
        assert!(!MssqlStructSinker::requires_autocommit(
            &StructStatement::Unknown
        ));
    }
}
