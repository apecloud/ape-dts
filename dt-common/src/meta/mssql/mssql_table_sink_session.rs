use anyhow::{bail, Context};

use super::{
    mssql_connection_pool::{MssqlClient, MssqlConnectionPool, MssqlPooledConnection},
    mssql_tb_meta::MssqlTbMeta,
};
use crate::{config::config_enums::DbType, utils::sql_util::SqlUtil};

#[must_use = "call post after table writes so the MSSQL session can be reused safely"]
pub struct MssqlTableSinkSession<'pool, 'meta> {
    connection: MssqlPooledConnection<'pool>,
    tb_meta: &'meta MssqlTbMeta,
    identity_insert_enabled: bool,
    transaction_active: bool,
}

impl MssqlConnectionPool {
    pub async fn get_table_sink_session<'pool, 'meta>(
        &'pool self,
        tb_meta: &'meta MssqlTbMeta,
    ) -> anyhow::Result<MssqlTableSinkSession<'pool, 'meta>> {
        MssqlTableSinkSession::new(self, tb_meta).await
    }
}

impl<'pool, 'meta> MssqlTableSinkSession<'pool, 'meta> {
    pub async fn new(
        pool: &'pool MssqlConnectionPool,
        tb_meta: &'meta MssqlTbMeta,
    ) -> anyhow::Result<Self> {
        let mut connection = pool.get().await?;
        let identity_insert_statement = Self::identity_insert_statement(tb_meta, true);

        if let Some(statement) = identity_insert_statement.as_deref() {
            // IDENTITY_INSERT is session-scoped. Keep the connection marked
            // until the matching post statement has completed successfully.
            connection.mark_for_discard();
            execute_control_statement(connection.client_mut(), statement).await?;
        }

        Ok(Self {
            connection,
            tb_meta,
            identity_insert_enabled: identity_insert_statement.is_some(),
            transaction_active: false,
        })
    }

    pub fn client_mut(&mut self) -> &mut MssqlClient {
        self.connection.client_mut()
    }

    pub fn tb_meta(&self) -> &MssqlTbMeta {
        self.tb_meta
    }

    pub async fn begin(&mut self) -> anyhow::Result<()> {
        if self.transaction_active {
            bail!("MSSQL table sink session already has an active transaction");
        }

        self.connection.mark_for_discard();
        self.transaction_active = true;
        execute_control_statement(self.connection.client_mut(), "BEGIN TRANSACTION").await?;
        Ok(())
    }

    pub async fn post(&mut self) -> anyhow::Result<()> {
        if self.transaction_active {
            bail!("MSSQL table sink session must commit or roll back before post");
        }
        if !self.identity_insert_enabled {
            return Ok(());
        }

        let statement = Self::identity_insert_statement(self.tb_meta, false)
            .context("MSSQL table sink session IDENTITY_INSERT post statement is missing")?;
        execute_control_statement(self.connection.client_mut(), &statement).await?;
        self.identity_insert_enabled = false;
        self.clear_discard_mark_if_clean();
        Ok(())
    }

    pub async fn commit(&mut self) -> anyhow::Result<()> {
        if !self.transaction_active {
            bail!("MSSQL table sink session has no active transaction to commit");
        }

        execute_control_statement(self.connection.client_mut(), "COMMIT TRANSACTION").await?;
        self.transaction_active = false;
        self.clear_discard_mark_if_clean();
        Ok(())
    }

    pub async fn rollback(&mut self) -> anyhow::Result<()> {
        if !self.transaction_active {
            bail!("MSSQL table sink session has no active transaction to roll back");
        }

        execute_control_statement(self.connection.client_mut(), "ROLLBACK TRANSACTION").await?;
        self.transaction_active = false;
        self.clear_discard_mark_if_clean();
        Ok(())
    }

    fn clear_discard_mark_if_clean(&mut self) {
        if !self.identity_insert_enabled && !self.transaction_active {
            self.connection.clear_discard_mark();
        }
    }

    fn identity_insert_statement(tb_meta: &MssqlTbMeta, enabled: bool) -> Option<String> {
        if !tb_meta.has_identity_col() {
            return None;
        }
        let table = SqlUtil::render_rdb_table(
            &DbType::Mssql,
            &tb_meta.basic.db,
            &tb_meta.basic.schema,
            &tb_meta.basic.tb,
        );
        Some(format!(
            "SET IDENTITY_INSERT {table} {}",
            if enabled { "ON" } else { "OFF" }
        ))
    }
}

async fn execute_control_statement(
    client: &mut MssqlClient,
    statement: &str,
) -> anyhow::Result<()> {
    client
        .simple_query(statement)
        .await
        .with_context(|| format!("failed to execute MSSQL {statement}"))?
        .into_results()
        .await
        .with_context(|| format!("failed to consume MSSQL {statement} response"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::rdb_tb_meta::RdbTbMeta;

    fn build_tb_meta(identity_col: Option<&str>) -> MssqlTbMeta {
        MssqlTbMeta {
            basic: RdbTbMeta {
                db: "database]name".to_string(),
                schema: "schema]name".to_string(),
                tb: "table]name".to_string(),
                ..Default::default()
            },
            identity_col: identity_col.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn builds_identity_insert_statements_from_table_meta() {
        let tb_meta = build_tb_meta(Some("id"));

        assert_eq!(
            MssqlTableSinkSession::identity_insert_statement(&tb_meta, true).as_deref(),
            Some("SET IDENTITY_INSERT [database]]name].[schema]]name].[table]]name] ON")
        );
        assert_eq!(
            MssqlTableSinkSession::identity_insert_statement(&tb_meta, false).as_deref(),
            Some("SET IDENTITY_INSERT [database]]name].[schema]]name].[table]]name] OFF")
        );
    }

    #[test]
    fn skips_identity_insert_when_table_meta_has_no_identity_column() {
        let tb_meta = build_tb_meta(None);

        assert!(MssqlTableSinkSession::identity_insert_statement(&tb_meta, true).is_none());
        assert!(MssqlTableSinkSession::identity_insert_statement(&tb_meta, false).is_none());
    }
}
