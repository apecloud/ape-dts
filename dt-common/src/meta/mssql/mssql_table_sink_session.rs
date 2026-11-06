use anyhow::{bail, Context};
use tiberius::TokenRow;

use super::{
    mssql_col_type::MssqlColType,
    mssql_connection_pool::{MssqlClient, MssqlConnectionPool, MssqlPooledConnection},
    mssql_tb_meta::MssqlTbMeta,
};
use crate::{
    config::config_enums::DbType,
    meta::{
        adaptor::mssql_col_value_convertor::MssqlColValueConvertor, row_data::RowData,
        row_type::RowType,
    },
    utils::sql_util::SqlUtil,
};

#[must_use = "call finalize after table writes so the MSSQL session can be reused safely"]
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
            // until it has been disabled successfully.
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

    pub fn can_bulk_insert(&self, rows: &[RowData]) -> bool {
        let Some(first) = rows.first() else {
            return false;
        };
        if !matches!(first.row_type, RowType::Insert) {
            return false;
        }

        let Ok(columns) = self.bulk_insert_columns() else {
            return false;
        };
        first
            .require_after()
            .is_ok_and(|after| columns.iter().all(|(col, _)| after.contains_key(*col)))
    }

    pub async fn bulk_insert(&mut self, rows: &[RowData]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let columns = self.bulk_insert_columns()?;

        // Finish all fallible RowData conversion before opening the TDS bulk stream.
        let token_rows = rows
            .iter()
            .map(|row_data| {
                let after = row_data.require_after()?;
                let mut token_row = TokenRow::with_capacity(columns.len());
                for (col, col_type) in &columns {
                    let value = after.get(*col).with_context(|| {
                        format!(
                            "MSSQL bulk insert row is missing column {}.{}.{}",
                            self.tb_meta.basic.schema, self.tb_meta.basic.tb, col
                        )
                    })?;
                    token_row.push(MssqlColValueConvertor::to_column_data(value, col_type)?);
                }
                Ok(token_row)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let table = SqlUtil::render_rdb_table(
            &DbType::Mssql,
            &self.tb_meta.basic.db,
            &self.tb_meta.basic.schema,
            &self.tb_meta.basic.tb,
        );
        self.connection.mark_for_discard();
        let result: anyhow::Result<()> = async {
            let mut request = self.connection.client_mut().bulk_insert(&table).await?;
            for token_row in token_rows {
                if let Err(err) = request.send(token_row).await {
                    let finalize_error = request.finalize().await.err();
                    let err = anyhow::Error::from(err);
                    return Err(match finalize_error {
                        Some(finalize_error) => err.context(format!(
                            "MSSQL bulk insert finalize also failed: {finalize_error}"
                        )),
                        None => err,
                    });
                }
            }
            request.finalize().await?;
            Ok(())
        }
        .await;

        result.with_context(|| format!("failed to bulk insert rows into {table}"))?;
        self.clear_discard_mark_if_clean();
        Ok(())
    }

    fn bulk_insert_columns(&self) -> anyhow::Result<Vec<(&str, MssqlColType)>> {
        if let Some(identity_col) = self.tb_meta.identity_col.as_deref() {
            bail!("MSSQL bulk insert cannot preserve identity column {identity_col}");
        }

        let columns = self
            .tb_meta
            .basic
            .cols
            .iter()
            .filter(|col| self.tb_meta.is_writable_col(col))
            .map(|col| {
                let col_type = *self.tb_meta.get_col_type(col)?;
                if col_type.requires_special_transfer() {
                    bail!("MSSQL bulk insert does not support specially transferred column {col}");
                }
                Self::ensure_bulk_insert_type_supported(col, &col_type)?;
                Ok((col.as_str(), col_type))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if columns.is_empty() {
            bail!("MSSQL bulk insert has no writable columns");
        }
        Ok(columns)
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

    pub async fn finalize(&mut self) -> anyhow::Result<()> {
        if self.transaction_active {
            bail!("MSSQL table sink session must commit or roll back before finalize");
        }
        if !self.identity_insert_enabled {
            return Ok(());
        }

        let statement = Self::identity_insert_statement(self.tb_meta, false)
            .context("MSSQL table sink session IDENTITY_INSERT OFF statement is missing")?;
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

    fn ensure_bulk_insert_type_supported(col: &str, col_type: &MssqlColType) -> anyhow::Result<()> {
        // With `tds73`, Tiberius converts NaiveDateTime only to DateTime2:
        // https://github.com/prisma/tiberius/blob/0e2897a276166503ba78fe3e1cee501e9a034021/src/tds/time/chrono.rs#L106-L128
        if matches!(
            col_type,
            MssqlColType::Money
                | MssqlColType::Money4
                | MssqlColType::Datetime4
                | MssqlColType::Datetime
                | MssqlColType::Datetimen
                | MssqlColType::Text
                | MssqlColType::Image
                | MssqlColType::NText
        ) {
            bail!("MSSQL bulk insert does not support column {col} type {col_type:?}");
        }
        Ok(())
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
