use anyhow::Context;

use super::mssql_connection_pool::{MssqlClient, MssqlConnectionPool, MssqlPooledConnection};

#[must_use = "the transaction must be committed or rolled back"]
pub struct MssqlTransaction<'pool> {
    connection: MssqlPooledConnection<'pool>,
}

impl MssqlConnectionPool {
    pub async fn begin(&self) -> anyhow::Result<MssqlTransaction<'_>> {
        MssqlTransaction::begin(self).await
    }
}

impl<'pool> MssqlTransaction<'pool> {
    async fn begin(pool: &'pool MssqlConnectionPool) -> anyhow::Result<Self> {
        let mut connection = pool.get().await?;
        connection.mark_for_discard();
        execute_control_statement(connection.client_mut(), "BEGIN TRANSACTION").await?;

        Ok(Self { connection })
    }

    pub fn client_mut(&mut self) -> &mut MssqlClient {
        self.connection.client_mut()
    }

    pub async fn commit(mut self) -> anyhow::Result<()> {
        execute_control_statement(self.connection.client_mut(), "COMMIT TRANSACTION").await?;
        self.connection.clear_discard_mark();
        Ok(())
    }

    pub async fn rollback(mut self) -> anyhow::Result<()> {
        execute_control_statement(self.connection.client_mut(), "ROLLBACK TRANSACTION").await?;
        self.connection.clear_discard_mark();
        Ok(())
    }
}

async fn execute_control_statement(
    client: &mut MssqlClient,
    statement: &'static str,
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
