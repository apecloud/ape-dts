use std::collections::HashMap;

use anyhow::bail;
use async_trait::async_trait;

use dt_common::{config::connection_auth_config::ConnectionAuthConfig, rdb_filter::RdbFilter};
use dt_task::task_util::TaskUtil;
use mongodb::{
    bson::{doc, Bson, Document},
    Client,
};

use crate::{
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct MongoFetcher {
    pub pool: Option<Client>,
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for MongoFetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()> {
        self.pool =
            Some(TaskUtil::create_mongo_client(&self.url, &self.connection_auth, "", None).await?);
        Ok(())
    }

    async fn fetch_version(&mut self) -> anyhow::Result<String> {
        let document = self.execute_for_db("buildInfo").await?;
        Ok(String::from(
            document
                .get("version")
                .and_then(Bson::as_str)
                .unwrap_or("unknown"),
        ))
    }

    async fn fetch_configuration(
        &mut self,
        _config_keys: Vec<String>,
    ) -> anyhow::Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }

    async fn fetch_databases(&mut self) -> anyhow::Result<Vec<Database>> {
        Ok(vec![])
    }

    async fn fetch_schemas(&mut self) -> anyhow::Result<Vec<Schema>> {
        Ok(vec![])
    }

    async fn fetch_tables(&mut self) -> anyhow::Result<Vec<Table>> {
        Ok(vec![])
    }

    async fn fetch_constraints(&mut self) -> anyhow::Result<Vec<Constraint>> {
        Ok(vec![])
    }
}

impl MongoFetcher {
    pub async fn execute_for_db(&self, command: &str) -> anyhow::Result<Document> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => bail! {"client is closed."},
        };

        let dbs = client.list_databases(None, None).await?;
        if dbs.is_empty() {
            bail! {"no db exists in mongo."};
        }

        let doc_command = doc! {command: 1};
        let doc = client
            .database(&dbs[0].name)
            .run_command(doc_command, None)
            .await?;
        Ok(doc)
    }
}
