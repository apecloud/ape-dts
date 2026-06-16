use std::collections::HashMap;

use anyhow::bail;
use async_trait::async_trait;

use dt_common::{
    config::{connection_auth_config::ConnectionAuthConfig, task_config::APE_DTS},
    meta::mongo::mongo_version::get_server_version,
    rdb_filter::RdbFilter,
};
use dt_task::task_util::TaskUtil;
use mongodb::{
    bson::{doc, Document},
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
    pub is_direct_connection: Option<bool>,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for MongoFetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()> {
        self.pool = Some(
            TaskUtil::create_mongo_client(
                &self.url,
                &self.connection_auth,
                self.is_direct_connection,
                Some(APE_DTS.to_owned()),
                None,
            )
            .await?,
        );
        Ok(())
    }

    async fn fetch_version(&mut self) -> anyhow::Result<String> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => bail! {"client is closed."},
        };
        Ok(format!("{}", get_server_version(client).await?))
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
    pub async fn execute_for_admin(&self, command: &str) -> anyhow::Result<Document> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => bail! {"client is closed."},
        };

        let doc_command = doc! {command: 1};
        Ok(client.database("admin").run_command(doc_command).await?)
    }

    pub async fn execute_for_db(&self, command: &str) -> anyhow::Result<Document> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => bail! {"client is closed."},
        };

        let dbs = client.list_databases().await?;
        if dbs.is_empty() {
            bail! {"no db exists in mongo."};
        }

        let doc_command = doc! {command: 1};
        let doc = client
            .database(&dbs[0].name)
            .run_command(doc_command)
            .await?;
        Ok(doc)
    }
}
