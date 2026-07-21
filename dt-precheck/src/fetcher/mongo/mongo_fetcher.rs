use std::collections::HashMap;

use anyhow::bail;
use async_trait::async_trait;

use dt_common::{
    config::{connection_auth_config::ConnectionAuthConfig, task_config::APE_DTS},
    error::{DtError, DtErrorContextExt, EndpointRole, ErrorCode, Stage},
    meta::mongo::mongo_version::get_server_version,
    rdb_filter::RdbFilter,
};
use dt_task::task_util::TaskUtil;
use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::{
    error_boundary::mongodb::{mongo_precheck_provider_error, mongo_precheck_state_error},
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
            None => bail! {mongo_precheck_state_error(
                self.is_source,
            )},
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
            None => bail! {mongo_precheck_state_error(
                self.is_source,
            )},
        };

        let doc_command = doc! {command: 1};
        client
            .database("admin")
            .run_command(doc_command)
            .await
            .map_err(|error| mongo_precheck_provider_error(error, self.is_source))
    }

    pub async fn execute_for_db(&self, command: &str) -> anyhow::Result<Document> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => bail! {mongo_precheck_state_error(
                self.is_source,
            )},
        };

        let dbs = client
            .list_databases()
            .await
            .map_err(|error| mongo_precheck_provider_error(error, self.is_source))?;
        if dbs.is_empty() {
            bail! {DtError::MetadataError("no database exists in MongoDB".to_string())
            .with_code(ErrorCode::DatabaseNotFound)
            .with_stage(Stage::Precheck)
            .with_endpoint(if self.is_source {
                EndpointRole::Source
            } else {
                EndpointRole::Destination
            })};
        }

        let doc_command = doc! {command: 1};
        let doc = client
            .database(&dbs[0].name)
            .run_command(doc_command)
            .await
            .map_err(|error| mongo_precheck_provider_error(error, self.is_source))?;
        Ok(doc)
    }
}
