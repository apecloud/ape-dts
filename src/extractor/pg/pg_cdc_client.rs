use log::info;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{replication::LogicalReplicationStream, Client};
use url::Url;

use crate::error::Error;

pub struct PgCdcClient {
    pub url: String,
    pub slot_name: String,
    pub start_sln: String,
}

impl PgCdcClient {
    pub async fn connect(&mut self) -> Result<LogicalReplicationStream, Error> {
        let url_info = Url::parse(&self.url).unwrap();
        let host = url_info.host_str().unwrap().to_string();
        let port = format!("{}", url_info.port().unwrap());
        let username = url_info.username().to_string();
        let password = url_info.password().unwrap().to_string();
        let conn_info = format!(
            "host={} port={} user={} password={} replication=database",
            host, port, username, password
        );

        let (client, connection) = tokio_postgres::connect(&conn_info, NoTls).await.unwrap();
        tokio::spawn(async move {
            info!("postgres replication connection starts",);
            if let Err(e) = connection.await {
                info!("postgres replication connection drops, error: {}", e);
            }
        });
        let stream = self.start_replication(&client).await?;
        Ok(stream)
    }

    async fn start_replication(
        &mut self,
        client: &Client,
    ) -> Result<LogicalReplicationStream, Error> {
        let mut start_lsn = self.start_sln.clone();

        // create publication for all tables if not exists
        let pub_name = format!("{}_publication_for_all_tables", self.slot_name);
        let query = format!(
            "SELECT * FROM {} WHERE pubname = '{}'",
            "pg_catalog.pg_publication", pub_name
        );
        let res = client.simple_query(&query).await.unwrap();
        if res.len() <= 1 {
            let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", pub_name);
            client.simple_query(&query).await.unwrap();
        }

        // check slot exists
        let query = format!(
            "SELECT * FROM {} WHERE slot_name = '{}'",
            "pg_catalog.pg_replication_slots", self.slot_name
        );
        let res = client.simple_query(&query).await.unwrap();
        let mut slot_exists = res.len() > 1;

        // drop existing slot to create a new one if no start_lsn was provided
        if start_lsn.is_empty() && slot_exists {
            let query = format!(
                "SELECT {} ('{}')",
                "pg_drop_replication_slot", self.slot_name
            );
            client.simple_query(&query).await.unwrap();
            slot_exists = false;
        }

        // create replication slot
        if !slot_exists {
            let query = format!(
                r#"CREATE_REPLICATION_SLOT {} LOGICAL "{}""#,
                self.slot_name, "pgoutput"
            );
            let res = client.simple_query(&query).await.unwrap();
            // get the lsn for the newly created slot
            start_lsn = if let Row(row) = &res[0] {
                row.get("consistent_point").unwrap().to_string()
            } else {
                return Err(Error::MetadataError {
                    error: format!("failed in: {}", query),
                });
            };
        }

        // start replication slot
        let options = format!(
            r#"("proto_version" '{}', "publication_names" '{}')"#,
            "1", pub_name
        );
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} {}",
            self.slot_name, start_lsn, options
        );

        // todo: remove this
        self.start_sln = start_lsn;

        let copy_stream = client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();
        let stream = LogicalReplicationStream::new(copy_stream);
        Ok(stream)
    }
}