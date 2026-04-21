use std::str::FromStr;

use anyhow::bail;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use postgres_types::PgLsn;
use tokio_postgres::{
    config::ReplicationMode, replication::LogicalReplicationStream, Client, Config, NoTls,
    SimpleQueryMessage::Row,
};
use url::Url;

use dt_common::{
    config::{
        connection_auth_config::ConnectionAuthConfig,
        ssl_config::{SslConfig, SslMode},
    },
    error::Error,
    log_info, log_warn,
};

pub struct PgCdcClient {
    pub url: String,
    pub connection_auth: ConnectionAuthConfig,
    pub slot_name: String,
    pub pub_name: String,
    pub start_lsn: String,
    pub recreate_slot_if_exists: bool,
}

impl PgCdcClient {
    pub async fn connect(&mut self) -> anyhow::Result<(LogicalReplicationStream, String)> {
        let (config, ssl_config) = self.build_replication_config()?;
        let client = match ssl_config.ssl_mode {
            SslMode::Disable => {
                let (client, connection) = config.connect(NoTls).await?;
                tokio::spawn(async move {
                    log_info!("postgres replication connection starts",);
                    if let Err(e) = connection.await {
                        log_info!("postgres replication connection drops, error: {}", e);
                    }
                });
                client
            }
            _ => {
                let connector = Self::build_tls_connector(&ssl_config)?;
                let (client, connection) = config.connect(connector).await?;
                tokio::spawn(async move {
                    log_info!("postgres replication connection starts",);
                    if let Err(e) = connection.await {
                        log_info!("postgres replication connection drops, error: {}", e);
                    }
                });
                client
            }
        };
        self.start_replication(&client).await
    }

    fn build_replication_config(&self) -> anyhow::Result<(Config, SslConfig)> {
        let (sanitized_url, url_ssl_config) = Self::parse_url_ssl_config(&self.url)?;
        let mut config: Config = Config::from_str(&sanitized_url)?;
        config.replication_mode(ReplicationMode::Logical);

        let mut effective_ssl_config = url_ssl_config.unwrap_or_else(Self::disabled_ssl_config);

        match &self.connection_auth {
            ConnectionAuthConfig::Basic { username, password } => {
                config.user(username);
                if let Some(password) = password {
                    config.password(password);
                }
            }
            ConnectionAuthConfig::BasicSsl {
                username,
                password,
                ssl_config,
            } => {
                if let Some(username) = username {
                    config.user(username);
                }
                if let Some(password) = password {
                    config.password(password);
                }
                effective_ssl_config = ssl_config.clone();
            }
            ConnectionAuthConfig::NoAuth => {}
        }

        Ok((config, effective_ssl_config))
    }

    fn disabled_ssl_config() -> SslConfig {
        SslConfig {
            ssl_mode: SslMode::Disable,
            ssl_ca_path: String::new(),
        }
    }

    fn build_tls_connector(ssl_config: &SslConfig) -> anyhow::Result<MakeTlsConnector> {
        let mut builder = SslConnector::builder(SslMethod::tls())?;

        match ssl_config.ssl_mode {
            SslMode::Disable => unreachable!("disable mode should use NoTls"),
            SslMode::Require => {
                builder.set_verify(SslVerifyMode::NONE);
            }
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if ssl_config.ssl_ca_path.is_empty() {
                    bail!(
                        "ssl_ca_path is required when ssl_mode={}",
                        ssl_config.ssl_mode
                    );
                }
                builder.set_ca_file(&ssl_config.ssl_ca_path)?;
                builder.set_verify(SslVerifyMode::PEER);
            }
        }

        let mut connector = MakeTlsConnector::new(builder.build());
        if matches!(ssl_config.ssl_mode, SslMode::VerifyCa) {
            connector.set_callback(|config, _domain| {
                config.set_verify_hostname(false);
                Ok(())
            });
        }

        Ok(connector)
    }

    fn parse_url_ssl_config(url: &str) -> anyhow::Result<(String, Option<SslConfig>)> {
        let mut parsed = Url::parse(url)?;
        let mut ssl_mode = None;
        let mut ssl_ca_path = None;
        let mut other_pairs = vec![];

        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "sslmode" => ssl_mode = Some(Self::parse_url_ssl_mode(value.as_ref())?),
                "sslrootcert" => ssl_ca_path = Some(value.into_owned()),
                _ => other_pairs.push((key.into_owned(), value.into_owned())),
            }
        }

        parsed.query_pairs_mut().clear().extend_pairs(other_pairs);

        let ssl_config = ssl_mode.map(|ssl_mode| SslConfig {
            ssl_mode,
            ssl_ca_path: ssl_ca_path.unwrap_or_default(),
        });

        Ok((parsed.to_string(), ssl_config))
    }

    fn parse_url_ssl_mode(value: &str) -> anyhow::Result<SslMode> {
        match value {
            "disable" => Ok(SslMode::Disable),
            "require" | "prefer" => Ok(SslMode::Require),
            "verify-ca" | "verify_ca" => Ok(SslMode::VerifyCa),
            "verify-full" | "verify_full" => Ok(SslMode::VerifyFull),
            _ => bail!("unsupported postgres sslmode in url: {}", value),
        }
    }

    async fn prepare_slot(&self, client: &Client) -> anyhow::Result<(String, String)> {
        let mut start_lsn = self.start_lsn.clone();

        // create publication for all tables if not exists
        let pub_name = if self.pub_name.is_empty() {
            format!("{}_publication_for_all_tables", self.slot_name)
        } else {
            self.pub_name.clone()
        };
        let query = format!(
            "SELECT * FROM {} WHERE pubname = '{}'",
            "pg_catalog.pg_publication", pub_name
        );
        let res = client.simple_query(&query).await?;
        let pub_exists = res.len() > 1;
        log_info!("publication: {} exists: {}", pub_name, pub_exists);

        if !pub_exists {
            let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", pub_name);
            log_info!("execute: {}", query);
            client.simple_query(&query).await?;
        }

        // check slot exists
        let (slot_exists, confirmed_flush_lsn) = self.check_slot_status(client).await?;
        let mut create_slot = !slot_exists;

        if slot_exists {
            if confirmed_flush_lsn.is_empty() {
                // should never happen
                create_slot = true;
                log_warn!("slot exists but confirmed_flush_lsn is empty, will recreate slot");
            } else if start_lsn.is_empty() {
                log_warn!("start_lsn is empty, will use confirmed_flush_lsn");
                start_lsn = confirmed_flush_lsn;
            } else {
                let actual_lsn: PgLsn = confirmed_flush_lsn.parse().unwrap();
                let input_lsn: PgLsn = start_lsn.parse().unwrap();
                if input_lsn < actual_lsn {
                    log_warn!("start_lsn: {} is order than confirmed_flush_lsn: {}, will use confirmed_flush_lsn", 
                        start_lsn, confirmed_flush_lsn);
                    start_lsn = confirmed_flush_lsn;
                }
            }
        }

        // create replication slot
        if create_slot || self.recreate_slot_if_exists {
            // should never happen
            if slot_exists {
                let query = format!(
                    "SELECT {} ('{}')",
                    "pg_drop_replication_slot", self.slot_name
                );
                log_info!("execute: {}", query);
                client.simple_query(&query).await?;
            }

            let query = format!(
                r#"CREATE_REPLICATION_SLOT {} LOGICAL "{}""#,
                self.slot_name, "pgoutput"
            );
            log_info!("execute: {}", query);

            let res = client.simple_query(&query).await?;
            // get the lsn for the newly created slot
            start_lsn = if let Row(row) = &res[0] {
                row.get("consistent_point").unwrap().to_string()
            } else {
                bail! {Error::ExtractorError(format!(
                    "failed to create replication slot by query: {}",
                    query
                ))}
            };

            log_info!(
                "slot created, returned start_sln: {}",
                start_lsn.to_string()
            );
        }

        Ok((pub_name, start_lsn))
    }

    async fn check_slot_status(&self, client: &Client) -> anyhow::Result<(bool, String)> {
        // check slot exists
        let query = format!(
            "SELECT * FROM {} WHERE slot_name = '{}'",
            "pg_catalog.pg_replication_slots", self.slot_name
        );
        let res = client.simple_query(&query).await?;
        let slot_exists = res.len() > 1;
        log_info!("slot: {} exists: {}", self.slot_name, slot_exists);

        let mut confirmed_flush_lsn = String::new();
        if slot_exists {
            if let Row(row) = &res[0] {
                confirmed_flush_lsn = row.get("confirmed_flush_lsn").unwrap().to_string()
            }
            log_info!("slot confirmed_flush_lsn: {}", confirmed_flush_lsn);
        }
        Ok((slot_exists, confirmed_flush_lsn))
    }

    async fn start_replication(
        &mut self,
        client: &Client,
    ) -> anyhow::Result<(LogicalReplicationStream, String)> {
        let (pub_name, start_lsn) = self.prepare_slot(client).await?;

        // set extra_float_digits to max so no precision will lose
        client.simple_query("SET extra_float_digits=3").await?;
        client.simple_query("SET TIME ZONE 'UTC'").await?;

        // start replication slot
        let options = format!(
            r#"("proto_version" '{}', "publication_names" '{}')"#,
            "1", pub_name
        );
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} {}",
            self.slot_name, start_lsn, options
        );
        log_info!("execute: {}", query);

        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;
        let stream = LogicalReplicationStream::new(copy_stream);
        Ok((stream, start_lsn))
    }
}

#[cfg(test)]
mod tests {
    use dt_common::config::{connection_auth_config::ConnectionAuthConfig, ssl_config::SslMode};

    use super::PgCdcClient;

    fn build_client(url: &str, connection_auth: ConnectionAuthConfig) -> PgCdcClient {
        PgCdcClient {
            url: url.to_string(),
            connection_auth,
            slot_name: "slot".to_string(),
            pub_name: String::new(),
            start_lsn: String::new(),
            recreate_slot_if_exists: false,
        }
    }

    #[test]
    fn build_replication_config_prefers_url_ssl_when_no_ssl_override() {
        let client = build_client(
            "postgres://url_user:url_pass@localhost:5432/test_db?sslmode=require",
            ConnectionAuthConfig::Basic {
                username: "auth_user".to_string(),
                password: Some("auth_pass".to_string()),
            },
        );

        let (config, ssl_config) = client.build_replication_config().unwrap();

        assert_eq!(ssl_config.ssl_mode, SslMode::Require);
        assert_eq!(config.get_user(), Some("auth_user"));
        assert_eq!(config.get_password(), Some("auth_pass".as_bytes()));
        assert_eq!(config.get_dbname(), Some("test_db"));
    }

    #[test]
    fn build_replication_config_overrides_url_ssl_when_basic_ssl_is_present() {
        let client = build_client(
            "postgres://url_user:url_pass@localhost:5432/test_db?sslmode=require",
            ConnectionAuthConfig::BasicSsl {
                username: None,
                password: None,
                ssl_config: dt_common::config::ssl_config::SslConfig {
                    ssl_mode: SslMode::Disable,
                    ssl_ca_path: String::new(),
                },
            },
        );

        let (config, ssl_config) = client.build_replication_config().unwrap();

        assert_eq!(ssl_config.ssl_mode, SslMode::Disable);
        assert_eq!(config.get_user(), Some("url_user"));
        assert_eq!(config.get_password(), Some("url_pass".as_bytes()));
        assert_eq!(config.get_dbname(), Some("test_db"));
    }

    #[test]
    fn build_replication_config_parses_verify_full_from_url() {
        let client = build_client(
            "postgres://url_user:url_pass@localhost:5432/test_db?sslmode=verify-full&sslrootcert=%2Ftmp%2Froot.crt",
            ConnectionAuthConfig::NoAuth,
        );

        let (config, ssl_config) = client.build_replication_config().unwrap();

        assert_eq!(ssl_config.ssl_mode, SslMode::VerifyFull);
        assert_eq!(ssl_config.ssl_ca_path, "/tmp/root.crt");
        assert_eq!(config.get_user(), Some("url_user"));
        assert_eq!(config.get_password(), Some("url_pass".as_bytes()));
        assert_eq!(config.get_dbname(), Some("test_db"));
    }

    #[test]
    fn build_replication_config_defaults_to_disable_without_ssl_config() {
        let client = build_client(
            "postgres://url_user:url_pass@localhost:5432/test_db",
            ConnectionAuthConfig::NoAuth,
        );

        let (_, ssl_config) = client.build_replication_config().unwrap();

        assert_eq!(ssl_config.ssl_mode, SslMode::Disable);
    }
}
