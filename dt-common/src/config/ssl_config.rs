use anyhow::{bail, Context};
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgSslMode},
};
use strum::{Display, EnumString};
use tiberius::{Config as MssqlConfig, EncryptionLevel};

use super::ini_loader::IniLoader;
use crate::error::DtError;

#[derive(Clone, Debug, Default, Display, EnumString, Hash, PartialEq, Eq)]
pub enum SslMode {
    #[default]
    #[strum(serialize = "disable")]
    Disable,
    #[strum(serialize = "require")]
    Require,
    #[strum(serialize = "verify_ca")]
    VerifyCa,
    #[strum(serialize = "verify_full")]
    VerifyFull,
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct SslConfig {
    pub ssl_mode: SslMode,
    pub ssl_ca_path: String,
    pub ssl_client_cert_path: String,
    pub ssl_client_key_path: String,
    pub ssl_allow_invalid_hostnames: bool,
}

impl SslConfig {
    pub fn build_openssl_connector(&self) -> anyhow::Result<openssl::ssl::SslConnector> {
        use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
        self.validate_client_identity()?;
        let mut builder = SslConnector::builder(SslMethod::tls_client())?;
        match self.ssl_mode {
            SslMode::Disable => bail!(DtError::invalid_config(
                "TLS connector requested while ssl_mode=disable"
            )),
            SslMode::Require => builder.set_verify(SslVerifyMode::NONE),
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    bail!(DtError::invalid_config(
                        "ssl_ca_path is required by the selected TLS mode"
                    ));
                }
                builder
                    .set_ca_file(&self.ssl_ca_path)
                    .context("failed to load TLS CA certificate")?;
                builder.set_verify(SslVerifyMode::PEER);
            }
        }
        if !self.ssl_client_cert_path.is_empty() {
            builder
                .set_certificate_chain_file(&self.ssl_client_cert_path)
                .context("failed to load TLS client certificate")?;
            builder
                .set_private_key_file(self.client_key_path(), SslFiletype::PEM)
                .context("failed to load TLS client private key")?;
            builder
                .check_private_key()
                .context("TLS client certificate/private key mismatch")?;
        }
        Ok(builder.build())
    }

    pub fn from(loader: &IniLoader, section: &str) -> anyhow::Result<Self> {
        Ok(SslConfig {
            ssl_mode: loader.get_required(section, "ssl_mode")?,
            ssl_ca_path: loader.get_optional(section, "ssl_ca_path")?,
            ssl_client_cert_path: loader.get_optional(section, "ssl_client_cert_path")?,
            ssl_client_key_path: loader.get_optional(section, "ssl_client_key_path")?,
            ssl_allow_invalid_hostnames: loader
                .get_optional(section, "ssl_allow_invalid_hostnames")?,
        })
    }

    pub fn validate_client_identity(&self) -> anyhow::Result<()> {
        if self.ssl_mode != SslMode::Disable
            && self.ssl_client_cert_path.is_empty()
            && !self.ssl_client_key_path.is_empty()
        {
            bail!(DtError::invalid_config(
                "ssl_client_key_path requires ssl_client_cert_path"
            ));
        }
        Ok(())
    }

    /// A combined PEM identity can be used in place of separate certificate/key files.
    pub fn client_key_path(&self) -> &str {
        if self.ssl_client_key_path.is_empty() {
            &self.ssl_client_cert_path
        } else {
            &self.ssl_client_key_path
        }
    }

    pub fn apply_mysql(
        &self,
        mut options: MySqlConnectOptions,
    ) -> anyhow::Result<MySqlConnectOptions> {
        self.validate_client_identity()?;
        let mode = match self.ssl_mode {
            SslMode::Disable => MySqlSslMode::Disabled,
            SslMode::Require => MySqlSslMode::Required,
            SslMode::VerifyCa | SslMode::VerifyFull if self.ssl_allow_invalid_hostnames => {
                MySqlSslMode::VerifyCa
            }
            SslMode::VerifyCa | SslMode::VerifyFull => MySqlSslMode::VerifyIdentity,
        };
        options = options.ssl_mode(mode);
        if !matches!(mode, MySqlSslMode::Disabled) && !self.ssl_ca_path.is_empty() {
            options = options.ssl_ca(&self.ssl_ca_path);
        }
        if !matches!(mode, MySqlSslMode::Disabled) && !self.ssl_client_cert_path.is_empty() {
            options = options
                .ssl_client_cert(&self.ssl_client_cert_path)
                .ssl_client_key(self.client_key_path());
        }
        Ok(options)
    }

    pub fn apply_pg(&self, mut options: PgConnectOptions) -> anyhow::Result<PgConnectOptions> {
        self.validate_client_identity()?;
        let mode = match self.ssl_mode {
            SslMode::Disable => PgSslMode::Disable,
            SslMode::Require => PgSslMode::Require,
            SslMode::VerifyCa | SslMode::VerifyFull if self.ssl_allow_invalid_hostnames => {
                PgSslMode::VerifyCa
            }
            SslMode::VerifyCa | SslMode::VerifyFull => PgSslMode::VerifyFull,
        };
        options = options.ssl_mode(mode);
        if !matches!(mode, PgSslMode::Disable) && !self.ssl_ca_path.is_empty() {
            options = options.ssl_root_cert(&self.ssl_ca_path);
        }
        if !matches!(mode, PgSslMode::Disable) && !self.ssl_client_cert_path.is_empty() {
            options = options
                .ssl_client_cert(&self.ssl_client_cert_path)
                .ssl_client_key(self.client_key_path());
        }
        Ok(options)
    }

    pub fn apply_mssql(&self, options: &mut MssqlConfig) -> anyhow::Result<()> {
        if self.ssl_mode != SslMode::Disable
            && (!self.ssl_client_cert_path.is_empty() || !self.ssl_client_key_path.is_empty())
        {
            bail!(DtError::invalid_config(
                "MSSQL does not support TLS client certificates (ssl_client_cert_path/ssl_client_key_path)"
            ));
        }
        match self.ssl_mode {
            // Tiberius `Off` still uses TLS for the login exchange. `disable`
            // means no TLS at all, which Tiberius calls `NotSupported`.
            SslMode::Disable => options.encryption(EncryptionLevel::NotSupported),
            SslMode::Require => {
                options.encryption(EncryptionLevel::Required);
                // `require` guarantees encryption but intentionally skips
                // certificate-chain and hostname validation.
                options.trust_cert();
            }
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    return Err(DtError::invalid_config(
                        "config ssl_ca_path is required when ssl_mode=verify_ca or verify_full",
                    )
                    .into());
                }
                options.encryption(EncryptionLevel::Required);
                options.trust_cert_ca(&self.ssl_ca_path);
            }
        }
        Ok(())
    }

    pub fn apply_mongo(&self, mut options: ClientOptions) -> anyhow::Result<ClientOptions> {
        self.validate_client_identity()?;
        options.tls = match self.ssl_mode {
            SslMode::Disable => Some(Tls::Disabled),
            SslMode::Require => Some(Tls::Enabled(
                TlsOptions::builder()
                    .allow_invalid_certificates(true)
                    .build(),
            )),
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    bail!(DtError::invalid_config(format!(
                        "ssl_ca_path is required when MongoDB ssl_mode={}",
                        self.ssl_mode
                    )))
                }
                // The MongoDB rustls backend also verifies the hostname in verify_ca mode.
                let ca_file_path = std::path::PathBuf::from(&self.ssl_ca_path);
                Some(Tls::Enabled(
                    TlsOptions::builder().ca_file_path(ca_file_path).build(),
                ))
            }
        };
        if let Some(Tls::Enabled(tls)) = &mut options.tls {
            if !self.ssl_client_cert_path.is_empty() {
                if self.client_key_path() != self.ssl_client_cert_path {
                    bail!(DtError::invalid_config(
                        "MongoDB requires a combined certificate/private-key PEM in ssl_client_cert_path; leave ssl_client_key_path empty"
                    ));
                }
                tls.cert_key_file_path = Some((&self.ssl_client_cert_path).into());
            }
        }
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use mongodb::options::{ClientOptions, Tls};
    use sqlx::{
        mysql::{MySqlConnectOptions, MySqlSslMode},
        postgres::{PgConnectOptions, PgSslMode},
        ConnectOptions,
    };

    use super::{SslConfig, SslMode};

    #[test]
    fn parse_client_identity_and_hostname_flag() {
        for (value, expected) in [(None, false), (Some("false"), false), (Some("true"), true)] {
            let mut ini = configparser::ini::Ini::new();
            ini.read("[extractor]\nssl_mode=verify_ca\nssl_client_cert_path=client.crt\nssl_client_key_path=client.key\n".into()).unwrap();
            if let Some(value) = value {
                ini.set(
                    "extractor",
                    "ssl_allow_invalid_hostnames",
                    Some(value.into()),
                );
            }
            let config = SslConfig::from(&super::IniLoader { ini }, "extractor").unwrap();
            assert_eq!(config.ssl_client_cert_path, "client.crt");
            assert_eq!(config.ssl_client_key_path, "client.key");
            assert_eq!(config.ssl_allow_invalid_hostnames, expected);
        }
    }

    #[test]
    fn sqlx_tls_mapping_table() {
        for (mode, allow, mysql_mode, pg_mode) in [
            (
                SslMode::Disable,
                false,
                MySqlSslMode::Disabled,
                PgSslMode::Disable,
            ),
            (
                SslMode::Require,
                false,
                MySqlSslMode::Required,
                PgSslMode::Require,
            ),
            (
                SslMode::VerifyCa,
                false,
                MySqlSslMode::VerifyIdentity,
                PgSslMode::VerifyFull,
            ),
            (
                SslMode::VerifyFull,
                false,
                MySqlSslMode::VerifyIdentity,
                PgSslMode::VerifyFull,
            ),
            (
                SslMode::VerifyCa,
                true,
                MySqlSslMode::VerifyCa,
                PgSslMode::VerifyCa,
            ),
            (
                SslMode::VerifyFull,
                true,
                MySqlSslMode::VerifyCa,
                PgSslMode::VerifyCa,
            ),
        ] {
            let config = SslConfig {
                ssl_mode: mode.clone(),
                ssl_ca_path: "ca.crt".into(),
                ssl_client_cert_path: "client.crt".into(),
                ssl_client_key_path: "client.key".into(),
                ssl_allow_invalid_hostnames: allow,
            };
            let mysql = config.apply_mysql(MySqlConnectOptions::new()).unwrap();
            let pg = config.apply_pg(PgConnectOptions::new()).unwrap();
            assert_eq!(
                std::mem::discriminant(&mysql.get_ssl_mode()),
                std::mem::discriminant(&mysql_mode)
            );
            assert_eq!(
                std::mem::discriminant(&pg.get_ssl_mode()),
                std::mem::discriminant(&pg_mode)
            );
            if mode != SslMode::Disable {
                for options in [mysql.to_url_lossy(), pg.to_url_lossy()] {
                    let values: Vec<_> =
                        options.query_pairs().map(|(_, v)| v.into_owned()).collect();
                    assert!(
                        values.contains(&"file: client.crt".to_string()),
                        "{options}"
                    );
                    assert!(
                        values.contains(&"file: client.key".to_string()),
                        "{options}"
                    );
                }
            }
        }
    }

    #[test]
    fn client_identity_validation() {
        let config = SslConfig {
            ssl_mode: SslMode::Require,
            ssl_client_key_path: "client.key".into(),
            ..SslConfig::default()
        };
        assert!(config.validate_client_identity().is_err());
        let combined = SslConfig {
            ssl_client_cert_path: "client.pem".into(),
            ..SslConfig::default()
        };
        assert_eq!(combined.client_key_path(), "client.pem");
        assert!(config.apply_mssql(&mut tiberius::Config::new()).is_err());
    }

    #[tokio::test]
    async fn mongo_combined_identity_and_unsupported_hostname_flag() {
        for mode in [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull] {
            let config = SslConfig {
                ssl_mode: mode,
                ssl_ca_path: "ca.crt".into(),
                ssl_client_cert_path: "client.pem".into(),
                ssl_allow_invalid_hostnames: true,
                ..SslConfig::default()
            };
            let options = config.apply_mongo(ClientOptions::default()).unwrap();
            match options.tls.unwrap() {
                Tls::Enabled(tls) => {
                    assert_eq!(
                        tls.cert_key_file_path.as_deref(),
                        Some(Path::new("client.pem"))
                    );
                    assert_eq!(
                        tls.allow_invalid_certificates == Some(true),
                        config.ssl_mode == SslMode::Require
                    );
                }
                _ => panic!("TLS should be enabled"),
            }
            let separate = SslConfig {
                ssl_client_key_path: "client.key".into(),
                ..config
            };
            assert!(separate.apply_mongo(ClientOptions::default()).is_err());
        }
    }

    #[tokio::test]
    async fn apply_mongo_tls_mode_table() {
        enum Expected {
            Disabled,
            Require,
            Verified,
            Error(&'static str),
        }

        struct Case {
            name: &'static str,
            url: &'static str,
            ssl_mode: SslMode,
            ssl_ca_path: &'static str,
            expected: Expected,
        }

        let cases = [
            Case {
                name: "disable overrides TLS URL",
                url: "mongodb://localhost/?tls=true&tlsInsecure=true",
                ssl_mode: SslMode::Disable,
                ssl_ca_path: "",
                expected: Expected::Disabled,
            },
            Case {
                name: "require overrides plaintext URL",
                url: "mongodb://localhost/?tls=false",
                ssl_mode: SslMode::Require,
                ssl_ca_path: "",
                expected: Expected::Require,
            },
            Case {
                name: "verify CA also verifies hostname with rustls",
                url: "mongodb://localhost/?tls=false",
                ssl_mode: SslMode::VerifyCa,
                ssl_ca_path: ".",
                expected: Expected::Verified,
            },
            Case {
                name: "verify CA requires CA path",
                url: "mongodb://localhost/",
                ssl_mode: SslMode::VerifyCa,
                ssl_ca_path: "",
                expected: Expected::Error("ssl_ca_path is required"),
            },
            Case {
                name: "verify full overrides insecure URL",
                url: "mongodb://localhost/?tls=true&tlsInsecure=true",
                ssl_mode: SslMode::VerifyFull,
                ssl_ca_path: ".",
                expected: Expected::Verified,
            },
            Case {
                name: "verify full requires CA path",
                url: "mongodb://localhost/",
                ssl_mode: SslMode::VerifyFull,
                ssl_ca_path: "",
                expected: Expected::Error("ssl_ca_path is required"),
            },
        ];

        for case in cases {
            let options = ClientOptions::parse(case.url).await.unwrap();
            let result = (SslConfig {
                ssl_mode: case.ssl_mode,
                ssl_ca_path: case.ssl_ca_path.to_string(),
                ..SslConfig::default()
            })
            .apply_mongo(options);

            match case.expected {
                Expected::Disabled => assert!(
                    matches!(result.unwrap().tls, Some(Tls::Disabled)),
                    "{}",
                    case.name
                ),
                Expected::Require => match result.unwrap().tls {
                    Some(Tls::Enabled(tls)) => {
                        assert_eq!(tls.allow_invalid_certificates, Some(true), "{}", case.name);
                        assert!(tls.ca_file_path.is_none(), "{}", case.name);
                    }
                    actual => panic!("{}: unexpected TLS options: {:?}", case.name, actual),
                },
                Expected::Verified => match result.unwrap().tls {
                    Some(Tls::Enabled(tls)) => {
                        assert_eq!(tls.allow_invalid_certificates, None, "{}", case.name);
                        assert_eq!(
                            tls.ca_file_path.as_deref(),
                            Some(Path::new(".")),
                            "{}",
                            case.name
                        );
                    }
                    actual => panic!("{}: unexpected TLS options: {:?}", case.name, actual),
                },
                Expected::Error(message) => {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains(message), "{}: {}", case.name, error);
                }
            }
        }
    }
}
