use anyhow::bail;
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgSslMode},
};
use strum::{Display, EnumString};
use tiberius::{Config as MssqlConfig, EncryptionLevel};

use super::ini_loader::IniLoader;
use crate::error::DtError;

#[derive(Clone, Debug, Display, EnumString, Hash, PartialEq, Eq)]
pub enum SslMode {
    #[strum(serialize = "disable")]
    Disable,
    #[strum(serialize = "require")]
    Require,
    #[strum(serialize = "verify_ca")]
    VerifyCa,
    #[strum(serialize = "verify_full")]
    VerifyFull,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SslConfig {
    pub ssl_mode: SslMode,
    pub ssl_ca_path: String,
}

impl SslConfig {
    pub fn from(loader: &IniLoader, section: &str) -> anyhow::Result<Self> {
        Ok(SslConfig {
            ssl_mode: loader.get_required(section, "ssl_mode")?,
            ssl_ca_path: loader.get_optional(section, "ssl_ca_path")?,
        })
    }

    pub fn apply_mysql(&self, mut options: MySqlConnectOptions) -> MySqlConnectOptions {
        let mode = match self.ssl_mode {
            SslMode::Disable => MySqlSslMode::Disabled,
            SslMode::Require => MySqlSslMode::Required,
            SslMode::VerifyCa => MySqlSslMode::VerifyCa,
            SslMode::VerifyFull => MySqlSslMode::VerifyIdentity,
        };
        options = options.ssl_mode(mode);
        if !matches!(mode, MySqlSslMode::Disabled) && !self.ssl_ca_path.is_empty() {
            options = options.ssl_ca(&self.ssl_ca_path);
        }
        options
    }

    pub fn apply_pg(&self, mut options: PgConnectOptions) -> PgConnectOptions {
        let mode = match self.ssl_mode {
            SslMode::Disable => PgSslMode::Disable,
            SslMode::Require => PgSslMode::Require,
            SslMode::VerifyCa => PgSslMode::VerifyCa,
            SslMode::VerifyFull => PgSslMode::VerifyFull,
        };
        options = options.ssl_mode(mode);
        if !matches!(mode, PgSslMode::Disable) && !self.ssl_ca_path.is_empty() {
            options = options.ssl_root_cert(&self.ssl_ca_path);
        }
        options
    }

    pub fn apply_mssql(&self, options: &mut MssqlConfig) -> anyhow::Result<()> {
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
            SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    return Err(DtError::invalid_config(
                        "config ssl_ca_path is required when ssl_mode=verify_full",
                    )
                    .into());
                }
                options.encryption(EncryptionLevel::Required);
                options.trust_cert_ca(&self.ssl_ca_path);
            }
            SslMode::VerifyCa => {
                return Err(DtError::invalid_config(
                    "MSSQL does not support ssl_mode=verify_ca; use require or verify_full",
                )
                .into())
            }
        }
        Ok(())
    }

    pub fn apply_mongo(&self, mut options: ClientOptions) -> anyhow::Result<ClientOptions> {
        options.tls = match self.ssl_mode {
            SslMode::Disable => Some(Tls::Disabled),
            SslMode::Require => Some(Tls::Enabled(
                TlsOptions::builder()
                    .allow_invalid_certificates(true)
                    .build(),
            )),
            SslMode::VerifyCa => {
                if self.ssl_ca_path.is_empty() {
                    bail!(DtError::invalid_config(
                        "ssl_ca_path is required when MongoDB ssl_mode=verify_ca"
                    ))
                }
                // The MongoDB rustls backend also verifies the hostname in verify_ca mode.
                let ca_file_path = std::path::PathBuf::from(&self.ssl_ca_path);
                Some(Tls::Enabled(
                    TlsOptions::builder().ca_file_path(ca_file_path).build(),
                ))
            }
            SslMode::VerifyFull => bail!(DtError::invalid_config(
                "MongoDB ssl_mode=verify_full is not supported"
            )),
        };
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use mongodb::options::{ClientOptions, Tls};

    use super::{SslConfig, SslMode};

    #[tokio::test]
    async fn apply_mongo_tls_mode_table() {
        enum Expected {
            Disabled,
            Require,
            VerifyCa,
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
                expected: Expected::VerifyCa,
            },
            Case {
                name: "verify CA requires CA path",
                url: "mongodb://localhost/",
                ssl_mode: SslMode::VerifyCa,
                ssl_ca_path: "",
                expected: Expected::Error("ssl_ca_path is required"),
            },
            Case {
                name: "verify full is unsupported",
                url: "mongodb://localhost/",
                ssl_mode: SslMode::VerifyFull,
                ssl_ca_path: ".",
                expected: Expected::Error("ssl_mode=verify_full is not supported"),
            },
        ];

        for case in cases {
            let options = ClientOptions::parse(case.url).await.unwrap();
            let result = (SslConfig {
                ssl_mode: case.ssl_mode,
                ssl_ca_path: case.ssl_ca_path.to_string(),
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
                Expected::VerifyCa => match result.unwrap().tls {
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
