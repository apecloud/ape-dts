use std::{fs, sync::Arc};

use anyhow::{bail, Context};
use mongodb::options::{ClientOptions, Tls, TlsOptions};
use redis::{Client as RedisClient, TlsCertificates};
use rustls::ClientConfig;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgSslMode},
};
use strum::{Display, EnumString};
use url::Url;

use super::ini_loader::IniLoader;
use crate::utils::tls_util::{
    load_root_cert_store, NoCertificateVerification, NoHostnameVerification,
};

/// Client-side TLS policy. Requiring a client certificate is a separate server policy.
#[derive(Clone, Debug, Display, EnumString, Hash, PartialEq, Eq)]
pub enum SslMode {
    /// Use a plaintext connection without TLS encryption.
    #[strum(serialize = "disable")]
    Disable,
    /// Require TLS encryption without verifying the server certificate or hostname.
    #[strum(serialize = "require")]
    Require,
    /// Require TLS and verify the server certificate chain against a trusted CA.
    /// This mode does not require a matching hostname.
    #[strum(serialize = "verify_ca")]
    VerifyCa,
    /// Require TLS and verify both the server certificate chain and hostname/IP.
    #[strum(serialize = "verify_full")]
    VerifyFull,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SslConfig {
    pub ssl_mode: SslMode,
    pub ssl_ca_path: String,
}

impl SslConfig {
    pub fn from(loader: &IniLoader, section: &str) -> Self {
        SslConfig {
            ssl_mode: loader.get_required(section, "ssl_mode"),
            ssl_ca_path: loader.get_optional(section, "ssl_ca_path"),
        }
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

    /// Normalize TLS settings for both redis-rs and the PSYNC client without loading files.
    pub fn apply_redis_url(&self, mut url: Url) -> anyhow::Result<Url> {
        match url.scheme() {
            "redis" => {}
            "rediss" => {
                if let Some(fragment) = url.fragment() {
                    if fragment != "insecure" {
                        bail!("unsupported Redis URL fragment: {}", fragment);
                    }
                }
            }
            scheme => bail!("unsupported Redis URL scheme: {}", scheme),
        }
        let (scheme, fragment) = match self.ssl_mode {
            SslMode::Disable => ("redis", None),
            SslMode::Require => ("rediss", Some("insecure")),
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    bail!(
                        "ssl_ca_path is required when Redis ssl_mode={}",
                        self.ssl_mode
                    );
                }
                ("rediss", None)
            }
        };
        url.set_scheme(scheme)
            .map_err(|_| anyhow::anyhow!("failed to set Redis URL scheme"))?;
        url.set_fragment(fragment);
        Ok(url)
    }

    pub fn apply_redis(&self, url: &str) -> anyhow::Result<RedisClient> {
        let url = self.apply_redis_url(Url::parse(url)?)?;
        if matches!(self.ssl_mode, SslMode::Disable | SslMode::Require) {
            return RedisClient::open(url.as_str()).map_err(Into::into);
        }
        let root_cert = fs::read(&self.ssl_ca_path).with_context(|| {
            format!("failed to read Redis CA certificate: {}", self.ssl_ca_path)
        })?;
        let client = RedisClient::build_with_tls(
            url.as_str(),
            TlsCertificates {
                client_tls: None,
                root_cert: Some(root_cert),
            },
        )?;
        if self.ssl_mode == SslMode::VerifyCa {
            let mut options = client.get_connection_info().clone();
            options.addr.set_danger_accept_invalid_hostnames(true);
            return RedisClient::open(options).map_err(Into::into);
        }
        Ok(client)
    }

    /// Convert SSL settings into a rustls client configuration without client authentication.
    pub fn to_rustls_client_config(&self) -> anyhow::Result<ClientConfig> {
        match &self.ssl_mode {
            SslMode::Disable => bail!("can not build a TLS client when ssl_mode=disable"),
            SslMode::Require => {
                let mut config = ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(
                        Arc::new(NoCertificateVerification::default()),
                    )
                    .with_no_client_auth();
                config.enable_sni = false;
                Ok(config)
            }
            SslMode::VerifyCa => {
                let verifier =
                    NoHostnameVerification::new(load_root_cert_store(&self.ssl_ca_path)?)?;
                Ok(ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(verifier))
                    .with_no_client_auth())
            }
            SslMode::VerifyFull => Ok(ClientConfig::builder()
                .with_root_certificates(load_root_cert_store(&self.ssl_ca_path)?)
                .with_no_client_auth()),
        }
    }

    pub fn apply_mongo(&self, mut options: ClientOptions) -> anyhow::Result<ClientOptions> {
        options.tls = match self.ssl_mode {
            SslMode::Disable => Some(Tls::Disabled),
            SslMode::Require => Some(Tls::Enabled(
                TlsOptions::builder()
                    .allow_invalid_certificates(true)
                    .build(),
            )),
            SslMode::VerifyCa | SslMode::VerifyFull => {
                if self.ssl_ca_path.is_empty() {
                    bail!(
                        "ssl_ca_path is required when MongoDB ssl_mode={}",
                        self.ssl_mode
                    )
                }
                // The MongoDB rustls backend also verifies the hostname in verify_ca mode.
                // TODO: use allow_invalid_hostnames for verify_ca when the rustls backend
                // supports it. The driver currently exposes this option only with OpenSSL.
                let ca_file_path = std::path::PathBuf::from(&self.ssl_ca_path);
                Some(Tls::Enabled(
                    TlsOptions::builder().ca_file_path(ca_file_path).build(),
                ))
            }
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
