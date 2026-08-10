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
            SslMode::Require => options.encryption(EncryptionLevel::Required),
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
}
