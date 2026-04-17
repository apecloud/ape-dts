use sqlx::{
    mysql::{MySqlConnectOptions, MySqlSslMode},
    postgres::{PgConnectOptions, PgSslMode},
};
use strum::{Display, EnumString};

use super::ini_loader::IniLoader;

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

#[derive(Clone, Debug, Hash)]
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
}
