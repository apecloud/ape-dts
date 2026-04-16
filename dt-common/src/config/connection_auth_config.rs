use anyhow::{Context, Result};
use url::Url;
use urlencoding::encode;

use crate::config::ini_loader::IniLoader;

use super::ssl_config::SslConfig;

const BASIC_AUTH_USERNAME_KEY: &str = "username";
const BASIC_AUTH_PASSWORD_KEY: &str = "password";
const SSL_ENABLE_KEY: &str = "ssl_enable";

#[derive(Clone, Debug, Default, Hash)]
pub enum ConnectionAuthConfig {
    #[default]
    NoAuth,

    Basic {
        username: String,
        password: Option<String>,
    },

    BasicSsl {
        username: String,
        password: Option<String>,
        ssl_config: SslConfig,
    },
}

impl ConnectionAuthConfig {
    pub fn from(loader: &IniLoader, section: &str) -> Self {
        let has_username = loader.contains(section, BASIC_AUTH_USERNAME_KEY);
        let ssl_enable: bool = loader.get_optional(section, SSL_ENABLE_KEY);

        let username = || loader.get_optional(section, BASIC_AUTH_USERNAME_KEY);
        let password = || {
            if loader.contains(section, BASIC_AUTH_PASSWORD_KEY) {
                Some(loader.get_optional(section, BASIC_AUTH_PASSWORD_KEY))
            } else {
                None
            }
        };
        let ssl_config = || SslConfig::from(loader, section);

        match (has_username, ssl_enable) {
            (true, true) => ConnectionAuthConfig::BasicSsl {
                username: username(),
                password: password(),
                ssl_config: ssl_config(),
            },
            (true, false) => ConnectionAuthConfig::Basic {
                username: username(),
                password: password(),
            },
            // ssl_enable without username is treated as NoAuth
            _ => ConnectionAuthConfig::NoAuth,
        }
    }

    pub fn ssl_config(&self) -> Option<&SslConfig> {
        match self {
            Self::BasicSsl { ssl_config, .. } => Some(ssl_config),
            _ => None,
        }
    }

    pub fn merge_url_with_auth(original_url: &str, connection_auth: &Self) -> Result<String> {
        let mut parsed_url = Url::parse(original_url)
            .with_context(|| format!("failed to parse URL: {}", original_url))?;

        match connection_auth {
            ConnectionAuthConfig::Basic { username, password }
            | ConnectionAuthConfig::BasicSsl {
                username, password, ..
            } => {
                if !username.is_empty() {
                    parsed_url
                        .set_username(encode(username).into_owned().as_str())
                        .map_err(|_| anyhow::anyhow!("failed to set username in URL"))?;
                }

                if let Some(pwd) = password {
                    if !pwd.is_empty() {
                        parsed_url
                            .set_password(Some(encode(pwd).into_owned().as_str()))
                            .map_err(|_| anyhow::anyhow!("failed to set password in URL"))?;
                    }
                }
            }
            ConnectionAuthConfig::NoAuth => {}
        }

        Ok(parsed_url.to_string())
    }
}
