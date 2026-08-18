use anyhow::{Context, Result};
use url::Url;
use urlencoding::encode;

use super::ssl_config::SslConfig;
use crate::{config::ini_loader::IniLoader, error::DtError};

const BASIC_AUTH_USERNAME_KEY: &str = "username";
const BASIC_AUTH_PASSWORD_KEY: &str = "password";
const SSL_MODE_KEY: &str = "ssl_mode";

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
pub enum ConnectionAuthConfig {
    #[default]
    NoAuth,

    Basic {
        username: String,
        password: Option<String>,
    },

    BasicSsl {
        username: Option<String>,
        password: Option<String>,
        ssl_config: SslConfig,
    },
}

impl ConnectionAuthConfig {
    pub fn from(loader: &IniLoader, section: &str) -> Result<Self> {
        let username = || loader.get_optional(section, BASIC_AUTH_USERNAME_KEY);
        let password = || {
            if loader.contains(section, BASIC_AUTH_PASSWORD_KEY) {
                loader
                    .get_optional(section, BASIC_AUTH_PASSWORD_KEY)
                    .map(Some)
            } else {
                Ok(None)
            }
        };

        if loader.contains(section, SSL_MODE_KEY) {
            let username = if loader.contains(section, BASIC_AUTH_USERNAME_KEY) {
                Some(username()?)
            } else {
                None
            };
            return Ok(ConnectionAuthConfig::BasicSsl {
                username,
                password: password()?,
                ssl_config: SslConfig::from(loader, section)?,
            });
        }

        if loader.contains(section, BASIC_AUTH_USERNAME_KEY) {
            return Ok(ConnectionAuthConfig::Basic {
                username: username()?,
                password: password()?,
            });
        }

        Ok(ConnectionAuthConfig::NoAuth)
    }

    pub fn ssl_config(&self) -> Option<&SslConfig> {
        match self {
            Self::BasicSsl { ssl_config, .. } => Some(ssl_config),
            _ => None,
        }
    }

    pub fn merge_url_with_auth(original_url: &str, connection_auth: &Self) -> Result<String> {
        let mut parsed_url = Url::parse(original_url).context(DtError::invalid_config(
            "the configured database connection URL could not be parsed",
        ))?;

        match connection_auth {
            ConnectionAuthConfig::Basic { username, password }
            | ConnectionAuthConfig::BasicSsl {
                username: Some(username),
                password,
                ..
            } => {
                if !username.is_empty() {
                    parsed_url
                        .set_username(encode(username).into_owned().as_str())
                        .map_err(|_| {
                            DtError::InvalidConfig(
                                "database connection URL does not support a username".to_string(),
                            )
                        })?;
                }

                if let Some(pwd) = password {
                    if !pwd.is_empty() {
                        parsed_url
                            .set_password(Some(encode(pwd).into_owned().as_str()))
                            .map_err(|_| {
                                DtError::InvalidConfig(
                                    "database connection URL does not support a password"
                                        .to_string(),
                                )
                            })?;
                    }
                }
            }
            _ => {}
        }

        Ok(parsed_url.to_string())
    }
}
