use anyhow::{Context, Result};
use url::Url;
use urlencoding::encode;

use crate::config::ini_loader::IniLoader;

const BASIC_AUTH_USERNAME_KEY: &str = "username";
const BASIC_AUTH_PASSWORD_KEY: &str = "password";

#[derive(Clone, Debug, Default, Hash)]
pub enum ConnectionAuthConfig {
    #[default]
    NoAuth,

    Basic {
        username: String,
        password: Option<String>,
    },
}

impl ConnectionAuthConfig {
    pub fn from(loader: &IniLoader, section: &str) -> Self {
        if loader.contains(section, BASIC_AUTH_USERNAME_KEY) {
            ConnectionAuthConfig::Basic {
                username: loader.get_optional(section, BASIC_AUTH_USERNAME_KEY),
                password: if loader.contains(section, BASIC_AUTH_PASSWORD_KEY) {
                    Some(loader.get_optional(section, BASIC_AUTH_PASSWORD_KEY))
                } else {
                    None
                },
            }
        } else {
            ConnectionAuthConfig::NoAuth
        }
    }

    pub fn merge_url_with_auth(original_url: &str, connection_auth: &Self) -> Result<String> {
        let mut parsed_url = Url::parse(original_url)
            .with_context(|| format!("failed to parse URL: {}", original_url))?;

        match connection_auth {
            ConnectionAuthConfig::Basic { username, password } => {
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
