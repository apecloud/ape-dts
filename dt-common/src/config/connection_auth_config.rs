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
}
