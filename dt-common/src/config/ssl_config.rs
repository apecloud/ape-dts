use super::ini_loader::IniLoader;

#[derive(Clone, Debug, Default, Hash)]
pub struct SslConfig {
    /// Path to the CA certificate file (PEM format).
    pub ssl_ca_path: String,
    /// Path to the client certificate file (PEM format).
    pub ssl_cert_path: String,
    /// Path to the client private key file (PEM format).
    pub ssl_key_path: String,
}

impl SslConfig {
    pub fn from(loader: &IniLoader, section: &str) -> Self {
        Self {
            ssl_ca_path: loader.get_optional(section, "ssl_ca_path"),
            ssl_cert_path: loader.get_optional(section, "ssl_cert_path"),
            ssl_key_path: loader.get_optional(section, "ssl_key_path"),
        }
    }
}
