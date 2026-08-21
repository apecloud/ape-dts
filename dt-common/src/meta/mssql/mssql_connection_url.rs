use std::collections::HashMap;

use tiberius::{AuthMethod, Config, EncryptionLevel};
use url::Url;

use crate::error::DtError;

const SQLSERVER_SCHEME: &str = "sqlserver";
const MSSQL_SCHEME: &str = "mssql";

const USER_ID: &str = "user id";
const PASSWORD: &str = "password";
const PORT: &str = "port";
const DATABASE: &str = "database";
const ENCRYPT: &str = "encrypt";
const TRUST_SERVER_CERTIFICATE: &str = "trustservercertificate";
const TRUST_SERVER_CERTIFICATE_CA: &str = "trustservercertificateca";
const APP_NAME: &str = "app name";
const APPLICATION_INTENT: &str = "applicationintent";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UrlEncryption {
    Off,
    Required,
    Disabled,
}

#[derive(Debug, PartialEq, Eq)]
struct MssqlUrlParts {
    host: String,
    port: Option<u16>,
    instance: Option<String>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    application_name: Option<String>,
    encryption: UrlEncryption,
    trust_server_certificate: bool,
    trust_server_certificate_ca: Option<String>,
    readonly: bool,
}

pub struct MssqlConnectionUrl;

impl MssqlConnectionUrl {
    /// Parses go-mssqldb-style URLs. `None` means the value should be handled by
    /// another connection-string parser.
    pub fn try_parse_to_config(connection_url: &str) -> anyhow::Result<Option<Config>> {
        Ok(MssqlUrlParts::try_parse(connection_url)?.map(MssqlUrlParts::into_config))
    }
}

impl MssqlUrlParts {
    fn try_parse(connection_url: &str) -> anyhow::Result<Option<Self>> {
        if !connection_url.starts_with("sqlserver://") && !connection_url.starts_with("mssql://") {
            return Ok(None);
        }

        let url = Url::parse(connection_url).map_err(|_| {
            DtError::invalid_config("MSSQL connection URL has an invalid URL format")
        })?;
        if !matches!(url.scheme(), SQLSERVER_SCHEME | MSSQL_SCHEME) {
            return Err(DtError::invalid_config(
                "MSSQL connection URL scheme must be sqlserver or mssql",
            )
            .into());
        }
        if url.fragment().is_some() {
            return Err(DtError::invalid_config(
                "MSSQL connection URL must not contain a fragment",
            )
            .into());
        }

        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| DtError::invalid_config("MSSQL connection URL must contain a host"))?
            .to_string();
        let instance = Self::parse_instance(&url)?;
        let mut params = Self::parse_query(&url)?;

        let url_username = (!url.username().is_empty())
            .then(|| Self::decode_component(url.username(), "username"))
            .transpose()?;
        let url_password = url
            .password()
            .map(|password| Self::decode_component(password, "password"))
            .transpose()?;

        Self::reject_duplicate_url_value(&params, USER_ID, url_username.is_some())?;
        Self::reject_duplicate_url_value(&params, PASSWORD, url_password.is_some())?;
        Self::reject_duplicate_url_value(&params, PORT, url.port().is_some())?;

        let username = url_username.or_else(|| params.remove(USER_ID));
        let password = url_password.or_else(|| params.remove(PASSWORD));
        if username.as_deref().is_none_or(str::is_empty) && password.is_some() {
            return Err(DtError::invalid_config(
                "MSSQL connection URL password requires a username",
            )
            .into());
        }

        let port = match (url.port(), params.remove(PORT)) {
            (port @ Some(_), None) => port,
            (None, Some(port)) => Some(Self::parse_port(&port)?),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("duplicate port was rejected"),
        };
        let database = params.remove(DATABASE).filter(|value| !value.is_empty());
        let application_name = params.remove(APP_NAME).filter(|value| !value.is_empty());
        let encryption = params
            .remove(ENCRYPT)
            .map(|value| Self::parse_encryption(&value))
            .transpose()?
            .unwrap_or(UrlEncryption::Off);
        let trust_server_certificate = params
            .remove(TRUST_SERVER_CERTIFICATE)
            .map(|value| Self::parse_bool(TRUST_SERVER_CERTIFICATE, &value))
            .transpose()?
            .unwrap_or(false);
        let trust_server_certificate_ca = params
            .remove(TRUST_SERVER_CERTIFICATE_CA)
            .filter(|value| !value.is_empty());
        if trust_server_certificate && trust_server_certificate_ca.is_some() {
            return Err(DtError::invalid_config(
                "MSSQL connection URL cannot set both trustservercertificate and trustservercertificateca",
            )
            .into());
        }

        let readonly = params
            .remove(APPLICATION_INTENT)
            .map(|value| Self::parse_application_intent(&value))
            .transpose()?
            .unwrap_or(false);
        if readonly && database.is_none() {
            return Err(DtError::invalid_config(
                "MSSQL connection URL database is required when applicationintent=ReadOnly",
            )
            .into());
        }

        debug_assert!(params.is_empty());
        Ok(Some(Self {
            host,
            port,
            instance,
            database,
            username,
            password,
            application_name,
            encryption,
            trust_server_certificate,
            trust_server_certificate_ca,
            readonly,
        }))
    }

    fn into_config(self) -> Config {
        let mut config = Config::new();
        config.host(self.host);
        if let Some(port) = self.port {
            config.port(port);
        }
        if let Some(instance) = self.instance {
            config.instance_name(instance);
        }
        if let Some(database) = self.database {
            config.database(database);
        }
        if let Some(username) = self.username {
            config.authentication(AuthMethod::sql_server(
                username,
                self.password.unwrap_or_default(),
            ));
        }
        if let Some(application_name) = self.application_name {
            config.application_name(application_name);
        }
        config.encryption(match self.encryption {
            UrlEncryption::Off => EncryptionLevel::Off,
            UrlEncryption::Required => EncryptionLevel::Required,
            UrlEncryption::Disabled => EncryptionLevel::NotSupported,
        });
        if self.trust_server_certificate {
            config.trust_cert();
        }
        if let Some(ca) = self.trust_server_certificate_ca {
            config.trust_cert_ca(ca);
        }
        config.readonly(self.readonly);
        config
    }

    fn parse_instance(url: &Url) -> anyhow::Result<Option<String>> {
        let path = url.path();
        if path.is_empty() || path == "/" {
            return Ok(None);
        }
        let Some(instance) = path.strip_prefix('/') else {
            return Err(DtError::invalid_config("MSSQL connection URL path is invalid").into());
        };
        if instance.contains('/') {
            return Err(DtError::invalid_config(
                "MSSQL connection URL path must contain at most one instance name",
            )
            .into());
        }
        let instance = Self::decode_component(instance, "instance")?;
        if instance.contains('/') || instance.contains('\\') {
            return Err(DtError::invalid_config(
                "MSSQL connection URL instance name must not contain a path separator",
            )
            .into());
        }
        Ok((!instance.is_empty()).then_some(instance))
    }

    fn parse_query(url: &Url) -> anyhow::Result<HashMap<&'static str, String>> {
        let mut params = HashMap::new();
        for (key, value) in url.query_pairs() {
            let canonical_key = Self::canonical_query_key(&key).ok_or_else(|| {
                DtError::invalid_config(format!(
                    "MSSQL connection URL query parameter {key:?} is not supported"
                ))
            })?;
            if params.insert(canonical_key, value.into_owned()).is_some() {
                return Err(DtError::invalid_config(format!(
                    "MSSQL connection URL query parameter {canonical_key:?} was provided more than once"
                ))
                .into());
            }
        }
        Ok(params)
    }

    fn canonical_query_key(key: &str) -> Option<&'static str> {
        match key.to_ascii_lowercase().as_str() {
            "user id" | "user" | "username" | "uid" => Some(USER_ID),
            "password" | "pwd" => Some(PASSWORD),
            "port" => Some(PORT),
            "database" | "initial catalog" | "databasename" => Some(DATABASE),
            "encrypt" => Some(ENCRYPT),
            "trustservercertificate" | "trust server certificate" => Some(TRUST_SERVER_CERTIFICATE),
            "trustservercertificateca" | "trust server certificate ca" => {
                Some(TRUST_SERVER_CERTIFICATE_CA)
            }
            "app name" | "application name" | "applicationname" => Some(APP_NAME),
            "applicationintent" | "application intent" => Some(APPLICATION_INTENT),
            _ => None,
        }
    }

    fn reject_duplicate_url_value(
        params: &HashMap<&'static str, String>,
        key: &'static str,
        url_contains_value: bool,
    ) -> anyhow::Result<()> {
        if url_contains_value && params.contains_key(key) {
            return Err(DtError::invalid_config(format!(
                "MSSQL connection URL parameter {key:?} was provided more than once"
            ))
            .into());
        }
        Ok(())
    }

    fn decode_component(value: &str, component: &str) -> anyhow::Result<String> {
        urlencoding::decode(value)
            .map(|value| value.into_owned())
            .map_err(|_| {
                DtError::invalid_config(format!(
                    "MSSQL connection URL contains an invalid percent-encoded {component}"
                ))
                .into()
            })
    }

    fn parse_port(port: &str) -> anyhow::Result<u16> {
        port.parse::<u16>().map_err(|_| {
            DtError::invalid_config("MSSQL connection URL contains an invalid port").into()
        })
    }

    fn parse_encryption(value: &str) -> anyhow::Result<UrlEncryption> {
        match value.to_ascii_lowercase().as_str() {
            "mandatory" | "yes" | "1" | "t" | "true" => Ok(UrlEncryption::Required),
            "optional" | "no" | "0" | "f" | "false" => Ok(UrlEncryption::Off),
            "disable" => Ok(UrlEncryption::Disabled),
            "strict" => Err(DtError::invalid_config(
                "MSSQL connection URL encrypt=strict is not supported by Tiberius",
            )
            .into()),
            _ => Err(DtError::invalid_config(
                "MSSQL connection URL contains an invalid encrypt value",
            )
            .into()),
        }
    }

    fn parse_bool(key: &str, value: &str) -> anyhow::Result<bool> {
        match value.to_ascii_lowercase().as_str() {
            "yes" | "1" | "t" | "true" => Ok(true),
            "no" | "0" | "f" | "false" => Ok(false),
            _ => Err(DtError::invalid_config(format!(
                "MSSQL connection URL contains an invalid {key} value"
            ))
            .into()),
        }
    }

    fn parse_application_intent(value: &str) -> anyhow::Result<bool> {
        match value.to_ascii_lowercase().as_str() {
            "readonly" => Ok(true),
            "readwrite" => Ok(false),
            _ => Err(DtError::invalid_config(
                "MSSQL connection URL applicationintent must be ReadOnly or ReadWrite",
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MssqlConnectionUrl, MssqlUrlParts, UrlEncryption};

    #[test]
    fn parses_go_mssqldb_style_url_components() {
        let parts = MssqlUrlParts::try_parse(
            "sqlserver://user%40domain:p%7Bass@db.example.com:1444/SQL%20Express?\
             database=tenant+db&app+name=ape+dts&encrypt=disable&\
             trustservercertificate=true&applicationintent=ReadOnly",
        )
        .unwrap()
        .unwrap();

        assert_eq!(parts.host, "db.example.com");
        assert_eq!(parts.port, Some(1444));
        assert_eq!(parts.instance.as_deref(), Some("SQL Express"));
        assert_eq!(parts.database.as_deref(), Some("tenant db"));
        assert_eq!(parts.username.as_deref(), Some("user@domain"));
        assert_eq!(parts.password.as_deref(), Some("p{ass"));
        assert_eq!(parts.application_name.as_deref(), Some("ape dts"));
        assert_eq!(parts.encryption, UrlEncryption::Disabled);
        assert!(parts.trust_server_certificate);
        assert!(parts.readonly);
    }

    #[test]
    fn supports_both_schemes_and_query_port() {
        for scheme in ["sqlserver", "mssql"] {
            let url = format!("{scheme}://localhost?port=1435&database=ape_dts");
            let config = MssqlConnectionUrl::try_parse_to_config(&url)
                .unwrap()
                .unwrap();
            assert_eq!(config.get_addr(), "localhost:1435");
        }
    }

    #[test]
    fn leaves_non_mssql_url_formats_for_fallback_parsers() {
        assert!(MssqlConnectionUrl::try_parse_to_config(
            "server=tcp:localhost,1433;database=ape_dts"
        )
        .unwrap()
        .is_none());
        assert!(MssqlConnectionUrl::try_parse_to_config(
            "jdbc:sqlserver://localhost:1433;database=ape_dts"
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn rejects_ambiguous_or_unsupported_url_values() {
        let invalid_urls = [
            "sqlserver://localhost:1433?port=1434",
            "sqlserver://user:password@localhost?USER+ID=other",
            "sqlserver://localhost?database=one&DATABASE=two",
            "sqlserver://localhost/one/two",
            "sqlserver://localhost?encrypt=strict",
            "sqlserver://localhost?unknown=value",
            "sqlserver://localhost?applicationintent=ReadOnly",
        ];

        for url in invalid_urls {
            assert!(
                MssqlConnectionUrl::try_parse_to_config(url).is_err(),
                "{url} should be rejected"
            );
        }
    }
}
