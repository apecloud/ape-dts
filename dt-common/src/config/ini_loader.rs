use std::{any::type_name, fs::File, io::Read, str::FromStr};

use configparser::ini::Ini;

use crate::error::{DtError, ErrorCode, Stage};

#[derive(Debug)]
pub struct IniLoader {
    pub ini: Ini,
}

impl IniLoader {
    pub fn new(ini_file: &str) -> anyhow::Result<Self> {
        let mut config_str = String::new();
        File::open(ini_file)
            .map_err(|error| {
                let code = if error.kind() == std::io::ErrorKind::NotFound {
                    ErrorCode::MissingConfig
                } else {
                    ErrorCode::IoFailed
                };
                DtError::new(code)
                    .message("failed to open config file")
                    .detail(format!("path: {ini_file}"))
                    .stage(Stage::Bootstrap)
                    .source(error)
            })?
            .read_to_string(&mut config_str)
            .map_err(|error| {
                DtError::new(ErrorCode::IoFailed)
                    .message("failed to read config file")
                    .detail(format!("path: {ini_file}"))
                    .stage(Stage::Bootstrap)
                    .source(error)
            })?;
        let mut ini = Ini::new();
        // allow using comment symbols(; and #) in value
        // E.g. do_dbs=`a;`,`bcd`
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(config_str).map_err(|_| {
            DtError::new(ErrorCode::InvalidConfig)
                .message("failed to parse config file")
                .detail(format!("path: {ini_file}"))
                .stage(Stage::Bootstrap)
        })?;
        Ok(Self { ini })
    }

    pub fn get_required<T>(&self, section: &str, key: &str) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value);
            }
        }
        Err(DtError::new(ErrorCode::MissingConfigItem)
            .message("required config value is missing")
            .detail(format!(
                "config [{section}].{key} does not exist or is empty"
            ))
            .stage(Stage::Bootstrap)
            .into())
    }

    pub fn get_optional<T>(&self, section: &str, key: &str) -> anyhow::Result<T>
    where
        T: Default,
        T: FromStr,
    {
        self.get_with_default(section, key, T::default())
    }

    pub fn get_with_default<T>(&self, section: &str, key: &str, default: T) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value);
            }
        }
        Ok(default)
    }

    pub fn contains(&self, section: &str, key: &str) -> bool {
        self.ini.get(section, key).is_some()
    }

    fn parse_value<T>(section: &str, key: &str, value: &str) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        value.parse::<T>().map_err(|_| {
            let rendered_value = if Self::is_sensitive_key(key) {
                "<redacted>"
            } else {
                value
            };
            DtError::new(ErrorCode::InvalidConfig)
                .message("config value has an invalid type")
                .detail(format!(
                    "config [{section}].{key}={rendered_value} can not be parsed as {}",
                    type_name::<T>()
                ))
                .stage(Stage::Bootstrap)
                .into()
        })
    }

    fn is_sensitive_key(key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        ["password", "secret", "token"]
            .iter()
            .any(|sensitive| key.contains(sensitive))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::error::ErrorReport;

    #[test]
    fn missing_file_has_missing_config_code() {
        let error = IniLoader::new("/tmp/ape-dts-config-that-does-not-exist.ini").unwrap_err();
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::MissingConfig
        );
    }

    #[test]
    fn missing_required_value_has_missing_config_code() {
        let loader = IniLoader { ini: Ini::new() };
        let error = loader
            .get_required::<String>("extractor", "url")
            .unwrap_err();
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::MissingConfigItem
        );
    }

    #[test]
    fn invalid_value_is_redacted_for_sensitive_keys() {
        let error =
            IniLoader::parse_value::<u32>("extractor", "password", "not-a-number").unwrap_err();
        let rendered = ErrorReport::from_anyhow(&error).to_string();
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("not-a-number"));
    }

    #[test]
    fn invalid_bool_and_number_are_invalid_config() {
        let mut ini = Ini::new();
        ini.set("runtime", "enable", Some("not-a-bool".to_string()));
        ini.set("runtime", "workers", Some("not-a-number".to_string()));
        let loader = IniLoader { ini };

        for error in [
            loader
                .get_required::<bool>("runtime", "enable")
                .unwrap_err(),
            loader
                .get_required::<usize>("runtime", "workers")
                .unwrap_err(),
        ] {
            assert_eq!(
                ErrorReport::from_anyhow(&error).code,
                ErrorCode::InvalidConfig
            );
        }
    }

    #[test]
    fn malformed_ini_is_invalid_config() {
        let path = std::env::temp_dir().join(format!(
            "ape-dts-malformed-config-{}.ini",
            std::process::id()
        ));
        fs::write(&path, "[runtime\nworkers=1\n").unwrap();
        let result = IniLoader::new(path.to_str().unwrap());
        fs::remove_file(path).unwrap();
        let error = result.err().unwrap();
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::InvalidConfig
        );
    }
}
