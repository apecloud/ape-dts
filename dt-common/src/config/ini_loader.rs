use std::{any::type_name, fs::File, io::Read, str::FromStr};

use anyhow::Context;
use configparser::ini::Ini;

use crate::error::{DtError, DtErrorContextExt, Stage};

#[derive(Debug)]
pub struct IniLoader {
    pub ini: Ini,
}

impl IniLoader {
    pub fn new(ini_file: &str) -> anyhow::Result<Self> {
        let mut config_str = String::new();
        File::open(ini_file)
            .map_err(|error| {
                let context = if error.kind() == std::io::ErrorKind::NotFound {
                    DtError::MissingConfig(format!("path: {ini_file}"))
                } else {
                    DtError::IoFailed(format!("failed to open config file: {ini_file}"))
                };
                anyhow::Error::new(error).context(context)
            })?
            .read_to_string(&mut config_str)
            .context(DtError::IoFailed(format!(
                "failed to read config file: {ini_file}"
            )))?;
        let mut ini = Ini::new();
        // allow using comment symbols(; and #) in value
        // E.g. do_dbs=`a;`,`bcd`
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(config_str).map_err(|_| {
            DtError::InvalidConfig(format!("failed to parse config file: {ini_file}"))
                .message("failed to parse config file")
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
        Err(DtError::MissingConfigItem(format!(
            "config [{section}].{key} does not exist or is empty"
        ))
        .message("required config value is missing")
        .stage(Stage::Bootstrap))
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
                "[redacted]"
            } else {
                value
            };
            DtError::InvalidConfig(format!(
                "config [{section}].{key}={rendered_value} can not be parsed as {}",
                type_name::<T>()
            ))
            .message("config value has an invalid type")
            .stage(Stage::Bootstrap)
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
    use super::*;
    use crate::error::ErrorReport;

    #[test]
    fn invalid_value_is_redacted_for_sensitive_keys() {
        let error =
            IniLoader::parse_value::<u32>("extractor", "password", "not-a-number").unwrap_err();
        let rendered = ErrorReport::from_anyhow(&error).to_string();
        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains("not-a-number"));
    }
}
