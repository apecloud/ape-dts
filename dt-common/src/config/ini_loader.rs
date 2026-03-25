use std::{any::type_name, fmt::Debug, fs::File, io::Read, str::FromStr};

use anyhow::bail;
use configparser::ini::Ini;

use crate::error::Error;

pub struct IniLoader {
    pub ini: Ini,
}

impl IniLoader {
    pub fn new(ini_file: &str) -> Self {
        let mut config_str = String::new();
        File::open(ini_file)
            .expect("failed to open ini file")
            .read_to_string(&mut config_str)
            .expect("failed to read ini content");
        let mut ini = Ini::new();
        // allow using comment symbols(; and #) in value
        // E.g. do_dbs=`a;`,`bcd`
        ini.set_inline_comment_symbols(Some(&Vec::new()));
        ini.read(config_str).expect("failed to read content as ini");
        Self { ini }
    }

    pub fn get_required<T>(&self, section: &str, key: &str) -> T
    where
        T: FromStr,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value).unwrap();
            }
        }
        panic!("config [{}].{} does not exist or is empty", section, key);
    }

    pub fn get_optional<T>(&self, section: &str, key: &str) -> T
    where
        T: Default,
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        self.get_with_default(section, key, T::default())
    }

    pub fn get_with_default<T>(&self, section: &str, key: &str, default: T) -> T
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        if let Some(value) = self.ini.get(section, key) {
            if !value.is_empty() {
                return Self::parse_value(section, key, &value).unwrap();
            }
        }
        default
    }

    pub fn contains(&self, section: &str, key: &str) -> bool {
        self.ini.get(section, key).is_some()
    }

    fn parse_value<T>(section: &str, key: &str, value: &str) -> anyhow::Result<T>
    where
        T: FromStr,
    {
        match value.parse::<T>() {
            Ok(v) => Ok(v),
            Err(_) => bail! {Error::ConfigError(format!(
                "config [{}].{}={}, can not be parsed as {}",
                section,
                key,
                value,
                type_name::<T>(),
            ))},
        }
    }
}
