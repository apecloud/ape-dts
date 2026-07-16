use dt_common::config::ini_loader::IniLoader;

use super::precheck_config::PrecheckConfig;

const PRECHECK: &str = "precheck";

pub struct PrecheckTaskConfig {
    pub precheck: PrecheckConfig,
}

impl PrecheckTaskConfig {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let loader = IniLoader::new(task_config_file)?;
        Self::from_loader(&loader)
    }

    pub fn load_if_present(task_config_file: &str) -> anyhow::Result<Option<Self>> {
        let loader = IniLoader::new(task_config_file)?;
        if !loader
            .ini
            .sections()
            .iter()
            .any(|section| section == PRECHECK)
        {
            return Ok(None);
        }
        Self::from_loader(&loader).map(Some)
    }

    fn from_loader(loader: &IniLoader) -> anyhow::Result<Self> {
        let precheck_config = Self::load_precheck_config(loader)?;
        Ok(Self {
            precheck: precheck_config,
        })
    }

    fn load_precheck_config(loader: &IniLoader) -> anyhow::Result<PrecheckConfig> {
        Ok(PrecheckConfig {
            do_struct_init: loader.get_required(PRECHECK, "do_struct_init")?,
            do_cdc: loader.get_required(PRECHECK, "do_cdc")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use dt_common::error::{ErrorCode, ErrorReport};

    use super::*;

    fn write_config(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ape-dts-precheck-{name}-{}.ini",
            std::process::id()
        ));
        fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn missing_section_is_not_an_error() {
        let path = write_config("missing", "[runtime]\nworker_threads=1\n");
        let config = PrecheckTaskConfig::load_if_present(path.to_str().unwrap()).unwrap();
        fs::remove_file(path).unwrap();
        assert!(config.is_none());
    }

    #[test]
    fn invalid_present_section_is_invalid_config() {
        let path = write_config(
            "invalid",
            "[precheck]\ndo_struct_init=not-a-bool\ndo_cdc=true\n",
        );
        let result = PrecheckTaskConfig::load_if_present(path.to_str().unwrap());
        fs::remove_file(path).unwrap();
        let error = result.err().unwrap();
        assert_eq!(
            ErrorReport::from_anyhow(&error).code,
            ErrorCode::InvalidConfig
        );
    }
}
