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
