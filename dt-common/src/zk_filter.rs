use std::collections::HashSet;

use crate::config::zk_filter_config::ZkFilterConfig;

#[derive(Debug, Clone)]
pub struct ZkFilter {
    pub do_paths: HashSet<String>,
    pub ignore_paths: HashSet<String>,
    pub include_ephemeral: bool,
}

impl ZkFilter {
    pub fn from_config(config: &ZkFilterConfig) -> anyhow::Result<Self> {
        let do_paths = Self::parse_paths(&config.do_paths);
        let ignore_paths = Self::parse_paths(&config.ignore_paths);
        Ok(Self {
            do_paths,
            ignore_paths,
            include_ephemeral: config.include_ephemeral,
        })
    }

    pub fn filter_path(&self, path: &str) -> bool {
        if self.ignore_paths.iter().any(|p| path.starts_with(p)) {
            return true;
        }
        if self.do_paths.is_empty() {
            return false;
        }
        !self.do_paths.iter().any(|p| path.starts_with(p))
    }

    pub fn filter_ephemeral(&self, ephemeral: bool) -> bool {
        ephemeral && !self.include_ephemeral
    }

    fn parse_paths(config_str: &str) -> HashSet<String> {
        if config_str.trim().is_empty() {
            return HashSet::new();
        }
        config_str.split(',').map(|s| s.trim().to_string()).collect()
    }
}
