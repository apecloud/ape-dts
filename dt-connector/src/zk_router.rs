use std::collections::HashMap;

use dt_common::config::router_config::RouterConfig;

type PathPrefixMap = HashMap<String, String>;

#[derive(Debug, Clone, Default)]
pub struct ZkRouter {
    pub path_prefix_map: PathPrefixMap,
}

impl ZkRouter {
    pub fn from_config(config: &RouterConfig) -> anyhow::Result<Self> {
        match config {
            RouterConfig::Zk { path_prefix_map } => {
                let map = Self::parse_path_prefix_map(path_prefix_map)?;
                Ok(Self {
                    path_prefix_map: map,
                })
            }
            _ => Ok(Self::default()),
        }
    }

    pub fn route_path(&self, path: &str) -> String {
        for (src_prefix, dst_prefix) in &self.path_prefix_map {
            if path.starts_with(src_prefix.as_str()) {
                return format!("{}{}", dst_prefix, &path[src_prefix.len()..]);
            }
        }
        path.to_string()
    }

    fn parse_path_prefix_map(config_str: &str) -> anyhow::Result<PathPrefixMap> {
        let mut map = PathPrefixMap::new();
        if config_str.trim().is_empty() {
            return Ok(map);
        }
        for pair in config_str.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() == 2 {
                map.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
            }
        }
        Ok(map)
    }
}
