use std::collections::HashSet;

use dt_common::{
    config::{config_enums::DbType, data_marker_config::DataMarkerConfig},
    meta::{dt_data::DtData, redis::redis_entry::RedisEntry},
};

#[derive(Debug, Clone, Default)]
pub struct DataMarker {
    pub db_type: DbType,
    pub topo_name: String,
    pub topo_nodes: Vec<String>,
    pub src_node: String,
    pub dst_node: String,
    pub do_nodes: HashSet<String>,
    pub ignore_nodes: HashSet<String>,
    // mysql/pg/mongo
    pub marker_schema: String,
    pub marker_tb: String,
    // redis
    pub marker: String,

    pub data_origin_node: String,
    pub filter: bool,
    pub reset: bool,
}

const DATA_ORIGIN_NODE: &str = "data_origin_node";

impl DataMarker {
    pub fn from_config(config: &DataMarkerConfig, db_type: &DbType) -> anyhow::Result<Self> {
        let topo_nodes: Vec<String> = config
            .topo_nodes
            .split(',')
            .map(|i| i.to_string())
            .collect();
        let do_nodes: HashSet<String> = config.do_nodes.split(',').map(|i| i.to_string()).collect();
        let ignore_nodes: HashSet<String> = config
            .ignore_nodes
            .split(',')
            .map(|i| i.to_string())
            .collect();

        let mut me = Self {
            db_type: db_type.clone(),
            topo_name: config.topo_name.clone(),
            topo_nodes,
            src_node: config.src_node.clone(),
            dst_node: config.dst_node.clone(),
            do_nodes,
            ignore_nodes,
            ..Default::default()
        };

        match *db_type {
            DbType::Mysql | DbType::Pg | DbType::Mongo => {
                let marker_info: Vec<&str> = config.marker.split('.').collect();
                me.marker_schema = marker_info[0].to_string();
                me.marker_tb = marker_info[1].to_string();
            }
            _ => me.marker = config.marker.clone(),
        }

        me.reset();
        Ok(me)
    }

    pub fn reset(&mut self) {
        self.data_origin_node = self.src_node.clone();
        // by default, no filter
        self.filter = false;
        self.reset = true;
    }

    pub fn is_marker_info(&self, dt_data: &DtData) -> bool {
        match dt_data {
            DtData::Dml { row_data } => self.is_rdb_marker_info(&row_data.schema, &row_data.tb),
            DtData::Redis { entry } => self.is_redis_marker_info(entry),
            _ => false,
        }
    }

    pub fn is_redis_marker_info(&self, entry: &RedisEntry) -> bool {
        let entry_key = if entry.is_raw() {
            entry.key.to_string()
        } else {
            entry.cmd.get_str_arg(1)
        };
        // if self.marker is "data_marker_topo1_test",
        // both "data_marker_topo1_test" and "data_marker_topo1_test{b}" can match
        entry_key == self.marker || entry_key.starts_with(&format!("{}{{", self.marker))
    }

    pub fn is_rdb_marker_info(&self, schema: &str, tb: &str) -> bool {
        self.marker_schema == schema && self.marker_tb == tb
    }

    pub fn refresh(&mut self, dt_data: &DtData) {
        match dt_data {
            DtData::Dml { row_data } => {
                // refresh should be only called when dt_data is a data marker
                // update data_origin_node
                if let Some(after) = &row_data.after {
                    if let Some(col_value) = after.get(DATA_ORIGIN_NODE) {
                        // mysql, ColValue::Blob
                        // pg, ColValue::String
                        self.data_origin_node = col_value.to_string();
                    }
                }
            }

            DtData::Redis { entry } => {
                self.data_origin_node = entry.cmd.get_str_arg(2);
            }

            _ => {}
        }

        // update filter
        self.filter = self.ignore_nodes.contains(&self.data_origin_node)
            || !self.do_nodes.contains(&self.data_origin_node);
        self.reset = false;
    }

    pub fn filter(&self) -> bool {
        self.filter
    }
}
