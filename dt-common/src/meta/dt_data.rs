use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ddl_meta::ddl_data::DdlData, row_data::RowData, struct_meta::struct_data::StructData};
use crate::meta::dcl_meta::dcl_data::DclData;
use crate::meta::row_type::RowSqlType;
use crate::meta::{position::Position, redis::redis_entry::RedisEntry};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DtItem {
    pub dt_data: DtData,
    pub position: Position,
    pub data_origin_node: String,
}

impl DtItem {
    pub fn is_ddl(&self) -> bool {
        self.dt_data.is_ddl()
    }

    pub fn is_dcl(&self) -> bool {
        self.dt_data.is_dcl()
    }

    pub fn get_row_sql_type(&self) -> RowSqlType {
        match &self.dt_data {
            DtData::Ddl { .. } => RowSqlType::DDL,
            DtData::Dcl { .. } => RowSqlType::DCL,
            _ => RowSqlType::DML,
        }
    }

    pub fn get_data_size(&self) -> u64 {
        self.dt_data.get_data_size()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DtData {
    Struct {
        struct_data: StructData,
    },
    Ddl {
        ddl_data: DdlData,
        #[serde(skip)]
        commit_position: Option<Box<Position>>,
    },
    Dcl {
        dcl_data: DclData,
    },
    Dml {
        row_data: RowData,
    },
    Begin {},
    Commit {
        xid: String,
    },
    Heartbeat {},
    #[serde(skip)]
    Redis {
        entry: RedisEntry,
    },
}

impl DtData {
    pub fn is_begin(&self) -> bool {
        matches!(self, DtData::Begin { .. })
    }

    pub fn is_commit(&self) -> bool {
        matches!(self, DtData::Commit { .. })
    }

    pub fn is_ddl(&self) -> bool {
        matches!(self, DtData::Ddl { .. })
    }

    pub fn is_dcl(&self) -> bool {
        matches!(self, DtData::Dcl { .. })
    }

    pub fn get_data_size(&self) -> u64 {
        match &self {
            DtData::Dml { row_data } => row_data.data_size as u64,
            DtData::Dcl { dcl_data } => dcl_data.get_malloc_size(),
            DtData::Ddl { ddl_data, .. } => ddl_data.get_malloc_size(),
            DtData::Redis { entry } => entry.get_data_malloc_size() as u64,
            // ignore other item types
            _ => 0,
        }
    }

    pub fn get_data_count(&self) -> usize {
        match &self {
            DtData::Begin {} | DtData::Commit { .. } | DtData::Heartbeat {} => 0,
            _ => 1,
        }
    }
}

impl std::fmt::Display for DtData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_checkpoint_metadata_preserves_the_serialized_format() {
        let legacy = json!({"Ddl": {"ddl_data": DdlData::default()}});
        let decoded: DtData = serde_json::from_value(legacy.clone()).unwrap();
        assert!(matches!(
            decoded,
            DtData::Ddl {
                commit_position: None,
                ..
            }
        ));

        let ddl = DtData::Ddl {
            ddl_data: DdlData::default(),
            commit_position: Some(Box::new(Position::MysqlCdc {
                server_id: "1".into(),
                binlog_filename: "mysql-bin.000001".into(),
                next_event_position: 1000,
                gtid_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100".into(),
                timestamp: Position::format_timestamp_millis(1000),
            })),
        };
        assert_eq!(serde_json::to_value(ddl).unwrap(), legacy);
    }
}
