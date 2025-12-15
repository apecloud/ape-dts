use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    ddl_meta::ddl_data::DdlData, foxlake::s3_file_meta::S3FileMeta, row_data::RowData,
    struct_meta::struct_data::StructData,
};
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
    Foxlake {
        file_meta: S3FileMeta,
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
            DtData::Ddl { ddl_data } => ddl_data.get_malloc_size(),
            DtData::Redis { entry } => entry.get_data_malloc_size() as u64,
            DtData::Foxlake { file_meta } => file_meta.data_size as u64,
            // ignore other item types
            _ => 0,
        }
    }

    pub fn get_data_count(&self) -> usize {
        match &self {
            DtData::Foxlake { file_meta } => file_meta.row_count,
            _ => 1,
        }
    }
}

impl std::fmt::Display for DtData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
