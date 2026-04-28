use crate::{
    config::extractor_config::ExtractorConfig,
    meta::{
        ddl_meta::ddl_data::DdlData, dt_data::DtData, row_data::RowData,
        struct_meta::struct_data::StructData,
    },
};

pub fn from_schema_tb(schema: &str, tb: &str) -> String {
    if schema.is_empty() || tb.is_empty() {
        String::new()
    } else {
        format!("{}.{}", schema, tb)
    }
}

pub fn from_row_data(row_data: &RowData) -> String {
    from_schema_tb(&row_data.schema, &row_data.tb)
}

pub fn from_ddl_data(ddl_data: &DdlData) -> String {
    let (schema, tb) = ddl_data.get_schema_tb();
    from_schema_tb(&schema, &tb)
}

pub fn from_struct_data(struct_data: &StructData) -> String {
    struct_data.schema.clone()
}

pub fn from_dt_data(dt_data: &DtData) -> String {
    match dt_data {
        DtData::Dml { row_data } => from_row_data(row_data),
        DtData::Ddl { ddl_data } => from_ddl_data(ddl_data),
        DtData::Struct { struct_data } => from_struct_data(struct_data),
        _ => String::new(),
    }
}

pub fn from_task_context_or_extractor(
    task_context_id: &str,
    extractor_config: &ExtractorConfig,
) -> String {
    if !task_context_id.is_empty() {
        return task_context_id.to_string();
    }

    match extractor_config {
        ExtractorConfig::MysqlSnapshot { db, tb, .. }
        | ExtractorConfig::MongoSnapshot { db, tb, .. } => from_schema_tb(db, tb),
        ExtractorConfig::PgSnapshot { schema, tb, .. }
        | ExtractorConfig::FoxlakeS3 { schema, tb, .. } => from_schema_tb(schema, tb),
        _ => String::new(),
    }
}
