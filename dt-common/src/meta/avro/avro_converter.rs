use std::{collections::HashMap, str::FromStr};

use anyhow::Ok;
use apache_avro::{from_avro_datum, to_avro_datum, types::Value, Schema};

use crate::{
    config::config_enums::DbType,
    meta::{
        col_value::ColValue,
        ddl_meta::{ddl_data::DdlData, ddl_type::DdlType},
        dt_data::DtData,
        rdb_meta_manager::RdbMetaManager,
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
        row_type::RowType,
    },
};

use super::avro_converter_schema::{AvroConverterSchema, AvroFieldDef};

#[derive(Clone)]
pub struct AvroConverter {
    schema: Schema,
    pub with_field_defs: bool,
    pub meta_manager: Option<RdbMetaManager>,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";
const EXTRA: &str = "extra";
const OPERATION: &str = "operation";
const DDL: &str = "ddl";
const DB_TYPE: &str = "db_type";
const DDL_TYPE: &str = "ddl_type";
const QUERY: &str = "query";
const SCHEMA: &str = "schema";
const TB: &str = "tb";
const FIELDS: &str = "fields";

impl AvroConverter {
    pub fn new(meta_manager: Option<RdbMetaManager>, with_field_defs: bool) -> Self {
        AvroConverter {
            schema: AvroConverterSchema::get_avro_schema(),
            meta_manager,
            with_field_defs,
        }
    }

    pub fn refresh_meta(&mut self, data: &[DdlData]) {
        if let Some(meta_manager) = &mut self.meta_manager {
            for ddl_data in data.iter() {
                meta_manager.invalidate_cache_by_ddl_data(ddl_data);
            }
        }
    }

    pub async fn row_data_to_avro_key(&mut self, row_data: &RowData) -> anyhow::Result<String> {
        if let Some(tb_meta) = self.get_tb_meta(row_data).await? {
            let convert = |col_values: &HashMap<String, ColValue>| {
                if let Some(col) = tb_meta.order_cols.first() {
                    if let Some(value) = col_values.get(col) {
                        return value.to_option_string();
                    }
                }
                None
            };

            if let Some(key) = match row_data.row_type {
                RowType::Insert => convert(row_data.require_after()?),
                RowType::Update | RowType::Delete => convert(row_data.require_before()?),
            } {
                return Ok(key);
            }
        }
        Ok(String::new())
    }

    pub async fn row_data_to_avro_value(&mut self, row_data: RowData) -> anyhow::Result<Vec<u8>> {
        let mut cols = vec![];
        let mut merge_cols = |col_values: &Option<HashMap<String, ColValue>>| {
            if let Some(value) = col_values {
                for key in value.keys() {
                    if !cols.contains(key) {
                        cols.push(key.into())
                    }
                }
            }
        };
        merge_cols(&row_data.before);
        merge_cols(&row_data.after);
        cols.sort();

        // before
        let (before_avro_values, before_avro_types) = Self::col_values_to_avro(&row_data.before);
        let before = if let Value::Map(_) = &before_avro_values {
            Value::Union(1, Box::new(before_avro_values))
        } else {
            Value::Union(0, Box::new(Value::Null))
        };

        // after
        let (after_avro_values, after_avro_types) = Self::col_values_to_avro(&row_data.after);
        let after = if let Value::Map(_) = &after_avro_values {
            Value::Union(1, Box::new(after_avro_values))
        } else {
            Value::Union(0, Box::new(Value::Null))
        };

        // fields
        let fields = if !self.with_field_defs || cols.is_empty() {
            Value::Union(0, Box::new(Value::Null))
        } else {
            let mut fields = vec![];
            let tb_meta = self.get_tb_meta(&row_data).await?;
            for col in cols.iter() {
                let mut column_type = String::new();
                if let Some(tb_meta) = tb_meta {
                    if let Some(col_origin_type) = tb_meta.col_origin_type_map.get(col) {
                        column_type = col_origin_type.to_owned();
                    }
                }

                let mut avro_type = String::new();
                if let Some(_type) = before_avro_types.get(col) {
                    avro_type = _type.to_owned();
                };
                if let Some(_type) = after_avro_types.get(col) {
                    if !_type.is_empty() && _type != "Null" {
                        avro_type = _type.to_owned();
                    }
                }

                fields.push(AvroFieldDef {
                    name: col.to_owned(),
                    column_type,
                    avro_type,
                });
            }
            Value::Union(1, Box::new(apache_avro::to_value(fields).unwrap()))
        };

        let value = Value::Record(vec![
            (SCHEMA.into(), Value::String(row_data.schema)),
            (TB.into(), Value::String(row_data.tb)),
            (
                OPERATION.into(),
                Value::String(row_data.row_type.to_string()),
            ),
            (FIELDS.into(), fields),
            (BEFORE.into(), before),
            (AFTER.into(), after),
            (EXTRA.into(), Value::Union(0, Box::new(Value::Null))),
        ]);
        Ok(to_avro_datum(&self.schema, value)?)
    }

    pub async fn ddl_data_to_avro_value(&mut self, ddl_data: DdlData) -> anyhow::Result<Vec<u8>> {
        let mut col_values: HashMap<String, ColValue> = HashMap::new();
        col_values.insert(
            DB_TYPE.into(),
            ColValue::String(ddl_data.db_type.to_string()),
        );
        col_values.insert(
            DDL_TYPE.into(),
            ColValue::String(ddl_data.ddl_type.to_string()),
        );
        col_values.insert(QUERY.into(), ColValue::String(ddl_data.query));

        let (avro_values, _) = Self::col_values_to_avro(&Some(col_values));
        let extra = Value::Union(1, Box::new(avro_values));

        let value = Value::Record(vec![
            (SCHEMA.into(), Value::String(ddl_data.default_schema)),
            (TB.into(), Value::String(String::new())),
            (OPERATION.into(), Value::String(DDL.into())),
            (FIELDS.into(), Value::Union(0, Box::new(Value::Null))),
            (BEFORE.into(), Value::Union(0, Box::new(Value::Null))),
            (AFTER.into(), Value::Union(0, Box::new(Value::Null))),
            (EXTRA.into(), extra),
        ]);
        Ok(to_avro_datum(&self.schema, value)?)
    }

    pub fn avro_value_to_dt_data(&self, payload: Vec<u8>) -> anyhow::Result<DtData> {
        let mut reader = payload.as_slice();
        let value = from_avro_datum(&self.schema, &mut reader, None)?;
        let mut avro_map = Self::avro_to_map(value);

        let avro_to_string = |value: Option<Value>| {
            if let Some(Value::String(v)) = value {
                return v;
            }
            String::new()
        };

        let schema = avro_to_string(avro_map.remove(SCHEMA));
        let tb = avro_to_string(avro_map.remove(TB));
        let operation = avro_to_string(avro_map.remove(OPERATION));

        if operation == *DDL {
            let get_extra_string = |extra: &Option<HashMap<String, ColValue>>, key: &str| {
                if let Some(extra) = extra {
                    if let Some(v) = extra.get(key) {
                        return v.to_string();
                    }
                }
                String::new()
            };
            let extra = self.avro_to_col_values(avro_map.remove(EXTRA));
            let db_type = get_extra_string(&extra, DB_TYPE);
            let ddl_type = get_extra_string(&extra, DDL_TYPE);
            let query = get_extra_string(&extra, QUERY);
            Ok(DtData::Ddl {
                ddl_data: DdlData {
                    default_schema: schema,
                    query,
                    db_type: DbType::from_str(&db_type)?,
                    ddl_type: DdlType::from_str(&ddl_type)?,
                    ..Default::default()
                },
            })
        } else {
            let _fields = self.avro_to_fields(avro_map.remove(FIELDS));
            let before = self.avro_to_col_values(avro_map.remove(BEFORE));
            let after = self.avro_to_col_values(avro_map.remove(AFTER));
            Ok(DtData::Dml {
                row_data: RowData::new(schema, tb, RowType::from_str(&operation)?, before, after),
            })
        }
    }

    fn avro_to_fields(&self, value: Option<Value>) -> Vec<AvroFieldDef> {
        if let Some(v) = value {
            return apache_avro::from_value(&v).unwrap();
        }
        vec![]
    }

    fn avro_to_col_values(&self, value: Option<Value>) -> Option<HashMap<String, ColValue>> {
        value.as_ref()?;

        // Some(Union(1, Map({
        //     "bytes_col": Union(4, Bytes([5, 6, 7, 8])),
        //     "string_col": Union(1, String("string_after")),
        //     "boolean_col": Union(5, Boolean(true)),
        //     "long_col": Union(2, Long(2)),
        //     "null_col": Union(0, Null),
        //     "double_col": Union(3, Double(2.2))
        //   })))

        if let Value::Union(1, v) = value.unwrap() {
            if let Value::Map(map_v) = *v {
                let mut col_values = HashMap::new();
                for (col, value) in map_v {
                    col_values.insert(col, Self::avro_to_col_value(value));
                }
                return Some(col_values);
            }
        }
        None
    }

    fn col_values_to_avro(
        col_values: &Option<HashMap<String, ColValue>>,
    ) -> (Value, HashMap<String, String>) {
        let mut avro_types = HashMap::new();
        if col_values.is_none() {
            return (Value::Null, avro_types);
        }

        let mut avro_values = HashMap::new();
        for (col, value) in col_values.as_ref().unwrap() {
            let avro_value = Self::col_value_to_avro(value);
            let (union_position, avro_type) = match avro_value {
                Value::Null => (0, "Null".to_string()),
                Value::String(_) => (1, "String".to_string()),
                Value::Long(_) => (2, "Long".to_string()),
                Value::Double(_) => (3, "Double".to_string()),
                Value::Bytes(_) => (4, "Bytes".to_string()),
                Value::Boolean(_) => (5, "Boolean".to_string()),
                // Not supported
                _ => (0, String::new()),
            };
            avro_values.insert(
                col.into(),
                Value::Union(union_position, Box::new(avro_value)),
            );
            avro_types.insert(col.into(), avro_type);
        }
        (Value::Map(avro_values), avro_types)
    }

    fn col_value_to_avro(value: &ColValue) -> Value {
        match value {
            ColValue::Tiny(v) => Value::Long(*v as i64),
            ColValue::UnsignedTiny(v) => Value::Long(*v as i64),
            ColValue::Short(v) => Value::Long(*v as i64),
            ColValue::UnsignedShort(v) => Value::Long(*v as i64),
            ColValue::Long(v) => Value::Long(*v as i64),
            ColValue::Year(v) => Value::Long(*v as i64),

            ColValue::UnsignedLong(v) => Value::Long(*v as i64),
            ColValue::LongLong(v) => Value::Long(*v),
            ColValue::Bit(v) => Value::Long(*v as i64),
            ColValue::Set(v) => Value::Long(*v as i64),
            ColValue::Enum(v) => Value::Long(*v as i64),
            // may lose precision
            ColValue::UnsignedLongLong(v) => Value::Long(*v as i64),

            ColValue::Float(v) => Value::Double(*v as f64),
            ColValue::Double(v) => Value::Double(*v),
            ColValue::Blob(v) | ColValue::Json(v) | ColValue::RawString(v) => {
                Value::Bytes(v.clone())
            }

            ColValue::Decimal(v)
            | ColValue::Time(v)
            | ColValue::Date(v)
            | ColValue::DateTime(v)
            | ColValue::Timestamp(v)
            | ColValue::String(v)
            | ColValue::Set2(v)
            | ColValue::Enum2(v)
            | ColValue::Json2(v) => Value::String(v.clone()),

            ColValue::Json3(v) => Value::String(v.to_string()),

            ColValue::MongoDoc(v) => Value::String(v.to_string()),

            ColValue::Bool(v) => Value::Boolean(*v),
            ColValue::None => Value::Null,
        }
    }

    fn avro_to_col_value(value: Value) -> ColValue {
        match value {
            Value::Long(v) => ColValue::LongLong(v),
            Value::Double(v) => ColValue::Double(v),
            Value::Bytes(v) => ColValue::Blob(v),
            Value::String(v) => ColValue::String(v),
            Value::Boolean(v) => ColValue::Bool(v),
            Value::Null => ColValue::None,
            Value::Union(_, v) => Self::avro_to_col_value(*v),
            // NOT supported
            _ => ColValue::None,
        }
    }

    fn avro_to_map(value: Value) -> HashMap<String, Value> {
        let mut avro_map = HashMap::new();
        if let Value::Record(record) = value {
            for (field, value) in record {
                avro_map.insert(field, value);
            }
        }
        avro_map
    }

    async fn get_tb_meta<'a>(
        &'a mut self,
        row_data: &RowData,
    ) -> anyhow::Result<Option<&'a RdbTbMeta>> {
        if let Some(meta_manager) = self.meta_manager.as_mut() {
            let tb_meta = meta_manager
                .get_tb_meta(&row_data.schema, &row_data.tb)
                .await?;
            return Ok(Some(tb_meta));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const STRING_COL: &str = "string_col";
    const LONG_COL: &str = "long_col";
    const DOUBLE_COL: &str = "double_col";
    const BYTES_COL: &str = "bytes_col";
    const BOOLEAN_COL: &str = "boolean_col";
    const NULL_COL: &str = "null_col";

    #[tokio::test]
    async fn test_row_data_to_avro() {
        let schema = "db1";
        let tb = "tb1";

        let mut before = HashMap::new();
        before.insert(STRING_COL.into(), ColValue::String("string_before".into()));
        before.insert(LONG_COL.into(), ColValue::LongLong(1));
        before.insert(DOUBLE_COL.into(), ColValue::Double(1.1));
        before.insert(BYTES_COL.into(), ColValue::Blob(vec![1, 2, 3, 4]));
        before.insert(BOOLEAN_COL.into(), ColValue::Bool(false));
        before.insert(NULL_COL.into(), ColValue::None);

        let mut after = HashMap::new();
        after.insert(STRING_COL.into(), ColValue::String("string_after".into()));
        after.insert(LONG_COL.into(), ColValue::LongLong(2));
        after.insert(DOUBLE_COL.into(), ColValue::Double(2.2));
        after.insert(BYTES_COL.into(), ColValue::Blob(vec![5, 6, 7, 8]));
        after.insert(BOOLEAN_COL.into(), ColValue::Bool(true));
        after.insert(NULL_COL.into(), ColValue::None);

        let mut avro_converter = AvroConverter::new(None, false);
        let mut row_data =
            RowData::new(schema.into(), tb.into(), RowType::Insert, None, Some(after));

        // insert
        validate_row_data(&mut avro_converter, &row_data).await;
        // update
        row_data.row_type = RowType::Update;
        row_data.before = Some(before);
        row_data.refresh_data_size();
        validate_row_data(&mut avro_converter, &row_data).await;
        // delete
        row_data.row_type = RowType::Delete;
        row_data.after = None;
        row_data.refresh_data_size();
        validate_row_data(&mut avro_converter, &row_data).await;
    }

    #[tokio::test]
    async fn test_ddl_data_to_avro() {
        let mut avro_converter = AvroConverter::new(None, false);

        let ddl_data = DdlData {
            default_schema: "db1".to_string(),
            query: "create table a(id int);".to_string(),
            ddl_type: DdlType::CreateTable,
            db_type: DbType::Mysql,
            ..Default::default()
        };
        validate_ddl_data(&mut avro_converter, &ddl_data).await;
    }

    async fn validate_row_data(avro_converter: &mut AvroConverter, row_data: &RowData) {
        let payload = avro_converter
            .row_data_to_avro_value(row_data.clone())
            .await
            .unwrap();
        let dt_data = avro_converter.avro_value_to_dt_data(payload).unwrap();
        if let DtData::Dml {
            row_data: decoded_row_data,
        } = dt_data
        {
            assert_eq!(row_data.to_owned(), decoded_row_data)
        } else {
            panic!()
        }
    }

    async fn validate_ddl_data(avro_converter: &mut AvroConverter, ddl_data: &DdlData) {
        let payload = avro_converter
            .ddl_data_to_avro_value(ddl_data.clone())
            .await
            .unwrap();
        let dt_data = avro_converter.avro_value_to_dt_data(payload).unwrap();
        if let DtData::Ddl {
            ddl_data: decoded_ddl_data,
        } = dt_data
        {
            assert_eq!(ddl_data.to_owned(), decoded_ddl_data)
        } else {
            panic!()
        }
    }
}
