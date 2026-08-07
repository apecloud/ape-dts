use tiberius::{Query, Row};

use crate::meta::{col_value::ColValue, mssql::mssql_col_type::MssqlColType};

pub struct MssqlColValueConvertor;

impl MssqlColValueConvertor {
    pub fn from_query(
        _row: &Row,
        _col: &str,
        _col_type: &MssqlColType,
    ) -> anyhow::Result<ColValue> {
        todo!("mssql row value conversion is not implemented")
    }

    pub fn bind<'a>(
        _query: &mut Query<'a>,
        _value: Option<&'a ColValue>,
        _col_type: &MssqlColType,
    ) -> anyhow::Result<()> {
        todo!("mssql query parameter binding is not implemented")
    }
}
