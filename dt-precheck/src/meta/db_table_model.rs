use std::collections::BTreeSet;

use anyhow::{bail, Result};
use dt_common::{
    filter::DtFilter,
    resource::{config_parser, ResourceKind},
};

pub struct DbTable {
    pub database_name: String,
    pub is_all: bool,
    pub table_name: String,
}

impl DbTable {
    pub fn from_filter(filter: &DtFilter) -> Result<Vec<Self>> {
        let mut ids = BTreeSet::new();
        for (kind, rules) in filter.resource_filters() {
            if *kind != ResourceKind::Column {
                for id in &rules.do_set {
                    ids.insert(config_parser::exact_path(
                        id.segments().to_vec(),
                        filter.db_type(),
                    )?);
                }
            }
        }
        let mut result = Vec::new();
        for id in ids {
            if filter.filter(&id)? {
                continue;
            }
            match id.segments() {
                [namespace] => result.push(Self {
                    database_name: namespace.clone(),
                    is_all: true,
                    table_name: String::new(),
                }),
                [namespace, table] => result.push(Self {
                    database_name: namespace.clone(),
                    is_all: false,
                    table_name: table.clone(),
                }),
                _ => bail!("unsupported precheck resource: {id}"),
            }
        }
        Ok(result)
    }

    pub fn get_db_names(arr: &[DbTable]) -> Result<Vec<String>> {
        Ok(arr
            .iter()
            .filter(|x| !x.database_name.is_empty())
            .map(|x| String::from(x.database_name.as_str()))
            .collect())
    }

    pub fn get_tb_names(arr: &[DbTable]) -> Result<(Vec<String>, Vec<String>)> {
        Ok((
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.table_name.as_str()))
                .collect(),
        ))
    }

    // Returns: Vec<String, do_dbs's database names>, Vec<String, do_tbs's database_names>, Vec<String, do_tbs's table names>
    #[allow(clippy::type_complexity)]
    pub fn get_config_maps(arr: &[DbTable]) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
        Ok((
            arr.iter()
                .filter(|x| !x.database_name.is_empty() && x.is_all)
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| format!("{}.{}", x.database_name, x.table_name))
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use dt_common::{
        config::{config_enums::DbType, filter_config::FilterConfig},
        filter::DtFilter,
    };

    use super::*;
    use crate::prechecker::basic::BasicPrechecker;

    #[test]
    fn legacy_and_basic_selectors_keep_quoted_names_and_exclusions() {
        for text in [
            "[filter]\ndo_tbs=`db.name`.`order.name`,`db.name`.ignored\nignore_tbs=`db.name`.ignored",
            "[rules.table]\ndo=`db.name`.`order.name`,`db.name`.ignored\nignore=`db.name`.ignored\n[rules.column]\nignore=*.*.secret",
        ] {
            let mut ini = configparser::ini::Ini::new();
            ini.read(text.into()).unwrap();
            let filter = DtFilter::from_config(&FilterConfig::from_ini(&ini).unwrap(), DbType::Mysql).unwrap();
            assert!(!BasicPrechecker::is_filter_pattern(&filter));
            let tables = DbTable::from_filter(&filter).unwrap();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0].database_name, "db.name");
            assert_eq!(tables[0].table_name, "order.name");
            assert!(!tables[0].is_all);
        }
    }
}
