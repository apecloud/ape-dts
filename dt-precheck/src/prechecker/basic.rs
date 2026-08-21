use dt_common::{config::config_enums::DbType, rdb_filter::RdbFilter};

pub struct BasicPrechecker {}

impl BasicPrechecker {
    pub fn is_filter_pattern(db_type: DbType, filter: &RdbFilter) -> bool {
        for schema in filter.do_schemas.iter() {
            if RdbFilter::is_pattern(schema, &db_type) {
                return true;
            }
        }
        for schema in filter.ignore_schemas.iter() {
            if RdbFilter::is_pattern(schema, &db_type) {
                return true;
            }
        }
        for (db, schema, table) in filter.do_tbs.iter() {
            if RdbFilter::is_pattern(db, &db_type)
                || RdbFilter::is_pattern(schema, &db_type)
                || RdbFilter::is_pattern(table, &db_type)
            {
                return true;
            }
        }
        for (db, schema, table) in filter.ignore_tbs.iter() {
            if RdbFilter::is_pattern(db, &db_type)
                || RdbFilter::is_pattern(schema, &db_type)
                || RdbFilter::is_pattern(table, &db_type)
            {
                return true;
            }
        }

        false
    }
}
