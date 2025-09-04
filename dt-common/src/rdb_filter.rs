use std::collections::{HashMap, HashSet};

use anyhow::Context;
use dashmap::DashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::meta::dcl_meta::dcl_type::DclType;
use crate::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, filter_config::FilterConfig,
    },
    meta::{
        ddl_meta::ddl_type::DdlType, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    utils::sql_util::SqlUtil,
};

type IgnoreCols = HashMap<(String, String), HashSet<String>>;
type WhereConditions = HashMap<(String, String), String>;

const JSON_PREFIX: &str = "json:";

#[derive(Debug, Clone)]
pub struct RdbFilter {
    pub db_type: DbType,
    pub do_schemas: HashSet<String>,
    pub ignore_schemas: HashSet<String>,
    pub do_tbs: HashSet<(String, String)>,
    pub ignore_tbs: HashSet<(String, String)>,
    pub ignore_cols: IgnoreCols,
    pub do_events: HashSet<String>,
    pub do_structures: HashSet<String>,
    pub do_ddls: HashSet<String>,
    pub do_dcls: HashSet<String>,
    pub ignore_cmds: HashSet<String>,
    pub where_conditions: WhereConditions,
    pub cache: DashMap<(String, String), bool>,
}

impl RdbFilter {
    pub fn from_config(config: &FilterConfig, db_type: &DbType) -> anyhow::Result<Self> {
        Ok(Self {
            db_type: db_type.to_owned(),
            do_schemas: Self::parse_single_tokens(&config.do_schemas, db_type)?,
            ignore_schemas: Self::parse_single_tokens(&config.ignore_schemas, db_type)?,
            do_tbs: Self::parse_pair_tokens(&config.do_tbs, db_type)?,
            ignore_tbs: Self::parse_pair_tokens(&config.ignore_tbs, db_type)?,
            ignore_cols: Self::parse_ignore_cols(&config.ignore_cols)?,
            do_events: Self::parse_single_tokens(&config.do_events, db_type)?,
            do_structures: Self::parse_single_tokens(&config.do_structures, db_type)?,
            do_ddls: Self::parse_single_tokens(&config.do_ddls, db_type)?,
            do_dcls: Self::parse_single_tokens(&config.do_dcls, db_type)?,
            ignore_cmds: Self::parse_single_tokens(&config.ignore_cmds, db_type)?,
            where_conditions: Self::parse_where_conditions(&config.where_conditions)?,
            cache: DashMap::new(),
        })
    }

    pub fn filter_schema(&self, schema: &str) -> bool {
        let tb = "*";
        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let filter = Self::contain_tb(&self.ignore_tbs, schema, tb, &escape_pairs)
            || Self::contain_schema(&self.ignore_schemas, schema, &escape_pairs);

        if filter {
            return filter;
        }

        let do_tb_schemas: HashSet<String> = self.do_tbs.iter().map(|(d, _)| d.clone()).collect();
        let keep = Self::contain_schema(&self.do_schemas, schema, &escape_pairs)
            || Self::contain_schema(&do_tb_schemas, schema, &escape_pairs);
        !keep
    }

    pub fn filter_tb(&self, schema: &str, tb: &str) -> bool {
        if let Some(cache) = self.cache.get(&(schema.to_string(), tb.to_string())) {
            return *cache;
        }

        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let filter = Self::contain_tb(&self.ignore_tbs, schema, tb, &escape_pairs)
            || Self::contain_schema(&self.ignore_schemas, schema, &escape_pairs);
        let keep = Self::contain_tb(&self.do_tbs, schema, tb, &escape_pairs)
            || Self::contain_schema(&self.do_schemas, schema, &escape_pairs);

        let filter = filter || !keep;
        self.cache
            .insert((schema.to_string(), tb.to_string()), filter);

        filter
    }

    pub fn filter_event(&self, schema: &str, tb: &str, row_type: &RowType) -> bool {
        if !Self::match_all(&self.do_events) && !self.do_events.contains(&row_type.to_string()) {
            return true;
        }
        self.filter_tb(schema, tb)
    }

    pub fn filter_all_ddl(&self) -> bool {
        self.do_ddls.is_empty()
    }

    pub fn filter_ddl(&self, schema: &str, tb: &str, ddl_type: &DdlType) -> bool {
        if !Self::match_all(&self.do_ddls) && !self.do_ddls.contains(&ddl_type.to_string()) {
            return true;
        }

        if tb.is_empty() {
            self.filter_schema(schema)
        } else {
            self.filter_tb(schema, tb)
        }
    }

    pub fn filter_all_dcl(&self) -> bool {
        self.do_dcls.is_empty()
    }

    pub fn filter_dcl(&mut self, dcl_type: &DclType) -> bool {
        !Self::match_all(&self.do_dcls) && !self.do_dcls.contains(&dcl_type.to_string())
    }

    pub fn filter_structure(&self, structure_type: &StructureType) -> bool {
        !Self::match_all(&self.do_structures)
            && !self.do_structures.contains(&structure_type.to_string())
    }

    pub fn filter_cmd(&self, cmd: &str) -> bool {
        self.ignore_cmds.contains(cmd)
    }

    pub fn get_ignore_cols(&self, schema: &str, tb: &str) -> Option<&HashSet<String>> {
        self.ignore_cols.get(&(schema.to_string(), tb.to_string()))
    }

    pub fn add_ignore_tb(&mut self, schema: &str, tb: &str) {
        self.ignore_tbs.insert((schema.into(), tb.into()));
    }

    pub fn add_do_tb(&mut self, schema: &str, tb: &str) {
        self.do_tbs.insert((schema.into(), tb.into()));
    }

    pub fn get_where_condition(&self, schema: &str, tb: &str) -> Option<&String> {
        self.where_conditions
            .get(&(schema.to_string(), tb.to_string()))
    }

    pub fn is_pattern(pattern: &str, db_type: &DbType) -> bool {
        for escape_pair in SqlUtil::get_escape_pairs(db_type).iter() {
            if SqlUtil::is_escaped(pattern, escape_pair) {
                return false;
            }
        }
        pattern.contains("*") || pattern.contains("?")
    }

    fn match_all(set: &HashSet<String>) -> bool {
        set.len() == 1 && set.contains("*")
    }

    fn contain_tb(
        set: &HashSet<(String, String)>,
        schema: &str,
        tb: &str,
        escape_pairs: &[(char, char)],
    ) -> bool {
        for i in set.iter() {
            if Self::match_token(&i.0, schema, escape_pairs)
                && Self::match_token(&i.1, tb, escape_pairs)
            {
                return true;
            }
        }
        false
    }

    fn contain_schema(set: &HashSet<String>, item: &str, escape_pairs: &[(char, char)]) -> bool {
        for i in set.iter() {
            if Self::match_token(i, item, escape_pairs) {
                return true;
            }
        }
        false
    }

    fn match_token(pattern: &str, item: &str, escape_pairs: &[(char, char)]) -> bool {
        // if pattern is enclosed by escapes, it is considered as exactly match
        // example: mysql table name : `aaa*`, it can only match the table `aaa*`, it won't match `aaa_bbb`
        for escape_pair in escape_pairs.iter() {
            if SqlUtil::is_escaped(pattern, escape_pair) {
                return pattern == SqlUtil::escape(item, escape_pair);
            }
        }
        // only support 2 wildchars : '*' and '?', '.' is NOT supported
        // * : matching multiple chars
        // ? : for matching 0-1 chars
        let mut pattern = pattern
            .replace('.', "\\.")
            .replace('*', ".*")
            .replace('?', ".?");
        pattern = format!(r"^{}$", pattern);

        Regex::new(&pattern)
            .with_context(|| format!("invalid filter pattern: [{}]", pattern))
            .unwrap()
            .is_match(item)
    }

    fn parse_pair_tokens(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<HashSet<(String, String)>> {
        let mut results = HashSet::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            results.insert((tokens[i].to_string(), tokens[i + 1].to_string()));
            i += 2;
        }
        Ok(results)
    }

    fn parse_single_tokens(config_str: &str, db_type: &DbType) -> anyhow::Result<HashSet<String>> {
        let tokens = Self::parse_config(config_str, db_type)?;
        let results: HashSet<String> = HashSet::from_iter(tokens);
        Ok(results)
    }

    fn parse_config(config_str: &str, db_type: &DbType) -> anyhow::Result<Vec<String>> {
        let delimiters = vec![',', '.'];
        ConfigTokenParser::parse_config(config_str, db_type, &delimiters)
    }

    fn parse_ignore_cols(config_str: &str) -> anyhow::Result<IgnoreCols> {
        let mut results = IgnoreCols::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }
        // ignore_cols=json:[{"db":"test_db","tb":"tb_1","ignore_cols":{"f_0","f_1"}}]
        #[derive(Serialize, Deserialize)]
        struct IgnoreColsType {
            db: String,
            tb: String,
            ignore_cols: HashSet<String>,
        }
        let config: Vec<IgnoreColsType> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX))?;
        for i in config {
            results.insert((i.db, i.tb), i.ignore_cols);
        }
        Ok(results)
    }

    fn parse_where_conditions(config_str: &str) -> anyhow::Result<WhereConditions> {
        let mut results = WhereConditions::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }
        // where_conditions=json:[{"db":"test_db","tb":"tb_1","condition":"id > 1 and `age` > 100"}]
        #[derive(Serialize, Deserialize)]
        struct Condition {
            db: String,
            tb: String,
            condition: String,
        }
        let config: Vec<Condition> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX))?;
        for i in config {
            results.insert((i.db, i.tb), i.condition);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ignore_cols() {
        let config_str = r#"json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]"#;
        let ignore_cols = RdbFilter::parse_ignore_cols(config_str).unwrap();
        let tb_1 = ignore_cols
            .get(&("db_1".to_string(), "tb_1".to_string()))
            .unwrap();
        let tb_2 = ignore_cols
            .get(&("db_2".to_string(), "tb_2".to_string()))
            .unwrap();
        assert_eq!(tb_1.len(), 2);
        assert!(tb_1.contains(&"f_2".to_string()));
        assert!(tb_1.contains(&"f_3".to_string()));

        assert_eq!(tb_2.len(), 1);
        assert!(tb_2.contains(&"f_3".to_string()));
    }

    #[test]
    fn test_match_token_without_escape() {
        let escape_pairs = vec![];
        // exactly match
        assert!(RdbFilter::match_token("hello", "hello", &escape_pairs));
        assert!(!RdbFilter::match_token("hello", "hellO", &escape_pairs));

        // match with question mark
        assert!(RdbFilter::match_token("he?lo", "hello", &escape_pairs));
        assert!(RdbFilter::match_token("he?lo", "helo", &escape_pairs));
        assert!(!RdbFilter::match_token("he?lo", "helllo", &escape_pairs));

        // match with asterisk
        assert!(RdbFilter::match_token("he*llo", "hello", &escape_pairs));
        assert!(RdbFilter::match_token(
            "he*llo",
            "heeeeeello",
            &escape_pairs
        ));
        assert!(RdbFilter::match_token("he*llo", "hello", &escape_pairs));
        assert!(!RdbFilter::match_token("he*llo", "helo", &escape_pairs));

        // match with dot, should also be exactly match
        assert!(RdbFilter::match_token("h.llo", "h.llo", &escape_pairs));
        assert!(!RdbFilter::match_token("h.llo", "he.llo", &escape_pairs));
        assert!(!RdbFilter::match_token("h.llo", "h.lo", &escape_pairs));
        assert!(!RdbFilter::match_token("h.llo", "hello", &escape_pairs));
    }

    #[test]
    fn test_match_token_with_mysql_escapes() {
        let escape_pairs = SqlUtil::get_escape_pairs(&DbType::Mysql);
        // exactly match
        assert!(RdbFilter::match_token("`hello`", "`hello`", &escape_pairs));
        assert!(!RdbFilter::match_token("`hello`", "`hellO`", &escape_pairs));

        // match with question mark
        assert!(RdbFilter::match_token("`he?lo`", "`he?lo`", &escape_pairs));
        assert!(!RdbFilter::match_token("`he?lo`", "`hello`", &escape_pairs));
        assert!(!RdbFilter::match_token("`he?lo`", "`helo`", &escape_pairs));
        assert!(!RdbFilter::match_token(
            "`he?lo`",
            "`helllo`",
            &escape_pairs
        ));

        // match with asterisk
        assert!(RdbFilter::match_token(
            "`he*llo`",
            "`he*llo`",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`he*llo`",
            "`hello`",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`he*llo`",
            "`heeeeeello`",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`he*llo`",
            "`hello`",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token("`he*llo`", "`helo`", &escape_pairs));

        // match with dot, should also be exactly match
        assert!(RdbFilter::match_token("`h.llo`", "`h.llo`", &escape_pairs));
        assert!(!RdbFilter::match_token(
            "`h.llo`",
            "`he.llo`",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token("`h.llo`", "`h.lo`", &escape_pairs));
        assert!(!RdbFilter::match_token("`h.llo`", "`hello`", &escape_pairs));
    }

    #[test]
    fn test_match_token_with_pg_escapes() {
        let escape_pairs = SqlUtil::get_escape_pairs(&DbType::Pg);
        // exactly match
        assert!(RdbFilter::match_token(
            r#""hello""#,
            r#""hello""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""hello""#,
            r#""hellO""#,
            &escape_pairs
        ));

        // match with question mark
        assert!(RdbFilter::match_token(
            r#""he?lo""#,
            r#""he?lo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he?lo""#,
            r#""hello""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he?lo""#,
            r#""helo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he?lo""#,
            r#""helllo""#,
            &escape_pairs
        ));

        // match with asterisk
        assert!(RdbFilter::match_token(
            r#""he*llo""#,
            r#""he*llo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he*llo""#,
            r#""hello""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he*llo""#,
            r#""heeeeeello""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he*llo""#,
            r#""hello""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""he*llo""#,
            r#""helo""#,
            &escape_pairs
        ));

        // match with dot, should also be exactly match
        assert!(RdbFilter::match_token(
            r#""h.llo""#,
            r#""h.llo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""h.llo""#,
            r#""he.llo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""h.llo""#,
            r#""h.lo""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""h.llo""#,
            r#""hello""#,
            &escape_pairs
        ));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "*".to_string(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: "*.b*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("b", "b", &RowType::Insert));
        assert!(!rdb_filter.filter_event("a", "cbd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "*".to_string(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: "*.`b*`,*.c*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("a", "b*", &RowType::Insert));
        assert!(rdb_filter.filter_event("b", "b*", &RowType::Insert));
        assert!(!rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b", "b", &RowType::Insert));
        assert!(rdb_filter.filter_event("a", "cbd", &RowType::Insert));
        assert!(rdb_filter.filter_event("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs_with_escapes_2() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_tbs: "`db_test_position.aaa`.`b.bbb,.b`,`db_test_position.aaa`.c".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_event("db_test_position.aaa", "b.bbb,.b", &RowType::Insert));
        assert!(!rdb_filter.filter_event("db_test_position.aaa", "c", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_ignore_dbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "*".to_string(),
            ignore_schemas: "a*".to_string(),
            do_tbs: "*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("abc", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b", "cbd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_ignore_dbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "*".to_string(),
            ignore_schemas: "`a*`,b*".to_string(),
            do_tbs: "*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_event("abc", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("bcd", "cbd", &RowType::Insert));
        assert!(rdb_filter.filter_event("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_dbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "b*".to_string(),
            ignore_schemas: "a*".to_string(),
            do_tbs: "aaaaaaa.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("c", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("bcd", "bcd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_dbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_schemas: "`b*`,abc,bcd*,cde".to_string(),
            ignore_schemas: "a*".to_string(),
            do_tbs: "aaaaaaa.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("c", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("b", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("bc", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("abc", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("b*", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("bcd", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("bcde", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("cde", "bcd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_tbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            ignore_schemas: "b*".to_string(),
            do_tbs: "a*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("bcd", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("cde", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("abc", "bcd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_tbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            ignore_schemas: "b*".to_string(),
            do_tbs: "a*.*,`c*`.`*`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_event("bcd", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("cde", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("a", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("abc", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("c", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("cde", "bcd", &RowType::Insert));
        assert!(rdb_filter.filter_event("c*", "bcd", &RowType::Insert));
        assert!(!rdb_filter.filter_event("c*", "*", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_db_without_escapes() {
        let db_type = DbType::Mysql;
        // keep by do_dbs, Not filtered by ignore_dbs
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_schemas: "test_db_2".to_string(),

            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_schemas: "test_db_1".to_string(),

            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            do_schemas: "test_db_1".to_string(),
            ignore_schemas: "*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));

        // keep by do_dbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_tbs: "test_db_1.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_1"));

        // keep by do_dbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_tbs: "test_db_1.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));

        // keep by do_tbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_1"));

        // keep by do_tbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            ignore_schemas: "b*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));

        // keep by do_tbs, NOT filtered by ignore_dbs
        let config = FilterConfig {
            ignore_schemas: "test_db_2".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            ignore_schemas: "test_db_1".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            ignore_schemas: "test_db_*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_1"));
    }

    #[test]
    fn test_rdb_filter_db_with_escapes() {
        let db_type = DbType::Mysql;
        // keep by do_dbs, Not filtered by ignore_dbs
        let config = FilterConfig {
            do_schemas: "`test_db_*`".to_string(),
            ignore_schemas: "`test_db_2`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_*"));

        // keep by do_dbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_schemas: "`test_db_*`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // keep by do_dbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            do_schemas: "`test_db_*`".to_string(),
            ignore_schemas: "test_db*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // keep by do_dbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_schemas: "`test_db_*`".to_string(),
            ignore_tbs: "`test_db_*`.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_*"));

        // keep by do_dbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_schemas: "`test_db_*`".to_string(),
            ignore_tbs: "`test_db_*`.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // keep by do_tbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            ignore_tbs: "`test_db_*`.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_*"));

        // keep by do_tbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            ignore_schemas: "b*".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            ignore_tbs: "`test_db_*`.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // keep by do_tbs, NOT filtered by ignore_dbs
        let config = FilterConfig {
            ignore_schemas: "test_db_2".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_schema("test_db_*"));

        // keep by do_tbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            ignore_schemas: "`test_db_*`".to_string(),
            do_tbs: "test_db_*.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // keep by do_tbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            ignore_schemas: "test_db*".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));

        // ignore some tbs in db, but not all tbs in db filtered
        let config = FilterConfig {
            ignore_tbs: "test_db_*.test_tb_*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_filter.filter_schema("test_db_*"));
    }

    #[test]
    fn test_rdb_filter_event() {
        let db_type = DbType::Mysql;

        // keep do_events empty
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_schemas: "test_db_2".to_string(),
            do_events: "*".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Insert));
        assert!(!rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Update));
        assert!(!rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Delete));

        // explicitly set do_events
        let config = FilterConfig {
            do_schemas: "test_db_*".to_string(),
            ignore_schemas: "test_db_2".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Insert));
        assert!(rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Update));
        assert!(rdb_filter.filter_event("test_db_1", "aaaa", &RowType::Delete));
    }
}
