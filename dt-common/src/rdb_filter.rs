use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};
use dashmap::DashMap;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::meta::dcl_meta::dcl_type::DclType;
use crate::{
    config::{
        config_enums::DbType,
        config_token_parser::{ConfigTokenParser, TokenEscapePair},
        filter_config::FilterConfig,
    },
    error::DtError,
    meta::{
        ddl_meta::ddl_type::DdlType, mssql::MSSQL_DEFAULT_SCHEMA, row_type::RowType,
        struct_meta::structure::structure_type::StructureType,
    },
    utils::sql_util::{CharEscapePair, SqlUtil},
};

type DbSchemaTb = (String, String, String);
type IgnoreCols = HashMap<DbSchemaTb, HashSet<String>>;
type WhereConditions = HashMap<DbSchemaTb, String>;

const JSON_PREFIX: &str = "json:";
const EMPTY_DB: &str = "";

const REGEX_ESCAPE_PAIR: (&str, &str) = ("r#", "#");

#[derive(Debug, Clone)]
pub struct RdbFilter {
    pub db_type: DbType,
    pub do_schemas: HashSet<String>,
    pub ignore_schemas: HashSet<String>,
    pub do_tbs: HashSet<DbSchemaTb>,
    pub ignore_tbs: HashSet<DbSchemaTb>,
    pub ignore_cols: IgnoreCols,
    pub do_events: HashSet<String>,
    pub do_structures: HashSet<String>,
    pub do_ddls: HashSet<String>,
    pub do_dcls: HashSet<String>,
    pub ignore_cmds: HashSet<String>,
    pub where_conditions: WhereConditions,
    pub cache: DashMap<DbSchemaTb, bool>,
}

impl RdbFilter {
    pub fn from_config(config: &FilterConfig, db_type: &DbType) -> anyhow::Result<Self> {
        Ok(Self {
            db_type: db_type.to_owned(),
            do_schemas: Self::parse_single_tokens(&config.do_schemas, db_type)?,
            ignore_schemas: Self::parse_single_tokens(&config.ignore_schemas, db_type)?,
            do_tbs: Self::parse_table_tokens(&config.do_tbs, db_type)?,
            ignore_tbs: Self::parse_table_tokens(&config.ignore_tbs, db_type)?,
            ignore_cols: Self::parse_ignore_cols(&config.ignore_cols, db_type)?,
            do_events: Self::parse_single_tokens(&config.do_events, db_type)?,
            do_structures: Self::parse_single_tokens(&config.do_structures, db_type)?,
            do_ddls: Self::parse_single_tokens(&config.do_ddls, db_type)?,
            do_dcls: Self::parse_single_tokens(&config.do_dcls, db_type)?,
            ignore_cmds: Self::parse_single_tokens(&config.ignore_cmds, db_type)?,
            where_conditions: Self::parse_where_conditions(&config.where_conditions, db_type)?,
            cache: DashMap::new(),
        })
    }

    pub fn filter_schema(&self, schema: &str) -> bool {
        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let filter = Self::contains_token(&self.ignore_schemas, schema, &escape_pairs)
            || (!matches!(self.db_type, DbType::Mssql)
                && Self::contains_table(&self.ignore_tbs, EMPTY_DB, schema, "*", &escape_pairs));

        if filter {
            return filter;
        }

        let keep_by_table = self.do_tbs.iter().any(|(table_db, table_schema, _)| {
            let table_namespace = if matches!(self.db_type, DbType::Mssql) {
                table_db
            } else {
                table_schema
            };
            Self::match_token(table_namespace, schema, &escape_pairs)
        });
        let keep = Self::contains_token(&self.do_schemas, schema, &escape_pairs) || keep_by_table;
        !keep
    }

    pub fn filter_tb(&self, schema: &str, tb: &str) -> bool {
        self.filter_tb_with_db(EMPTY_DB, schema, tb)
    }

    pub fn filter_tb_with_db(&self, db: &str, schema: &str, tb: &str) -> bool {
        let key = Self::table_key(db, schema, tb);
        if let Some(cache) = self.cache.get(&key) {
            return *cache;
        }

        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let namespace = if matches!(self.db_type, DbType::Mssql) {
            db
        } else {
            schema
        };
        let filter = Self::contains_table(&self.ignore_tbs, db, schema, tb, &escape_pairs)
            || Self::contains_token(&self.ignore_schemas, namespace, &escape_pairs);
        let keep = Self::contains_table(&self.do_tbs, db, schema, tb, &escape_pairs)
            || Self::contains_token(&self.do_schemas, namespace, &escape_pairs);

        let filter = filter || !keep;
        self.cache.insert(key, filter);

        filter
    }

    pub fn filter_event(&self, schema: &str, tb: &str, row_type: &RowType) -> bool {
        self.filter_event_with_db(EMPTY_DB, schema, tb, row_type)
    }

    pub fn filter_event_with_db(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
        row_type: &RowType,
    ) -> bool {
        if !Self::match_all(&self.do_events) && !self.do_events.contains(&row_type.to_string()) {
            return true;
        }
        self.filter_tb_with_db(db, schema, tb)
    }

    pub fn filter_all_ddl(&self) -> bool {
        self.do_ddls.is_empty()
    }

    pub fn filter_spec_ddl(&self, ddl_type: &DdlType) -> bool {
        !Self::match_all(&self.do_ddls) && !self.do_ddls.contains(&ddl_type.to_string())
    }

    pub fn filter_ddl(&self, schema: &str, tb: &str, ddl_type: &DdlType) -> bool {
        self.filter_ddl_with_db(EMPTY_DB, schema, tb, ddl_type)
    }

    pub fn filter_ddl_with_db(&self, db: &str, schema: &str, tb: &str, ddl_type: &DdlType) -> bool {
        if self.filter_spec_ddl(ddl_type) {
            return true;
        }

        if tb.is_empty() {
            if matches!(self.db_type, DbType::Mssql) {
                self.filter_schema(db)
            } else {
                self.filter_schema(schema)
            }
        } else {
            self.filter_tb_with_db(db, schema, tb)
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
        self.get_ignore_cols_with_db(EMPTY_DB, schema, tb)
    }

    pub fn get_ignore_cols_with_db(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> Option<&HashSet<String>> {
        self.ignore_cols.get(&Self::table_key(db, schema, tb))
    }

    pub fn add_ignore_tb(&mut self, schema: &str, tb: &str) {
        self.add_ignore_tb_with_db(EMPTY_DB, schema, tb);
    }

    pub fn add_ignore_tb_with_db(&mut self, db: &str, schema: &str, tb: &str) {
        self.ignore_tbs.insert(Self::table_key(db, schema, tb));
        self.cache.clear();
    }

    pub fn add_do_tb(&mut self, schema: &str, tb: &str) {
        self.add_do_tb_with_db(EMPTY_DB, schema, tb);
    }

    pub fn add_do_tb_with_db(&mut self, db: &str, schema: &str, tb: &str) {
        self.do_tbs.insert(Self::table_key(db, schema, tb));
        self.cache.clear();
    }

    pub fn get_where_condition(&self, schema: &str, tb: &str) -> Option<&String> {
        self.get_where_condition_with_db(EMPTY_DB, schema, tb)
    }

    pub fn get_where_condition_with_db(&self, db: &str, schema: &str, tb: &str) -> Option<&String> {
        self.where_conditions.get(&Self::table_key(db, schema, tb))
    }

    pub fn is_pattern(pattern: &str, db_type: &DbType) -> bool {
        for escape_pair in SqlUtil::get_escape_pairs(db_type).iter() {
            if SqlUtil::is_escaped(pattern, escape_pair) {
                return false;
            }
        }
        pattern.contains("*") || pattern.contains("?") || pattern.starts_with(REGEX_ESCAPE_PAIR.0)
    }

    fn match_all(set: &HashSet<String>) -> bool {
        set.len() == 1 && set.contains("*")
    }

    fn contains_table(
        set: &HashSet<DbSchemaTb>,
        db: &str,
        schema: &str,
        tb: &str,
        escape_pairs: &[CharEscapePair],
    ) -> bool {
        for i in set.iter() {
            if Self::match_token(&i.0, db, escape_pairs)
                && Self::match_token(&i.1, schema, escape_pairs)
                && Self::match_token(&i.2, tb, escape_pairs)
            {
                return true;
            }
        }
        false
    }

    fn contains_token(set: &HashSet<String>, item: &str, escape_pairs: &[CharEscapePair]) -> bool {
        for i in set.iter() {
            if Self::match_token(i, item, escape_pairs) {
                return true;
            }
        }
        false
    }

    fn match_token(pattern: &str, item: &str, escape_pairs: &[CharEscapePair]) -> bool {
        // if pattern is enclosed by escapes, it is considered as exactly match
        // example: mysql table name : `aaa*`, it can only match the table `aaa*`, it won't match `aaa_bbb`
        for escape_pair in escape_pairs.iter() {
            if SqlUtil::is_escaped(pattern, escape_pair) {
                return pattern == SqlUtil::escape(item, escape_pair);
            }
        }

        let mut pattern = pattern.to_string();
        if !pattern.starts_with(REGEX_ESCAPE_PAIR.0) || !pattern.ends_with(REGEX_ESCAPE_PAIR.1) {
            // only support 2 wildchars : '*' and '?', '.' is NOT supported
            // * : matching multiple chars
            // ? : for matching 0-1 chars
            pattern = pattern
                .replace('.', "\\.")
                .replace('*', ".*")
                .replace('?', ".?");
        } else {
            // support raw regex expression.
            // a raw regex expression string should be enclosed by `r#` and `#` as escape pair
            // eg: `r#.*#` indicates the regex expression `.*`
            pattern.drain(..2);
            pattern.pop();
        }
        pattern = format!(r"^{}$", pattern);

        Regex::new(&pattern).is_ok_and(|regex| regex.is_match(item))
    }

    fn parse_table_tokens(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<HashSet<DbSchemaTb>> {
        if matches!(db_type, DbType::Mssql) {
            return Self::parse_mssql_table_tokens(config_str, db_type);
        }

        let mut results = HashSet::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            results.insert(Self::table_key(EMPTY_DB, &tokens[i], &tokens[i + 1]));
            i += 2;
        }
        Ok(results)
    }

    fn parse_mssql_table_tokens(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<HashSet<DbSchemaTb>> {
        let mut results = HashSet::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }

        let custom_escape_pairs = Self::regex_escape_pairs();
        let tokens = ConfigTokenParser::parse_config_with_delimiters(
            config_str,
            db_type,
            &[',', '.'],
            Some(&custom_escape_pairs),
        )?;
        Self::validate_regex_tokens(&tokens)?;
        for entry in tokens.split(|token| token == ",") {
            let key = match entry {
                [db, dot, tb] if dot == "." => Self::table_key(db, MSSQL_DEFAULT_SCHEMA, tb),
                [db, dot_1, schema, dot_2, tb] if dot_1 == "." && dot_2 == "." => {
                    Self::table_key(db, schema, tb)
                }
                _ => {
                    bail!(DtError::invalid_config(format!(
                        "invalid MSSQL table pattern: {}; expected database.table or database.schema.table",
                        entry.concat()
                    )))
                }
            };
            results.insert(key);
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
        let custom_escape_pairs = Self::regex_escape_pairs();
        let tokens = ConfigTokenParser::parse_config(
            config_str,
            db_type,
            &delimiters,
            Some(&custom_escape_pairs),
        )?;
        Self::validate_regex_tokens(&tokens)?;
        Ok(tokens)
    }

    fn regex_escape_pairs() -> Vec<TokenEscapePair> {
        vec![TokenEscapePair::from((
            REGEX_ESCAPE_PAIR.0.to_string(),
            REGEX_ESCAPE_PAIR.1.to_string(),
        ))]
    }

    fn validate_regex_tokens(tokens: &[String]) -> anyhow::Result<()> {
        for token in tokens {
            if token.starts_with(REGEX_ESCAPE_PAIR.0) && token.ends_with(REGEX_ESCAPE_PAIR.1) {
                let pattern = &token[REGEX_ESCAPE_PAIR.0.len()..token.len() - 1];
                Regex::new(pattern).context(DtError::invalid_config(format!(
                    "invalid filter regex {token}"
                )))?;
            }
        }
        Ok(())
    }

    fn table_key(db: &str, schema: &str, tb: &str) -> DbSchemaTb {
        (db.to_string(), schema.to_string(), tb.to_string())
    }

    fn parse_ignore_cols(config_str: &str, db_type: &DbType) -> anyhow::Result<IgnoreCols> {
        let mut results = IgnoreCols::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }
        // ignore_cols=json:[{"db":"test_db","tb":"tb_1","ignore_cols":{"f_0","f_1"}}]
        #[derive(Serialize, Deserialize)]
        struct IgnoreColsConfig {
            db: String,
            tb: String,
            ignore_cols: HashSet<String>,
        }
        let config: Vec<IgnoreColsConfig> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX)).context(
                DtError::invalid_config("config [filter].ignore_cols is invalid JSON"),
            )?;
        for i in config {
            let key = Self::parse_json_table_key(&i.db, &i.tb, db_type)?;
            results.insert(key, i.ignore_cols);
        }
        Ok(results)
    }

    fn parse_where_conditions(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<WhereConditions> {
        let mut results = WhereConditions::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }
        // where_conditions=json:[{"db":"test_db","tb":"tb_1","condition":"id > 1 and `age` > 100"}]
        #[derive(Serialize, Deserialize)]
        struct WhereConditionConfig {
            db: String,
            tb: String,
            condition: String,
        }
        let config: Vec<WhereConditionConfig> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX)).context(
                DtError::invalid_config("config [filter].where_conditions is invalid JSON"),
            )?;
        for i in config {
            let key = Self::parse_json_table_key(&i.db, &i.tb, db_type)?;
            results.insert(key, i.condition);
        }
        Ok(results)
    }

    fn parse_json_table_key(db: &str, tb: &str, db_type: &DbType) -> anyhow::Result<DbSchemaTb> {
        if !matches!(db_type, DbType::Mssql) {
            return Ok(Self::table_key(EMPTY_DB, db, tb));
        }
        if db.is_empty() {
            bail!(DtError::invalid_config(
                "MSSQL table selector database must not be empty"
            ));
        }

        let parts = ConfigTokenParser::parse_config(tb, db_type, &['.'], None)?;
        let (schema, tb) = match parts.as_slice() {
            [tb] => (MSSQL_DEFAULT_SCHEMA.to_string(), tb.clone()),
            [schema, tb] => (schema.clone(), tb.clone()),
            _ => {
                bail!(DtError::invalid_config(format!(
                    "invalid MSSQL table selector: database={db}, table={tb}"
                )))
            }
        };
        Ok(Self::table_key(
            &SqlUtil::unescape_by_db_type(db, db_type),
            &SqlUtil::unescape_by_db_type(&schema, db_type),
            &SqlUtil::unescape_by_db_type(&tb, db_type),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ignore_cols() {
        let config_str = r#"json:[{"db":"db_1","tb":"tb_1","ignore_cols":["f_2","f_3"]},{"db":"db_2","tb":"tb_2","ignore_cols":["f_3"]}]"#;
        let ignore_cols = RdbFilter::parse_ignore_cols(config_str, &DbType::Mysql).unwrap();
        let tb_1 = ignore_cols
            .get(&(String::new(), "db_1".to_string(), "tb_1".to_string()))
            .unwrap();
        let tb_2 = ignore_cols
            .get(&(String::new(), "db_2".to_string(), "tb_2".to_string()))
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

        // match with `r#` and `#`
        assert!(RdbFilter::match_token("r#hello#", "hello", &escape_pairs));
        assert!(RdbFilter::match_token("r#he?llo#", "hllo", &escape_pairs));
        assert!(RdbFilter::match_token("r#he?llo#", "hello", &escape_pairs));
        assert!(RdbFilter::match_token("r#he*llo#", "hllo", &escape_pairs));
        assert!(RdbFilter::match_token(
            "r#he*llo#",
            "heeeeeeeello",
            &escape_pairs
        ));
        assert!(RdbFilter::match_token("r#h.?llo#", "htllo", &escape_pairs));
        assert!(RdbFilter::match_token(
            "r#h.*llo#",
            "htestllo",
            &escape_pairs
        ));
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

        // match with `r#` and `#`, should also be exactly match
        assert!(RdbFilter::match_token(
            "`r#hello#`",
            "r#hello#",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#hello#`",
            "hello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            "`r#he?llo#`",
            "r#he?llo#",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#he?llo#`",
            "hllo",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#he?llo#`",
            "hello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            "`r#he*llo#`",
            "r#he*llo#",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#he*llo#`",
            "hllo",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#he*llo#`",
            "heeeeeeeello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            "`r#h.?llo#`",
            "r#h.?llo#",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#h.?llo#`",
            "htllo",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            "`r#h.*llo#`",
            "r#h.*llo#",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            "`r#h.*llo#`",
            "htestllo",
            &escape_pairs
        ));
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

        // match with `r#` and `#`, should also be exactly match
        assert!(RdbFilter::match_token(
            r#""r#hello#""#,
            r#""r#hello#""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#hello#""#,
            "hello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            r#""r#he?llo#""#,
            r#""r#he?llo#""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#he?llo#""#,
            "hllo",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#he?llo#""#,
            "hello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            r#""r#he*llo#""#,
            r#""r#he*llo#""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#he*llo#""#,
            "hllo",
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#he*llo#""#,
            "heeeeeello",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            r#""r#h.?llo#""#,
            r#""r#h.?llo#""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#h.?llo#""#,
            "htllo",
            &escape_pairs
        ));

        assert!(RdbFilter::match_token(
            r#""r#h.*llo#""#,
            r#""r#h.*llo#""#,
            &escape_pairs
        ));
        assert!(!RdbFilter::match_token(
            r#""r#h.*llo#""#,
            "htestllo",
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
    fn test_mssql_filter_with_escaped_right_delimiters() {
        let config = FilterConfig {
            do_tbs: "[db]]name].[schema]]name].[table]]name]".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let rdb_filter = RdbFilter::from_config(&config, &DbType::Mssql).unwrap();

        assert!(!rdb_filter.filter_event_with_db(
            "db]name",
            "schema]name",
            "table]name",
            &RowType::Insert
        ));
        assert!(rdb_filter.filter_event_with_db(
            "db]name",
            "schema]name",
            "other",
            &RowType::Insert
        ));
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

    #[test]
    fn table_filter_cache_is_isolated_by_db() {
        let config = FilterConfig {
            do_events: "*".to_string(),
            ..Default::default()
        };
        let mut filter = RdbFilter::from_config(&config, &DbType::Mssql).unwrap();
        filter.do_schemas.insert("*".to_string());
        filter.add_ignore_tb_with_db("db1", "schema1", "tb1");

        assert!(filter.filter_tb_with_db("db1", "schema1", "tb1"));
        assert!(!filter.filter_tb_with_db("db2", "schema1", "tb1"));
    }

    #[test]
    fn configured_tables_use_empty_physical_db() {
        for db_type in [DbType::Mysql, DbType::Pg] {
            let config = FilterConfig {
                do_tbs: "schema1.tb1".to_string(),
                do_events: "*".to_string(),
                ..Default::default()
            };
            let filter = RdbFilter::from_config(&config, &db_type).unwrap();

            assert!(filter
                .do_tbs
                .contains(&RdbFilter::table_key(EMPTY_DB, "schema1", "tb1")));
            assert!(!filter.filter_tb("schema1", "tb1"));
            assert!(filter.filter_tb_with_db("another_db", "schema1", "tb1"));
        }
    }

    #[test]
    fn configured_schemas_use_empty_physical_db() {
        for db_type in [DbType::Mysql, DbType::Pg] {
            let config = FilterConfig {
                do_schemas: "schema1".to_string(),
                do_events: "*".to_string(),
                ..Default::default()
            };
            let filter = RdbFilter::from_config(&config, &db_type).unwrap();

            assert!(filter.do_schemas.contains("schema1"));
            assert!(!filter.filter_schema("schema1"));
            assert!(filter.filter_schema("another_db"));
        }
    }

    #[test]
    fn mssql_schema_config_keeps_single_token_parsing() {
        let tokens =
            RdbFilter::parse_single_tokens("db.name,[db.with.dot]", &DbType::Mssql).unwrap();

        assert_eq!(
            tokens,
            HashSet::from([
                "db".to_string(),
                "name".to_string(),
                "[db.with.dot]".to_string(),
            ])
        );
    }

    #[test]
    fn adding_table_rules_invalidates_cached_results() {
        let mut filter = RdbFilter::from_config(
            &FilterConfig {
                do_schemas: "*".to_string(),
                do_events: "*".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();

        assert!(!filter.filter_tb("dbo", "tb1"));
        filter.add_ignore_tb("dbo", "tb1");
        assert!(filter.filter_tb("dbo", "tb1"));

        let mut filter = RdbFilter::from_config(
            &FilterConfig {
                do_events: "*".to_string(),
                ..Default::default()
            },
            &DbType::Mssql,
        )
        .unwrap();

        assert!(filter.filter_tb("dbo", "tb1"));
        filter.add_do_tb("dbo", "tb1");
        assert!(!filter.filter_tb("dbo", "tb1"));
    }

    #[test]
    fn mssql_config_uses_physical_database_and_default_schema() {
        let config = FilterConfig {
            do_schemas: "db1,r#archive_.*#".to_string(),
            ignore_schemas: "db3".to_string(),
            do_tbs: "db1.*,db1.sales.orders,r#db[0-9]+#.r#schema.*#.r#table.*#".to_string(),
            ignore_tbs: "db2.orders".to_string(),
            do_events: "*".to_string(),
            ..Default::default()
        };

        let filter = RdbFilter::from_config(&config, &DbType::Mssql).unwrap();

        assert!(filter.do_schemas.contains("db1"));
        assert!(filter.do_schemas.contains("r#archive_.*#"));
        assert!(filter
            .do_tbs
            .contains(&RdbFilter::table_key("db1", "dbo", "*")));
        assert!(filter
            .do_tbs
            .contains(&RdbFilter::table_key("db1", "sales", "orders")));
        assert!(filter
            .ignore_tbs
            .contains(&RdbFilter::table_key("db2", "dbo", "orders")));
        assert!(!filter.filter_schema("db1"));
        assert!(!filter.filter_tb_with_db("db1", "dbo", "customers"));
        assert!(!filter.filter_tb_with_db("db1", "sales", "orders"));
        assert!(!filter.filter_tb_with_db("db4", "schema1", "table1"));
        assert!(filter.filter_tb_with_db("db2", "dbo", "orders"));
        assert!(filter.filter_tb_with_db("db3", "dbo", "orders"));
    }

    #[test]
    fn mssql_json_table_selectors_support_default_and_explicit_schema() {
        let ignore_cols = RdbFilter::parse_ignore_cols(
            r#"json:[{"db":"db1","tb":"orders","ignore_cols":["id"]},{"db":"db2","tb":"[sales].[orders]","ignore_cols":["name"]}]"#,
            &DbType::Mssql,
        )
        .unwrap();

        assert!(ignore_cols.contains_key(&RdbFilter::table_key("db1", "dbo", "orders")));
        assert!(ignore_cols.contains_key(&RdbFilter::table_key("db2", "sales", "orders")));
    }

    #[test]
    fn mssql_table_patterns_reject_invalid_arity() {
        for pattern in ["orders", "db.schema.table.extra"] {
            let result = RdbFilter::from_config(
                &FilterConfig {
                    do_tbs: pattern.to_string(),
                    ..Default::default()
                },
                &DbType::Mssql,
            );
            assert!(result.is_err(), "pattern should be rejected: {pattern}");
        }
    }
}
