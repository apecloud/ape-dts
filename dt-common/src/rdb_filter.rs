use std::collections::{HashMap, HashSet};

use crate::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, filter_config::FilterConfig,
    },
    error::Error,
    utils::sql_util::SqlUtil,
};

use regex::Regex;

#[derive(Debug, Clone)]
pub struct RdbFilter {
    pub db_type: DbType,
    pub do_dbs: HashSet<String>,
    pub ignore_dbs: HashSet<String>,
    pub do_tbs: HashSet<(String, String)>,
    pub ignore_tbs: HashSet<(String, String)>,
    pub do_events: HashSet<String>,
    pub do_structures: HashSet<String>,
    pub do_ddls: HashSet<String>,
    pub ignore_cmds: HashSet<String>,
    pub cache: HashMap<(String, String), bool>,
}

impl RdbFilter {
    pub fn from_config(config: &FilterConfig, db_type: &DbType) -> Result<Self, Error> {
        Ok(Self {
            db_type: db_type.to_owned(),
            do_dbs: Self::parse_single_tokens(&config.do_dbs, db_type)?,
            ignore_dbs: Self::parse_single_tokens(&config.ignore_dbs, db_type)?,
            do_tbs: Self::parse_pair_tokens(&config.do_tbs, db_type)?,
            ignore_tbs: Self::parse_pair_tokens(&config.ignore_tbs, db_type)?,
            do_events: Self::parse_single_tokens(&config.do_events, db_type)?,
            do_structures: Self::parse_single_tokens(&config.do_structures, db_type)?,
            do_ddls: Self::parse_single_tokens(&config.do_ddls, db_type)?,
            ignore_cmds: Self::parse_single_tokens(&config.ignore_cmds, db_type)?,
            cache: HashMap::new(),
        })
    }

    pub fn filter_db(&mut self, db: &str) -> bool {
        let tb = "*";
        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let filter = Self::contain_tb(&self.ignore_tbs, db, tb, &escape_pairs)
            || Self::contain_db(&self.ignore_dbs, db, &escape_pairs);

        if filter {
            return filter;
        }

        let do_tb_db: HashSet<String> = self.do_tbs.iter().map(|(d, _)| d.clone()).collect();
        let keep = Self::contain_db(&self.do_dbs, db, &escape_pairs)
            || Self::contain_db(&do_tb_db, db, &escape_pairs);
        !keep
    }

    pub fn filter_tb(&mut self, db: &str, tb: &str) -> bool {
        if let Some(cache) = self.cache.get(&(db.to_string(), tb.to_string())) {
            return *cache;
        }

        let escape_pairs = SqlUtil::get_escape_pairs(&self.db_type);
        let filter = Self::contain_tb(&self.ignore_tbs, db, tb, &escape_pairs)
            || Self::contain_db(&self.ignore_dbs, db, &escape_pairs);
        let keep = Self::contain_tb(&self.do_tbs, db, tb, &escape_pairs)
            || Self::contain_db(&self.do_dbs, db, &escape_pairs);

        let filter = filter || !keep;
        self.cache.insert((db.to_string(), tb.to_string()), filter);

        filter
    }

    pub fn filter_event(&mut self, db: &str, tb: &str, row_type: &str) -> bool {
        if !Self::match_all(&self.do_events) && !self.do_events.contains(row_type) {
            return true;
        }
        self.filter_tb(db, tb)
    }

    pub fn filter_all_ddl(&self) -> bool {
        self.do_ddls.is_empty()
    }

    pub fn filter_ddl(&mut self, db: &str, tb: &str, ddl_type: &str) -> bool {
        if !Self::match_all(&self.do_ddls) && !self.do_ddls.contains(ddl_type) {
            return true;
        }

        if tb.is_empty() {
            self.filter_db(db)
        } else {
            self.filter_tb(db, tb)
        }
    }

    pub fn filter_structure(&self, structure_type: &str) -> bool {
        !Self::match_all(&self.do_structures) && !self.do_structures.contains(structure_type)
    }

    pub fn filter_cmd(&self, cmd: &str) -> bool {
        self.ignore_cmds.contains(cmd)
    }

    fn match_all(set: &HashSet<String>) -> bool {
        set.len() == 1 && set.contains("*")
    }

    fn contain_tb(
        set: &HashSet<(String, String)>,
        db: &str,
        tb: &str,
        escape_pairs: &[(char, char)],
    ) -> bool {
        for i in set.iter() {
            if Self::match_token(&i.0, db, escape_pairs)
                && Self::match_token(&i.1, tb, escape_pairs)
            {
                return true;
            }
        }
        false
    }

    fn contain_db(set: &HashSet<String>, item: &str, escape_pairs: &[(char, char)]) -> bool {
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
        // * : matching mutiple chars
        // ? : for matching 0-1 chars
        let mut pattern = pattern
            .replace('.', "\\.")
            .replace('*', ".*")
            .replace('?', ".?");
        pattern = format!(r"^{}$", pattern);
        if Regex::new(&pattern).unwrap().is_match(item) {
            return true;
        }
        false
    }

    fn parse_pair_tokens(
        config_str: &str,
        db_type: &DbType,
    ) -> Result<HashSet<(String, String)>, Error> {
        let mut results = HashSet::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            results.insert((tokens[i].to_string(), tokens[i + 1].to_string()));
            i += 2;
        }
        Ok(results)
    }

    fn parse_single_tokens(config_str: &str, db_type: &DbType) -> Result<HashSet<String>, Error> {
        let tokens = Self::parse_config(config_str, db_type)?;
        let results: HashSet<String> = HashSet::from_iter(tokens.into_iter());
        Ok(results)
    }

    fn parse_config(config_str: &str, db_type: &DbType) -> Result<Vec<String>, Error> {
        let delimiters = vec![',', '.'];
        match ConfigTokenParser::parse_config(config_str, db_type, &delimiters) {
            Ok(tokens) => Ok(tokens),
            Err(Error::ConfigError(err)) => Err(Error::ConfigError(format!(
                "invalid filter config, {}",
                err
            ))),
            _ => Err(Error::ConfigError("invalid filter config".into())),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

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
            do_dbs: "*".to_string(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: "*.b*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("b", "b", "insert"));
        assert!(!rdb_fitler.filter_event("a", "cbd", "insert"));
        assert!(!rdb_fitler.filter_event("b", "cbd", "insert"));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_dbs: "*".to_string(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: "*.`b*`,*.c*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("a", "b*", "insert"));
        assert!(rdb_fitler.filter_event("b", "b*", "insert"));
        assert!(!rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("b", "b", "insert"));
        assert!(rdb_fitler.filter_event("a", "cbd", "insert"));
        assert!(rdb_fitler.filter_event("b", "cbd", "insert"));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs_with_escapes_2() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_tbs: "`db_test_position.aaa`.`b.bbb,.b`,`db_test_position.aaa`.c".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_event("db_test_position.aaa", "b.bbb,.b", "insert"));
        assert!(!rdb_fitler.filter_event("db_test_position.aaa", "c", "insert"));
    }

    #[test]
    fn test_rdb_filter_ignore_dbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_dbs: "*".to_string(),
            ignore_dbs: "a*".to_string(),
            do_tbs: "*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("abc", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("b", "cbd", "insert"));
        assert!(!rdb_fitler.filter_event("b", "cbd", "insert"));
    }

    #[test]
    fn test_rdb_filter_ignore_dbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_dbs: "*".to_string(),
            ignore_dbs: "`a*`,b*".to_string(),
            do_tbs: "*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_event("abc", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("bcd", "cbd", "insert"));
        assert!(rdb_fitler.filter_event("b", "cbd", "insert"));
    }

    #[test]
    fn test_rdb_filter_do_dbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_dbs: "b*".to_string(),
            ignore_dbs: "a*".to_string(),
            do_tbs: "aaaaaaa.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("c", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("b", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("bcd", "bcd", "insert"));
    }

    #[test]
    fn test_rdb_filter_do_dbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            do_dbs: "`b*`,abc,bcd*,cde".to_string(),
            ignore_dbs: "a*".to_string(),
            do_tbs: "aaaaaaa.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("c", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("b", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("bc", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("abc", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("b*", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("bcd", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("bcde", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("cde", "bcd", "insert"));
    }

    #[test]
    fn test_rdb_filter_do_tbs_without_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            ignore_dbs: "b*".to_string(),
            do_tbs: "a*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("bcd", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("cde", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("abc", "bcd", "insert"));
    }

    #[test]
    fn test_rdb_filter_do_tbs_with_escapes() {
        let db_type = DbType::Mysql;
        let config = FilterConfig {
            ignore_dbs: "b*".to_string(),
            do_tbs: "a*.*,`c*`.`*`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_event("bcd", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("cde", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("a", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("abc", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("c", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("cde", "bcd", "insert"));
        assert!(rdb_fitler.filter_event("c*", "bcd", "insert"));
        assert!(!rdb_fitler.filter_event("c*", "*", "insert"));
    }

    #[test]
    fn test_rdb_filter_db_without_escapes() {
        let db_type = DbType::Mysql;
        // keep by do_dbs, Not filtered by ignore_dbs
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_2".to_string(),

            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_1".to_string(),

            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            do_dbs: "test_db_1".to_string(),
            ignore_dbs: "*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_tbs: "test_db_1.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_tbs: "test_db_1.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            ignore_dbs: "b*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, NOT filtered by ignore_dbs
        let config = FilterConfig {
            ignore_dbs: "test_db_2".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            ignore_dbs: "test_db_1".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            ignore_dbs: "test_db_*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));
    }

    #[test]
    fn test_rdb_filter_db_with_esacpes() {
        let db_type = DbType::Mysql;
        // keep by do_dbs, Not filtered by ignore_dbs
        let config = FilterConfig {
            do_dbs: "`test_db_*`".to_string(),
            ignore_dbs: "`test_db_2`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_*"));

        // keep by do_dbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "`test_db_*`".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // keep by do_dbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            do_dbs: "`test_db_*`".to_string(),
            ignore_dbs: "test_db*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // keep by do_dbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_dbs: "`test_db_*`".to_string(),
            ignore_tbs: "`test_db_*`.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_*"));

        // keep by do_dbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_dbs: "`test_db_*`".to_string(),
            ignore_tbs: "`test_db_*`.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // keep by do_tbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig {
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            ignore_tbs: "`test_db_*`.a*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_*"));

        // keep by do_tbs, all tables filtered by ignore_tbs
        let config = FilterConfig {
            ignore_dbs: "b*".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            ignore_tbs: "`test_db_*`.*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // keep by do_tbs, NOT filtered by ignore_dbs
        let config = FilterConfig {
            ignore_dbs: "test_db_2".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_*"));

        // keep by do_tbs, filtered by ignore_dbs exactly
        let config = FilterConfig {
            ignore_dbs: "`test_db_*`".to_string(),
            do_tbs: "test_db_*.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // keep by do_tbs, filtered by ignore_dbs wildchar
        let config = FilterConfig {
            ignore_dbs: "test_db*".to_string(),
            do_tbs: "`test_db_*`.one_pk_multi_uk".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));

        // ingore some tbs in db, but not all tbs in db filtered
        let config = FilterConfig {
            ignore_tbs: "test_db_*.test_tb_*".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(rdb_fitler.filter_db("test_db_*"));
    }

    #[test]
    fn test_rdb_filter_event() {
        let db_type = DbType::Mysql;

        // keep do_events emtpy
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_2".to_string(),
            do_events: "*".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_event("test_db_1", "aaaa", "insert"));
        assert!(!rdb_fitler.filter_event("test_db_1", "aaaa", "update"));
        assert!(!rdb_fitler.filter_event("test_db_1", "aaaa", "delete"));

        // explicitly set do_events
        let config = FilterConfig {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_2".to_string(),
            do_events: "insert".to_string(),
            ..Default::default()
        };
        let mut rdb_fitler = RdbFilter::from_config(&config, &db_type).unwrap();
        assert!(!rdb_fitler.filter_event("test_db_1", "aaaa", "insert"));
        assert!(rdb_fitler.filter_event("test_db_1", "aaaa", "update"));
        assert!(rdb_fitler.filter_event("test_db_1", "aaaa", "delete"));
    }
}