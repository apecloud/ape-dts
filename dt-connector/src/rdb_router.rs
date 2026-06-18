use anyhow::{bail, Context, Ok};
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, router_config::RouterConfig,
    },
    meta::{
        ddl_meta::{ddl_data::DdlData, ddl_statement::DdlStatement},
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    utils::sql_util::SqlUtil,
};
use std::collections::HashMap;

use dt_common::meta::{col_value::ColValue, row_data::RowData};
use serde::{Deserialize, Serialize};

type SchemaMap = HashMap<String, String>;
type TbMap = HashMap<(String, String), (String, String)>;
type TbColMap = HashMap<(String, String), HashMap<String, String>>;

const JSON_PREFIX: &str = "json:";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RdbRouter {
    forward: RdbRouterInner,
    reverse: RdbRouterInner,
    topic: RdbTopicRouterInner,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RdbRouterInner {
    // HashMap<src_schema, dst_schema>
    schema_map: SchemaMap,
    // HashMap<(src_schema, src_tb), (dst_schema, dst_tb)>
    tb_map: TbMap,
    // HashMap<(src_schema, src_tb), HashMap<src_col, dst_col>>
    col_map: TbColMap,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RdbTopicRouterInner {
    // HashMap<(src_schema, src_tb), String>
    topic_map: HashMap<(String, String), String>,
}

impl RdbRouter {
    pub fn from_config(config: &RouterConfig, db_type: &DbType) -> anyhow::Result<Option<Self>> {
        let router = Self::from_config_for_topic(config, db_type)?;
        if router.has_route_rules() {
            Ok(Some(router))
        } else {
            Ok(None)
        }
    }

    pub fn from_config_for_topic(config: &RouterConfig, db_type: &DbType) -> anyhow::Result<Self> {
        let inner = RdbRouterInner::from_config(config, db_type)?;
        let topic = RdbTopicRouterInner::from_config(config, db_type)?;
        let reverse = inner.reverse();

        Ok(Self {
            forward: inner,
            reverse,
            topic,
        })
    }

    pub fn has_route_rules(&self) -> bool {
        self.forward.has_route_rules()
    }

    pub fn get_schema_map<'a>(&'a self, schema: &'a str) -> &'a str {
        self.forward.get_schema_map(schema)
    }

    pub fn reverse_get_schema_map<'a>(&'a self, schema: &'a str) -> &'a str {
        self.reverse.get_schema_map(schema)
    }

    pub fn get_tb_map<'a>(&'a self, schema: &'a str, tb: &'a str) -> (&'a str, &'a str) {
        self.forward.get_tb_map(schema, tb)
    }

    pub fn reverse_get_tb_map<'a>(&'a self, schema: &'a str, tb: &'a str) -> (&'a str, &'a str) {
        self.reverse.get_tb_map(schema, tb)
    }

    pub fn get_col_map(&self, schema: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.forward.get_col_map(schema, tb)
    }

    pub fn reverse_get_col_map(&self, schema: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.reverse.get_col_map(schema, tb)
    }

    pub fn get_topic<'a>(&'a self, schema: &str, tb: &str) -> &'a str {
        self.topic.get_topic(schema, tb)
    }

    pub fn route_row(&self, row_data: RowData) -> RowData {
        self.forward.route_row(row_data)
    }

    pub fn reverse_route_row(&self, row_data: RowData) -> RowData {
        self.reverse.route_row(row_data)
    }

    pub fn route_ddl(&self, ddl_data: DdlData) -> DdlData {
        self.forward.route_ddl(ddl_data)
    }

    pub fn reverse_route_ddl(&self, ddl_data: DdlData) -> DdlData {
        self.reverse.route_ddl(ddl_data)
    }

    pub fn route_struct(&self, struct_data: StructData) -> StructData {
        self.forward.route_struct(struct_data)
    }

    pub fn reverse_route_struct(&self, struct_data: StructData) -> StructData {
        self.reverse.route_struct(struct_data)
    }

    pub fn route_redis_db_id(&self, db_id: i64) -> anyhow::Result<i64> {
        self.forward.route_redis_db_id(db_id)
    }

    pub fn validate_redis_db_map(&self, is_cluster: bool) -> anyhow::Result<()> {
        self.forward.validate_redis_db_map()?;
        if is_cluster {
            return self.forward.validate_redis_target_cluster_db_map();
        }
        Ok(())
    }

    #[cfg(test)]
    fn parse_schema_map(config_str: &str, db_type: &DbType) -> anyhow::Result<SchemaMap> {
        RdbRouterInner::parse_schema_map(config_str, db_type)
    }

    #[cfg(test)]
    fn parse_tb_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TbMap> {
        RdbRouterInner::parse_tb_map(config_str, db_type)
    }

    #[cfg(test)]
    fn parse_col_map(config_str: &str) -> anyhow::Result<TbColMap> {
        RdbRouterInner::parse_col_map(config_str)
    }

    #[cfg(test)]
    pub(crate) fn from_maps_for_test(
        schema_map: SchemaMap,
        tb_map: TbMap,
        col_map: TbColMap,
        topic_map: HashMap<(String, String), String>,
    ) -> Self {
        let inner = RdbRouterInner {
            schema_map,
            tb_map,
            col_map,
        };
        let reverse = inner.reverse();
        Self {
            forward: inner,
            reverse,
            topic: RdbTopicRouterInner { topic_map },
        }
    }
}

impl RdbRouterInner {
    fn from_config(config: &RouterConfig, db_type: &DbType) -> anyhow::Result<Self> {
        match config {
            RouterConfig::Rdb {
                schema_map,
                tb_map,
                col_map,
                ..
            } => {
                let schema_map = Self::parse_schema_map(schema_map, db_type)?;
                let tb_map = Self::parse_tb_map(tb_map, db_type)?;
                let col_map = Self::parse_col_map(col_map)?;
                Ok(Self {
                    schema_map,
                    tb_map,
                    col_map,
                })
            }
        }
    }

    fn has_route_rules(&self) -> bool {
        !self.schema_map.is_empty() || !self.tb_map.is_empty() || !self.col_map.is_empty()
    }

    fn get_schema_map<'a>(&'a self, schema: &'a str) -> &'a str {
        if let Some(dst_schema) = self.schema_map.get(schema) {
            return dst_schema;
        }
        schema
    }

    fn get_tb_map<'a>(&'a self, schema: &'a str, tb: &'a str) -> (&'a str, &'a str) {
        if let Some((dst_schema, dst_tb)) = self.tb_map.get(&(schema.into(), tb.into())) {
            return (dst_schema, dst_tb);
        }
        if let Some(dst_schema) = self.schema_map.get(schema) {
            return (dst_schema, tb);
        }
        (schema, tb)
    }

    fn get_col_map(&self, schema: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.col_map.get(&(schema.into(), tb.into()))
    }

    fn reverse(&self) -> Self {
        let mut reverse_schema_map = HashMap::new();
        let mut reverse_tb_map = HashMap::new();
        let mut reverse_tb_col_map = HashMap::new();

        for (src_schema_tb, col_map) in self.col_map.iter() {
            let mut reverse_col_map = HashMap::new();
            for (src_col, dst_col) in col_map.iter() {
                reverse_col_map.insert(dst_col.into(), src_col.into());
            }
            let dst_tb = self.tb_map.get(src_schema_tb).unwrap();
            reverse_tb_col_map.insert(dst_tb.clone(), reverse_col_map);
        }

        for (src_tb, dst_tb) in self.tb_map.iter() {
            reverse_tb_map.insert(dst_tb.to_owned(), src_tb.to_owned());
        }

        for (src_schema, dst_db) in self.schema_map.iter() {
            reverse_schema_map.insert(dst_db.to_owned(), src_schema.to_owned());
        }

        Self {
            schema_map: reverse_schema_map,
            tb_map: reverse_tb_map,
            col_map: reverse_tb_col_map,
        }
    }

    fn route_row(&self, mut row_data: RowData) -> RowData {
        // tb map
        let (schema, tb) = (row_data.schema.clone(), row_data.tb.clone());
        let (dst_schema, dst_tb) = self.get_tb_map(&schema, &tb);
        row_data.schema = dst_schema.to_string();
        row_data.tb = dst_tb.to_string();

        // col map
        let col_map = self.get_col_map(&schema, &tb);
        if col_map.is_none() {
            return row_data;
        }
        let col_map = col_map.unwrap();

        let route_col_values =
            |col_values: HashMap<String, ColValue>| -> HashMap<String, ColValue> {
                col_values
                    .into_iter()
                    .map(|(col, val)| {
                        if let Some(dst_col) = col_map.get(&col) {
                            (dst_col.clone(), val)
                        } else {
                            (col, val)
                        }
                    })
                    .collect()
            };

        if let Some(before) = row_data.before {
            row_data.before = Some(route_col_values(before));
        }

        if let Some(after) = row_data.after {
            row_data.after = Some(route_col_values(after));
        }

        row_data
    }

    fn route_ddl(&self, mut ddl_data: DdlData) -> DdlData {
        let has_rename_target = !ddl_data.get_rename_to_schema_tb().1.is_empty();
        match &mut ddl_data.statement {
            DdlStatement::MysqlAlterTableRename(_)
            | DdlStatement::PgAlterTableRename(_)
            | DdlStatement::RenameTable(_)
            | DdlStatement::MongoCommand(_)
                if has_rename_target =>
            {
                let (src_schema, src_tb) = ddl_data.get_schema_tb();
                let (src_new_schema, src_new_tb) = ddl_data.get_rename_to_schema_tb();
                let (dst_schema, dst_tb) = self.get_tb_map(&src_schema, &src_tb);
                let (dst_new_schema, dst_new_tb) = self.get_tb_map(&src_new_schema, &src_new_tb);
                ddl_data.statement.route_rename_table(
                    dst_schema.into(),
                    dst_tb.into(),
                    dst_new_schema.into(),
                    dst_new_tb.into(),
                );
            }

            _ => {
                let (src_schema, src_tb) = ddl_data.get_schema_tb();
                let (dst_schema, dst_tb) = self.get_tb_map(&src_schema, &src_tb);
                ddl_data.statement.route(dst_schema.into(), dst_tb.into());
            }
        }

        let dst_default_schema = self.get_schema_map(&ddl_data.default_schema);
        ddl_data.default_schema = dst_default_schema.into();
        ddl_data
    }

    fn route_struct(&self, mut struct_data: StructData) -> StructData {
        match &mut struct_data.statement {
            StructStatement::MysqlCreateTable(s) => {
                let (schema, tb) = (s.table.database_name.clone(), s.table.table_name.clone());
                let (dst_schema, dst_tb) = self.get_tb_map(&schema, &tb);
                s.route(dst_schema, dst_tb)
            }

            StructStatement::MysqlCreateDatabase(s) => {
                let dst_schema = self.get_schema_map(&s.database.name).to_string();
                s.route(&dst_schema)
            }

            StructStatement::MongoCreateCollection(s) => {
                let (schema, tb) = (s.database_name.clone(), s.collection_name.clone());
                let (dst_schema, dst_tb) = self.get_tb_map(&schema, &tb);
                s.route(dst_schema, dst_tb)
            }

            StructStatement::MongoShardKey(s) => {
                let ns = s.shard_collection.ns.clone();
                if let Some((schema, tb)) = ns.split_once('.') {
                    let (dst_schema, dst_tb) = self.get_tb_map(schema, tb);
                    s.route(schema, tb, dst_schema, dst_tb)
                }
            }

            StructStatement::PgCreateTable(s) => {
                let (schema, tb) = (s.table.schema_name.clone(), s.table.table_name.clone());
                let (dst_schema, dst_tb) = self.get_tb_map(&schema, &tb);
                s.route(dst_schema, dst_tb)
            }

            StructStatement::PgCreateSchema(s) => {
                let dst_schema = self.get_schema_map(&s.schema.name).to_string();
                s.route(&dst_schema)
            }

            _ => {}
        }

        struct_data
    }

    fn route_redis_db_id(&self, db_id: i64) -> anyhow::Result<i64> {
        let src_db = db_id.to_string();
        let dst_db = self.get_schema_map(&src_db);
        dst_db.parse::<i64>().with_context(|| {
            format!(
                "invalid Redis db mapping target. src_db=[{}], dst_db=[{}]",
                src_db, dst_db
            )
        })
    }

    fn validate_redis_db_map(&self) -> anyhow::Result<()> {
        for (src_db, dst_db) in self.schema_map.iter() {
            src_db
                .parse::<i64>()
                .with_context(|| format!("invalid Redis db mapping source: {}", src_db))?;
            dst_db
                .parse::<i64>()
                .with_context(|| format!("invalid Redis db mapping target: {}", dst_db))?;
        }
        Ok(())
    }

    fn validate_redis_target_cluster_db_map(&self) -> anyhow::Result<()> {
        for (src_db, dst_db) in self.schema_map.iter() {
            let dst_db_id = dst_db
                .parse::<i64>()
                .with_context(|| format!("invalid Redis db mapping target: {}", dst_db))?;
            if dst_db_id != 0 {
                bail!(
                    "Redis Cluster target only supports db 0, invalid db_map: {}:{}",
                    src_db,
                    dst_db
                );
            }
        }
        Ok(())
    }

    fn parse_schema_map(config_str: &str, db_type: &DbType) -> anyhow::Result<SchemaMap> {
        // db_map=src_db_1:dst_db_1,src_db_2:dst_db_2
        let mut schema_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            schema_map.insert(tokens[i].to_string(), tokens[i + 1].to_string());
            i += 2;
        }
        Ok(schema_map)
    }

    fn parse_tb_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TbMap> {
        // tb_map=src_db_1.src_tb_1:dst_db_1.dst_tb_1,src_db_2.src_tb_2:dst_db_2.dst_tb_2
        let mut tb_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            tb_map.insert(
                (tokens[i].to_string(), tokens[i + 1].to_string()),
                (tokens[i + 2].to_string(), tokens[i + 3].to_string()),
            );
            i += 4;
        }
        Ok(tb_map)
    }

    fn parse_col_map(config_str: &str) -> anyhow::Result<TbColMap> {
        let mut results = TbColMap::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }

        #[derive(Serialize, Deserialize)]
        struct TbColMapType {
            db: String,
            tb: String,
            col_map: HashMap<String, String>,
        }
        // col_map=json:[{"db":"test_db","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}]
        let config: Vec<TbColMapType> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX))?;
        for i in config {
            results.insert((i.db, i.tb), i.col_map);
        }
        Ok(results)
    }

    fn parse_config(config_str: &str, db_type: &DbType) -> anyhow::Result<Vec<String>> {
        let delimiters = vec![',', '.', ':'];
        let tokens = ConfigTokenParser::parse_config(config_str, db_type, &delimiters, None)?;
        let escape_pairs = SqlUtil::get_escape_pairs(db_type);
        let mut results = Vec::new();
        for t in tokens {
            let mut token = t;
            for escape_pair in escape_pairs.iter() {
                token = SqlUtil::unescape(&token, escape_pair);
            }
            results.push(token);
        }
        Ok(results)
    }
}

impl RdbTopicRouterInner {
    fn from_config(config: &RouterConfig, db_type: &DbType) -> anyhow::Result<Self> {
        match config {
            RouterConfig::Rdb { topic_map, .. } => Ok(Self {
                topic_map: Self::parse_topic_map(topic_map, db_type)?,
            }),
        }
    }

    fn get_topic<'a>(&'a self, schema: &str, tb: &str) -> &'a str {
        // *.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        if let Some(topic) = self.topic_map.get(&(schema.into(), tb.into())) {
            return topic;
        }
        if let Some(topic) = self.topic_map.get(&(schema.into(), "*".into())) {
            return topic;
        }
        // should always has a default topic map
        self.topic_map.get(&("*".into(), "*".into())).unwrap()
    }

    fn parse_topic_map(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<HashMap<(String, String), String>> {
        // topic_map=*.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        let mut topic_map = HashMap::new();
        let tokens = RdbRouterInner::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            topic_map.insert(
                (tokens[i].to_string(), tokens[i + 1].to_string()),
                tokens[i + 2].to_string(),
            );
            i += 3;
        }
        Ok(topic_map)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dt_common::config::{config_enums::DbType, router_config::RouterConfig};

    use super::{RdbRouter, TbColMap, TbMap};

    #[test]
    fn test_parse_ignore_cols() {
        let config_str =
            r#"json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}]"#;
        let col_map = RdbRouter::parse_col_map(config_str).unwrap();
        let tb_1 = col_map
            .get(&("db_1".to_string(), "tb_1".to_string()))
            .unwrap();
        assert_eq!(tb_1.len(), 2);
        assert_eq!(*tb_1.get("f_0").unwrap(), "dst_f_0".to_string());
        assert_eq!(*tb_1.get("f_1").unwrap(), "dst_f_1".to_string());
    }

    #[test]
    fn test_parse_schema_map() {
        // mysql
        let config_str = "src_1:dst_1,`src,2'`:dst_2,`src:3,`:`dst:3,`";
        let db_map = RdbRouter::parse_schema_map(config_str, &DbType::Mysql).unwrap();
        assert_eq!(db_map.get("src_1").unwrap(), "dst_1");
        assert_eq!(db_map.get("src,2'").unwrap(), "dst_2");
        assert_eq!(db_map.get("src:3,").unwrap(), "dst:3,");
        assert_eq!(db_map.get("src_4"), None);

        // pg
        let config_str = r#"src_1:dst_1,"src,2'":dst_2,"src:3,":"dst:3,""#;
        let db_map = RdbRouter::parse_schema_map(config_str, &DbType::Pg).unwrap();
        assert_eq!(db_map.get("src_1").unwrap(), "dst_1");
        assert_eq!(db_map.get("src,2'").unwrap(), "dst_2");
        assert_eq!(db_map.get("src:3,").unwrap(), "dst:3,");
        assert_eq!(db_map.get("src_4"), None);
    }

    #[test]
    fn test_parse_tb_map() {
        let assert_exists =
            |tb_map: &TbMap, src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
                assert_eq!(
                    tb_map.get(&(src_db.into(), src_tb.into())).unwrap(),
                    &(dst_db.into(), dst_tb.into())
                )
            };

        // mysql
        let config_str = "src_db_1.src_tb_1:dst_db_1.dst_tb_1,".to_string()
            + "`src_db,2'`.`src_tb,2'`:dst_db_2.dst_tb_2,"
            + "`src_db:3,`.`src_tb:3,`:`dst_db:3,`.`dst_tb:3,`";
        let tb_map = RdbRouter::parse_tb_map(&config_str, &DbType::Mysql).unwrap();

        assert_exists(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
        assert_exists(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_exists(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
        assert_eq!(tb_map.get(&("src_db_4".into(), "src_tb_4".into())), None);

        // pg
        let config_str = r#"src_db_1.src_tb_1:dst_db_1.dst_tb_1,"#.to_string()
            + r#""src_db,2'"."src_tb,2'":dst_db_2.dst_tb_2,"#
            + r#""src_db:3,"."src_tb:3,":"dst_db:3,"."dst_tb:3,""#;
        let tb_map = RdbRouter::parse_tb_map(&config_str, &DbType::Pg).unwrap();

        assert_exists(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
        assert_exists(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_exists(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
        assert_eq!(tb_map.get(&("src_db_4".into(), "src_tb_4".into())), None);
    }

    #[test]
    fn test_parse_tb_col_map() {
        let assert_col_map =
            |tb_map: &TbColMap, src_db: &str, src_tb: &str, col_map: &HashMap<String, String>| {
                assert_eq!(
                    tb_map.get(&(src_db.into(), src_tb.into())).unwrap(),
                    col_map
                )
            };

        let check_results = |tb_col_map: &TbColMap| {
            let mut col_map = HashMap::new();
            col_map.insert("src_col_1".to_string(), "dst_col_1".to_string());
            col_map.insert("src_col_2".to_string(), "dst_col_2".to_string());
            assert_col_map(tb_col_map, "src_db_1", "src_tb_1", &col_map);

            let mut col_map = HashMap::new();
            col_map.insert("src_col,1'".to_string(), "dst_col_1".to_string());
            col_map.insert("src_col,2'".to_string(), "dst_col_2".to_string());
            assert_col_map(tb_col_map, "src_db,2'", "src_tb,2'", &col_map);

            let mut col_map = HashMap::new();
            col_map.insert("src_col:1,".to_string(), "dst_col:1,".to_string());
            col_map.insert("src_col:2,".to_string(), "dst_col:2,".to_string());
            assert_col_map(tb_col_map, "src_db:3,", "src_tb:3,", &col_map);

            assert_eq!(
                tb_col_map.get(&("src_db_4".into(), "src_tb_4".into())),
                None
            );
        };

        // mysql
        let config_str = r#"[{"db":"src_db_1","tb":"src_tb_1","col_map":{"src_col_1":"dst_col_1","src_col_2":"dst_col_2"}},"#.to_string()
            + r#"{"db":"src_db,2'","tb":"src_tb,2'","col_map":{"src_col,1'":"dst_col_1","src_col,2'":"dst_col_2"}},"#
            + r#"{"db":"src_db:3,","tb":"src_tb:3,","col_map":{"src_col:1,":"dst_col:1,","src_col:2,":"dst_col:2,"}}]"#;
        let tb_col_map = RdbRouter::parse_col_map(&config_str).unwrap();
        check_results(&tb_col_map);
    }

    #[test]
    fn test_parse_config() {
        let db_map_str = "src_1:dst_1";
        let tb_map_str = "`src_db,2'`.`src_tb,2'`:dst_db_2.dst_tb_2,`src_db:3,`.`src_tb:3,`:`dst_db:3,`.`dst_tb:3,`";
        let col_map_str = r#"[{"db":"src_db:3,","tb":"src_tb:3,","col_map":{"src_col:1,":"dst_col:1,","src_col:2,":"dst_col:2,"}}]"#;
        let topic_map = "*.*:test,`db:1`.*:test2,`db:1`.`tb:1`:test3";

        let config = RouterConfig::Rdb {
            schema_map: db_map_str.into(),
            tb_map: tb_map_str.into(),
            col_map: col_map_str.into(),
            topic_map: topic_map.into(),
        };
        let router = RdbRouter::from_config(&config, &DbType::Mysql)
            .unwrap()
            .unwrap();

        let assert_tb_map = |src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
            assert_eq!(router.get_tb_map(src_db, src_tb), (dst_db, dst_tb));
        };
        let assert_col_map = |src_db: &str, src_tb: &str, col_map: &HashMap<String, String>| {
            assert_eq!(router.get_col_map(src_db, src_tb).unwrap(), col_map)
        };

        // db_map
        assert_tb_map("src_1", "aaa.1,:1", "dst_1", "aaa.1,:1");
        assert_tb_map("src_4", "aaa.1,:1", "src_4", "aaa.1,:1");
        // tb_map
        assert_tb_map("src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_tb_map("src_db,2'", "src_tb,3'", "src_db,2'", "src_tb,3'");
        assert_eq!(
            router.reverse_get_tb_map("dst_db_2", "dst_tb_2"),
            ("src_db,2'", "src_tb,2'")
        );
        // col_map
        let mut col_map = HashMap::new();
        col_map.insert("src_col:1,".to_string(), "dst_col:1,".to_string());
        col_map.insert("src_col:2,".to_string(), "dst_col:2,".to_string());
        assert_col_map("src_db:3,", "src_tb:3,", &col_map);
        let reverse_col_map = router
            .reverse_get_col_map("dst_db:3,", "dst_tb:3,")
            .unwrap();
        assert_eq!(reverse_col_map.get("dst_col:1,").unwrap(), "src_col:1,");
        assert_eq!(reverse_col_map.get("dst_col:2,").unwrap(), "src_col:2,");
        // topic_map
        assert_eq!(router.get_topic("db:1", "tb:1"), "test3");
        assert_eq!(router.get_topic("db:1", "tb:2"), "test2");
        assert_eq!(router.get_topic("db:2", "tb:1"), "test");
    }

    #[test]
    fn test_topic_only_router_does_not_enable_table_route() {
        let config = RouterConfig::Rdb {
            schema_map: String::new(),
            tb_map: String::new(),
            col_map: String::new(),
            topic_map: "*.*:test".into(),
        };
        let router = RdbRouter::from_config(&config, &DbType::Mysql).unwrap();

        assert!(router.is_none());
        let topic_router = RdbRouter::from_config_for_topic(&config, &DbType::Mysql).unwrap();
        assert_eq!(
            topic_router.get_tb_map("src_db", "src_tb"),
            ("src_db", "src_tb")
        );
        assert_eq!(
            topic_router.reverse_get_tb_map("dst_db", "dst_tb"),
            ("dst_db", "dst_tb")
        );
        assert_eq!(topic_router.get_col_map("src_db", "src_tb"), None);
        assert_eq!(topic_router.reverse_get_col_map("dst_db", "dst_tb"), None);
        assert_eq!(topic_router.get_topic("src_db", "src_tb"), "test");
    }

    #[test]
    fn test_redis_db_map() {
        let db_map = RdbRouter::parse_schema_map("0:1,2:3", &DbType::Redis).unwrap();
        let router =
            RdbRouter::from_maps_for_test(db_map, HashMap::new(), HashMap::new(), HashMap::new());

        router.validate_redis_db_map(false).unwrap();
        assert_eq!(router.route_redis_db_id(0).unwrap(), 1);
        assert_eq!(router.route_redis_db_id(2).unwrap(), 3);
        assert_eq!(router.route_redis_db_id(4).unwrap(), 4);
    }

    #[test]
    fn test_redis_db_map_validation() {
        let db_map = RdbRouter::parse_schema_map("0:abc", &DbType::Redis).unwrap();
        let router =
            RdbRouter::from_maps_for_test(db_map, HashMap::new(), HashMap::new(), HashMap::new());
        assert!(router.validate_redis_db_map(false).is_err());

        let db_map = RdbRouter::parse_schema_map("0:1", &DbType::Redis).unwrap();
        let router =
            RdbRouter::from_maps_for_test(db_map, HashMap::new(), HashMap::new(), HashMap::new());
        assert!(router.validate_redis_db_map(true).is_err());

        let db_map = RdbRouter::parse_schema_map("0:0", &DbType::Redis).unwrap();
        let router =
            RdbRouter::from_maps_for_test(db_map, HashMap::new(), HashMap::new(), HashMap::new());
        router.validate_redis_db_map(true).unwrap();
    }
}
