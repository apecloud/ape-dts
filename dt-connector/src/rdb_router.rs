use anyhow::{bail, Context, Ok};
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser, router_config::RouterConfig,
    },
    error::DtError,
    meta::{
        ddl_meta::{ddl_data::DdlData, ddl_statement::DdlStatement},
        mssql::MSSQL_DEFAULT_SCHEMA,
        struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    },
    utils::sql_util::SqlUtil,
};
use std::collections::HashMap;

use dt_common::meta::{col_value::ColValue, row_data::RowData};
use serde::{Deserialize, Serialize};

type SchemaMap = HashMap<String, String>;
type DbSchemaTb = (String, String, String);
type TableMap = HashMap<DbSchemaTb, DbSchemaTb>;
type TableColMap = HashMap<DbSchemaTb, HashMap<String, String>>;

const JSON_PREFIX: &str = "json:";
const EMPTY_DB: &str = "";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RdbRouter {
    forward: RdbRouterInner,
    reverse: RdbRouterInner,
    topic: RdbTopicRouterInner,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RdbRouterInner {
    // HashMap<src_schema, dst_schema>. For MSSQL, schema means physical database here.
    schema_map: SchemaMap,
    // HashMap<(src_db, src_schema, src_tb), (dst_db, dst_schema, dst_tb)>
    tb_map: TableMap,
    // HashMap<(src_db, src_schema, src_tb), HashMap<src_col, dst_col>>
    col_map: TableColMap,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RdbTopicRouterInner {
    // HashMap<(src_db, src_schema, src_tb), String>
    topic_map: HashMap<DbSchemaTb, String>,
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
        let (_, dst_schema, dst_tb) = self.forward.get_tb_map_with_db(EMPTY_DB, schema, tb);
        (dst_schema, dst_tb)
    }

    pub fn get_tb_map_with_db<'a>(
        &'a self,
        db: &'a str,
        schema: &'a str,
        tb: &'a str,
    ) -> (&'a str, &'a str, &'a str) {
        self.forward.get_tb_map_with_db(db, schema, tb)
    }

    pub fn reverse_get_tb_map<'a>(&'a self, schema: &'a str, tb: &'a str) -> (&'a str, &'a str) {
        let (_, src_schema, src_tb) = self.reverse.get_tb_map_with_db(EMPTY_DB, schema, tb);
        (src_schema, src_tb)
    }

    pub fn reverse_get_tb_map_with_db<'a>(
        &'a self,
        db: &'a str,
        schema: &'a str,
        tb: &'a str,
    ) -> (&'a str, &'a str, &'a str) {
        self.reverse.get_tb_map_with_db(db, schema, tb)
    }

    pub fn get_col_map(&self, schema: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.forward.get_col_map_with_db(EMPTY_DB, schema, tb)
    }

    pub fn get_col_map_with_db(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> Option<&HashMap<String, String>> {
        self.forward.get_col_map_with_db(db, schema, tb)
    }

    pub fn reverse_get_col_map(&self, schema: &str, tb: &str) -> Option<&HashMap<String, String>> {
        self.reverse.get_col_map_with_db(EMPTY_DB, schema, tb)
    }

    pub fn reverse_get_col_map_with_db(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> Option<&HashMap<String, String>> {
        self.reverse.get_col_map_with_db(db, schema, tb)
    }

    pub fn get_topic<'a>(&'a self, schema: &str, tb: &str) -> &'a str {
        self.topic.get_topic(EMPTY_DB, schema, tb)
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
    fn parse_tb_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TableMap> {
        RdbRouterInner::parse_tb_map(config_str, db_type)
    }

    #[cfg(test)]
    fn parse_col_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TableColMap> {
        RdbRouterInner::parse_col_map(config_str, db_type)
    }

    #[cfg(test)]
    pub(crate) fn from_maps_for_test(
        schema_map: SchemaMap,
        tb_map: TableMap,
        col_map: TableColMap,
        topic_map: HashMap<DbSchemaTb, String>,
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
                let tb_map = if matches!(db_type, DbType::Mssql) {
                    Self::parse_mssql_tb_map(tb_map, db_type)?
                } else {
                    Self::parse_tb_map(tb_map, db_type)?
                };
                let col_map = Self::parse_col_map(col_map, db_type)?;
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
        self.schema_map.get(schema).map_or(schema, String::as_str)
    }

    fn get_tb_map_with_db<'a>(
        &'a self,
        db: &'a str,
        schema: &'a str,
        tb: &'a str,
    ) -> (&'a str, &'a str, &'a str) {
        if let Some((dst_db, dst_schema, dst_tb)) =
            self.tb_map.get(&Self::table_key(db, schema, tb))
        {
            return (dst_db, dst_schema, dst_tb);
        }
        if db.is_empty() {
            (db, self.get_schema_map(schema), tb)
        } else {
            (self.get_schema_map(db), schema, tb)
        }
    }

    fn get_col_map_with_db(
        &self,
        db: &str,
        schema: &str,
        tb: &str,
    ) -> Option<&HashMap<String, String>> {
        self.col_map.get(&Self::table_key(db, schema, tb))
    }

    fn reverse(&self) -> Self {
        let mut reverse_schema_map = HashMap::new();
        let mut reverse_tb_map = HashMap::new();
        let mut reverse_tb_col_map = HashMap::new();

        for (src_table, col_map) in self.col_map.iter() {
            let mut reverse_col_map = HashMap::new();
            for (src_col, dst_col) in col_map.iter() {
                reverse_col_map.insert(dst_col.into(), src_col.into());
            }
            let (dst_db, dst_schema, dst_tb) =
                self.get_tb_map_with_db(&src_table.0, &src_table.1, &src_table.2);
            reverse_tb_col_map.insert(Self::table_key(dst_db, dst_schema, dst_tb), reverse_col_map);
        }

        for (src_tb, dst_tb) in self.tb_map.iter() {
            reverse_tb_map.insert(dst_tb.to_owned(), src_tb.to_owned());
        }

        for (src_schema, dst_schema) in self.schema_map.iter() {
            reverse_schema_map.insert(dst_schema.to_owned(), src_schema.to_owned());
        }
        Self {
            schema_map: reverse_schema_map,
            tb_map: reverse_tb_map,
            col_map: reverse_tb_col_map,
        }
    }

    fn route_row(&self, mut row_data: RowData) -> RowData {
        // tb map
        let (db, schema, tb) = (
            row_data.db.clone(),
            row_data.schema.clone(),
            row_data.tb.clone(),
        );
        let (dst_db, dst_schema, dst_tb) = self.get_tb_map_with_db(&db, &schema, &tb);
        row_data.db = dst_db.to_string();
        row_data.schema = dst_schema.to_string();
        row_data.tb = dst_tb.to_string();

        // col map
        let Some(col_map) = self.get_col_map_with_db(&db, &schema, &tb) else {
            return row_data;
        };

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
        let src_db = ddl_data.default_db.clone();
        let has_rename_target = !ddl_data.get_rename_to_schema_tb().1.is_empty();
        match &mut ddl_data.statement {
            DdlStatement::MysqlAlterTableRename(_)
            | DdlStatement::PgAlterTableRename(_)
            | DdlStatement::RenameTable(_)
            | DdlStatement::MongoCommand(_)
                if has_rename_target =>
            {
                let (_, src_schema, src_tb) = ddl_data.get_db_schema_tb();
                let (_, src_new_schema, src_new_tb) = ddl_data.get_rename_to_db_schema_tb();
                let (_, dst_schema, dst_tb) =
                    self.get_tb_map_with_db(&src_db, &src_schema, &src_tb);
                let (_, dst_new_schema, dst_new_tb) =
                    self.get_tb_map_with_db(&src_db, &src_new_schema, &src_new_tb);
                ddl_data.statement.route_rename_table(
                    dst_schema.into(),
                    dst_tb.into(),
                    dst_new_schema.into(),
                    dst_new_tb.into(),
                );
            }

            _ => {
                let (_, src_schema, src_tb) = ddl_data.get_db_schema_tb();
                let (_, dst_schema, dst_tb) =
                    self.get_tb_map_with_db(&src_db, &src_schema, &src_tb);
                ddl_data.statement.route(dst_schema.into(), dst_tb.into());
            }
        }

        if src_db.is_empty() {
            ddl_data.default_schema = self.get_schema_map(&ddl_data.default_schema).into();
        } else {
            ddl_data.default_db = self.get_schema_map(&src_db).into();
        }
        ddl_data
    }

    fn route_struct(&self, mut struct_data: StructData) -> StructData {
        let src_db = struct_data.db.clone();
        let mut dst_db = src_db.clone();
        match &mut struct_data.statement {
            StructStatement::MysqlCreateTable(s) => {
                let (schema, tb) = (s.table.database_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb) =
                    self.get_tb_map_with_db(&src_db, &schema, &tb);
                dst_db = mapped_db.to_string();
                s.route(dst_schema, dst_tb)
            }

            StructStatement::MysqlCreateDatabase(s) => {
                let src_schema = s.database.name.clone();
                let dst_schema = self.get_schema_map(&src_schema).to_string();
                s.route(&dst_schema)
            }

            StructStatement::MongoCreateCollection(s) => {
                let (schema, tb) = (s.database_name.clone(), s.collection_name.clone());
                let (mapped_db, dst_schema, dst_tb) =
                    self.get_tb_map_with_db(&src_db, &schema, &tb);
                dst_db = mapped_db.to_string();
                s.route(dst_schema, dst_tb)
            }

            StructStatement::MongoShardKey(s) => {
                let ns = s.shard_collection.ns.clone();
                if let Some((schema, tb)) = ns.split_once('.') {
                    let (mapped_db, dst_schema, dst_tb) =
                        self.get_tb_map_with_db(&src_db, schema, tb);
                    dst_db = mapped_db.to_string();
                    s.route(schema, tb, dst_schema, dst_tb)
                }
            }

            StructStatement::PgCreateTable(s) => {
                let (schema, tb) = (s.table.schema_name.clone(), s.table.table_name.clone());
                let (mapped_db, dst_schema, dst_tb) =
                    self.get_tb_map_with_db(&src_db, &schema, &tb);
                dst_db = mapped_db.to_string();
                s.route(dst_schema, dst_tb)
            }

            StructStatement::PgCreateSchema(s) => {
                let src_schema = s.schema.name.clone();
                let dst_schema = self.get_schema_map(&src_schema).to_string();
                s.route(&dst_schema)
            }

            _ => {}
        }

        struct_data.db = dst_db;
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
        let mut schema_map = HashMap::new();
        if matches!(db_type, DbType::Mssql) {
            if config_str.trim().is_empty() {
                return Ok(schema_map);
            }
            let tokens = ConfigTokenParser::parse_config_with_delimiters(
                config_str,
                db_type,
                &[',', ':'],
                None,
            )?;
            for entry in tokens.split(|token| token == ",") {
                let [src, colon, dst] = entry else {
                    bail!(DtError::invalid_config(format!(
                        "invalid MSSQL database mapping: {}; expected source:target",
                        entry.concat()
                    )))
                };
                if colon != ":" {
                    bail!(DtError::invalid_config(format!(
                        "invalid MSSQL database mapping: {}; expected source:target",
                        entry.concat()
                    )))
                }
                schema_map.insert(
                    Self::parse_identifier(src, db_type)?,
                    Self::parse_identifier(dst, db_type)?,
                );
            }
            return Ok(schema_map);
        }

        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            schema_map.insert(tokens[i].to_string(), tokens[i + 1].to_string());
            i += 2;
        }
        Ok(schema_map)
    }

    fn parse_tb_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TableMap> {
        // tb_map=src_db_1.src_tb_1:dst_db_1.dst_tb_1,src_db_2.src_tb_2:dst_db_2.dst_tb_2
        let mut tb_map = HashMap::new();
        let tokens = Self::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            tb_map.insert(
                Self::table_key(EMPTY_DB, &tokens[i], &tokens[i + 1]),
                Self::table_key(EMPTY_DB, &tokens[i + 2], &tokens[i + 3]),
            );
            i += 4;
        }
        Ok(tb_map)
    }

    fn parse_mssql_tb_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TableMap> {
        let mut tb_map = HashMap::new();
        if config_str.trim().is_empty() {
            return Ok(tb_map);
        }

        let tokens = ConfigTokenParser::parse_config_with_delimiters(
            config_str,
            db_type,
            &[',', ':', '.'],
            None,
        )?;
        for entry in tokens.split(|token| token == ",") {
            let mut sides = entry.split(|token| token == ":");
            let (Some(src), Some(dst), None) = (sides.next(), sides.next(), sides.next()) else {
                bail!(DtError::invalid_config(format!(
                    "invalid MSSQL table mapping: {}; expected source:target",
                    entry.concat()
                )))
            };
            tb_map.insert(
                Self::parse_mssql_table_name(src, db_type)?,
                Self::parse_mssql_table_name(dst, db_type)?,
            );
        }
        Ok(tb_map)
    }

    fn parse_col_map(config_str: &str, db_type: &DbType) -> anyhow::Result<TableColMap> {
        let mut results = TableColMap::new();
        if config_str.trim().is_empty() {
            return Ok(results);
        }

        #[derive(Serialize, Deserialize)]
        struct TableColMapConfig {
            db: String,
            tb: String,
            col_map: HashMap<String, String>,
        }
        // col_map=json:[{"db":"test_db","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}]
        let config: Vec<TableColMapConfig> =
            serde_json::from_str(config_str.trim_start_matches(JSON_PREFIX)).context(
                DtError::invalid_config("config [router].col_map is invalid JSON"),
            )?;
        for i in config {
            let key = if matches!(db_type, DbType::Mssql) {
                Self::parse_mssql_json_table_name(&i.db, &i.tb, db_type)?
            } else {
                Self::table_key(EMPTY_DB, &i.db, &i.tb)
            };
            results.insert(key, i.col_map);
        }
        Ok(results)
    }

    fn parse_mssql_json_table_name(
        db: &str,
        tb: &str,
        db_type: &DbType,
    ) -> anyhow::Result<DbSchemaTb> {
        if db.trim().is_empty() {
            bail!(DtError::invalid_config(
                "MSSQL table mapping database must not be empty"
            ));
        }
        let parts = ConfigTokenParser::parse_config(tb, db_type, &['.'], None)?;
        let (schema, tb) = match parts.as_slice() {
            [tb] => (MSSQL_DEFAULT_SCHEMA.to_string(), tb.clone()),
            [schema, tb] => (schema.clone(), tb.clone()),
            _ => {
                bail!(DtError::invalid_config(format!(
                    "invalid MSSQL table mapping selector: database={db}, table={tb}"
                )))
            }
        };
        Ok(Self::table_key(
            &SqlUtil::unescape_by_db_type(db, db_type),
            &SqlUtil::unescape_by_db_type(&schema, db_type),
            &SqlUtil::unescape_by_db_type(&tb, db_type),
        ))
    }

    fn parse_mssql_table_name(parts: &[String], db_type: &DbType) -> anyhow::Result<DbSchemaTb> {
        let (db, schema, tb) = match parts {
            [db, dot, tb] if dot == "." => (db, MSSQL_DEFAULT_SCHEMA, tb),
            [db, dot_1, schema, dot_2, tb] if dot_1 == "." && dot_2 == "." => {
                (db, schema.as_str(), tb)
            }
            _ => {
                bail!(DtError::invalid_config(format!(
                    "invalid MSSQL table name: {}; expected database.table or database.schema.table",
                    parts.concat()
                )))
            }
        };
        Ok(Self::table_key(
            &SqlUtil::unescape_by_db_type(db, db_type),
            &SqlUtil::unescape_by_db_type(schema, db_type),
            &SqlUtil::unescape_by_db_type(tb, db_type),
        ))
    }

    fn parse_identifier(value: &str, db_type: &DbType) -> anyhow::Result<String> {
        let tokens = ConfigTokenParser::parse_config(value, db_type, &[], None)?;
        let [token] = tokens.as_slice() else {
            bail!(DtError::invalid_config(format!(
                "invalid MSSQL identifier: {value}"
            )))
        };
        if token.is_empty() {
            bail!(DtError::invalid_config(
                "MSSQL identifier must not be empty"
            ));
        }
        Ok(SqlUtil::unescape_by_db_type(token, db_type))
    }

    fn table_key(db: &str, schema: &str, tb: &str) -> DbSchemaTb {
        (db.to_string(), schema.to_string(), tb.to_string())
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

    fn get_topic<'a>(&'a self, db: &str, schema: &str, tb: &str) -> &'a str {
        // *.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        if let Some(topic) = self
            .topic_map
            .get(&RdbRouterInner::table_key(db, schema, tb))
        {
            return topic;
        }
        if let Some(topic) = self
            .topic_map
            .get(&RdbRouterInner::table_key(db, schema, "*"))
        {
            return topic;
        }
        // should always has a default topic map
        self.topic_map
            .get(&RdbRouterInner::table_key(EMPTY_DB, "*", "*"))
            .map_or("", String::as_str)
    }

    fn parse_topic_map(
        config_str: &str,
        db_type: &DbType,
    ) -> anyhow::Result<HashMap<DbSchemaTb, String>> {
        // topic_map=*.*:test,test_db_1.*:test2,test_db_1.no_pk_one_uk:test3
        let mut topic_map = HashMap::new();
        let tokens = RdbRouterInner::parse_config(config_str, db_type)?;
        let mut i = 0;
        while i < tokens.len() {
            topic_map.insert(
                RdbRouterInner::table_key(EMPTY_DB, &tokens[i], &tokens[i + 1]),
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

    use dt_common::{
        config::{config_enums::DbType, router_config::RouterConfig},
        meta::{row_data::RowData, row_type::RowType},
    };

    use super::{RdbRouter, SchemaMap, TableColMap, TableMap};

    #[test]
    fn test_parse_ignore_cols() {
        let config_str =
            r#"json:[{"db":"db_1","tb":"tb_1","col_map":{"f_0":"dst_f_0","f_1":"dst_f_1"}}]"#;
        let col_map = RdbRouter::parse_col_map(config_str, &DbType::Mysql).unwrap();
        let tb_1 = col_map
            .get(&(String::new(), "db_1".to_string(), "tb_1".to_string()))
            .unwrap();
        assert_eq!(tb_1.len(), 2);
        assert_eq!(*tb_1.get("f_0").unwrap(), "dst_f_0".to_string());
        assert_eq!(*tb_1.get("f_1").unwrap(), "dst_f_1".to_string());
    }

    #[test]
    fn test_parse_schema_map() {
        let assert_mapping = |schema_map: &SchemaMap, src_schema: &str, dst_schema: &str| {
            assert_eq!(schema_map.get(src_schema).unwrap(), dst_schema);
        };

        // mysql
        let config_str = "src_1:dst_1,`src,2'`:dst_2,`src:3,`:`dst:3,`";
        let schema_map = RdbRouter::parse_schema_map(config_str, &DbType::Mysql).unwrap();
        assert_mapping(&schema_map, "src_1", "dst_1");
        assert_mapping(&schema_map, "src,2'", "dst_2");
        assert_mapping(&schema_map, "src:3,", "dst:3,");
        assert_eq!(schema_map.get("src_4"), None);

        // pg
        let config_str = r#"src_1:dst_1,"src,2'":dst_2,"src:3,":"dst:3,""#;
        let schema_map = RdbRouter::parse_schema_map(config_str, &DbType::Pg).unwrap();
        assert_mapping(&schema_map, "src_1", "dst_1");
        assert_mapping(&schema_map, "src,2'", "dst_2");
        assert_mapping(&schema_map, "src:3,", "dst:3,");
        assert_eq!(schema_map.get("src_4"), None);
    }

    #[test]
    fn test_parse_tb_map() {
        let assert_exists =
            |tb_map: &TableMap, src_db: &str, src_tb: &str, dst_db: &str, dst_tb: &str| {
                assert_eq!(
                    tb_map
                        .get(&(String::new(), src_db.into(), src_tb.into()))
                        .unwrap(),
                    &(String::new(), dst_db.into(), dst_tb.into())
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
        assert_eq!(
            tb_map.get(&(String::new(), "src_db_4".into(), "src_tb_4".into())),
            None
        );

        // pg
        let config_str = r#"src_db_1.src_tb_1:dst_db_1.dst_tb_1,"#.to_string()
            + r#""src_db,2'"."src_tb,2'":dst_db_2.dst_tb_2,"#
            + r#""src_db:3,"."src_tb:3,":"dst_db:3,"."dst_tb:3,""#;
        let tb_map = RdbRouter::parse_tb_map(&config_str, &DbType::Pg).unwrap();

        assert_exists(&tb_map, "src_db_1", "src_tb_1", "dst_db_1", "dst_tb_1");
        assert_exists(&tb_map, "src_db,2'", "src_tb,2'", "dst_db_2", "dst_tb_2");
        assert_exists(&tb_map, "src_db:3,", "src_tb:3,", "dst_db:3,", "dst_tb:3,");
        assert_eq!(
            tb_map.get(&(String::new(), "src_db_4".into(), "src_tb_4".into())),
            None
        );
    }

    #[test]
    fn test_parse_tb_col_map() {
        let assert_col_map = |tb_map: &TableColMap,
                              src_db: &str,
                              src_tb: &str,
                              col_map: &HashMap<String, String>| {
            assert_eq!(
                tb_map
                    .get(&(String::new(), src_db.into(), src_tb.into()))
                    .unwrap(),
                col_map
            )
        };

        let check_results = |tb_col_map: &TableColMap| {
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
                tb_col_map.get(&(String::new(), "src_db_4".into(), "src_tb_4".into())),
                None
            );
        };

        // mysql
        let config_str = r#"[{"db":"src_db_1","tb":"src_tb_1","col_map":{"src_col_1":"dst_col_1","src_col_2":"dst_col_2"}},"#.to_string()
            + r#"{"db":"src_db,2'","tb":"src_tb,2'","col_map":{"src_col,1'":"dst_col_1","src_col,2'":"dst_col_2"}},"#
            + r#"{"db":"src_db:3,","tb":"src_tb:3,","col_map":{"src_col:1,":"dst_col:1,","src_col:2,":"dst_col:2,"}}]"#;
        let tb_col_map = RdbRouter::parse_col_map(&config_str, &DbType::Mysql).unwrap();
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
    fn table_routes_are_isolated_by_db() {
        let mut tb_map = TableMap::new();
        tb_map.insert(
            ("db1".into(), "schema1".into(), "tb1".into()),
            ("dst_db1".into(), "dst_schema1".into(), "dst_tb1".into()),
        );
        let router =
            RdbRouter::from_maps_for_test(HashMap::new(), tb_map, HashMap::new(), HashMap::new());

        let routed = router.route_row(RowData::new(
            "db1".into(),
            "schema1".into(),
            "tb1".into(),
            0,
            RowType::Insert,
            None,
            None,
        ));
        assert_eq!(
            (
                routed.db.as_str(),
                routed.schema.as_str(),
                routed.tb.as_str()
            ),
            ("dst_db1", "dst_schema1", "dst_tb1")
        );
        assert_eq!(
            router.get_tb_map_with_db("db2", "schema1", "tb1"),
            ("db2", "schema1", "tb1")
        );
    }

    #[test]
    fn configured_routes_use_empty_physical_db() {
        let config = RouterConfig::Rdb {
            schema_map: "schema1:dst_schema1".to_string(),
            tb_map: "schema2.tb1:dst_schema2.dst_tb1".to_string(),
            col_map: String::new(),
            topic_map: String::new(),
        };

        for db_type in [DbType::Mysql, DbType::Pg] {
            let router = RdbRouter::from_config(&config, &db_type).unwrap().unwrap();

            assert_eq!(router.get_schema_map("schema1"), "dst_schema1");
            assert_eq!(router.get_tb_map("schema1", "tb1"), ("dst_schema1", "tb1"));
            assert_eq!(
                router.get_tb_map("schema2", "tb1"),
                ("dst_schema2", "dst_tb1")
            );
            assert_eq!(
                router.get_tb_map_with_db("another_db", "schema2", "tb1"),
                ("another_db", "schema2", "tb1")
            );
        }
    }

    #[test]
    fn mssql_routes_use_physical_database_and_default_schema() {
        let config = RouterConfig::Rdb {
            schema_map: "[db:1]:[archive,1],db2:archive2".to_string(),
            tb_map: "[db:1].orders:[archive,1].orders_copy,db2.sales.orders:archive2.history.orders_copy"
                .to_string(),
            col_map: r#"json:[{"db":"db2","tb":"sales.orders","col_map":{"id":"order_id"}}]"#
                .to_string(),
            topic_map: String::new(),
        };

        let router = RdbRouter::from_config(&config, &DbType::Mssql)
            .unwrap()
            .unwrap();

        assert_eq!(router.get_schema_map("db:1"), "archive,1");
        assert_eq!(
            router.get_tb_map_with_db("db:1", "dbo", "orders"),
            ("archive,1", "dbo", "orders_copy")
        );
        assert_eq!(
            router.get_tb_map_with_db("db2", "sales", "orders"),
            ("archive2", "history", "orders_copy")
        );
        assert_eq!(
            router
                .get_col_map_with_db("db2", "sales", "orders")
                .unwrap()
                .get("id")
                .map(String::as_str),
            Some("order_id")
        );
        assert_eq!(
            router.reverse_get_tb_map_with_db("archive,1", "dbo", "orders_copy"),
            ("db:1", "dbo", "orders")
        );
        assert_eq!(router.get_schema_map("db2"), "archive2");
    }

    #[test]
    fn schema_map_routes_mssql_database_fallback() {
        let mut schema_map = HashMap::new();
        schema_map.insert("db1".to_string(), "dst_db1".to_string());
        let mut col_map = TableColMap::new();
        col_map.insert(
            ("db1".into(), "schema1".into(), "tb1".into()),
            HashMap::from([("col1".to_string(), "dst_col1".to_string())]),
        );
        let router =
            RdbRouter::from_maps_for_test(schema_map, HashMap::new(), col_map, HashMap::new());

        assert_eq!(router.get_schema_map("db1"), "dst_db1");
        assert_eq!(router.reverse_get_schema_map("dst_db1"), "db1");
        assert_eq!(
            router.get_tb_map_with_db("db1", "schema1", "tb1"),
            ("dst_db1", "schema1", "tb1")
        );
        assert_eq!(
            router
                .reverse_get_col_map_with_db("dst_db1", "schema1", "tb1")
                .unwrap()
                .get("dst_col1")
                .map(String::as_str),
            Some("col1")
        );
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
