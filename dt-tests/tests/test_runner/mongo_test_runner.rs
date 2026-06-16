use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

use dt_common::meta::mongo::mongo_constant::MongoConstants;
use dt_common::{
    config::{
        config_enums::DbType, extractor_config::ExtractorConfig, sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    utils::time_util::TimeUtil,
};
use dt_connector::rdb_router::RdbRouter;
use dt_task::task_util::TaskUtil;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Client,
};
use regex::{Captures, Regex};
use sqlx::types::chrono::Utc;

use crate::test_config_util::TestConfigUtil;

use super::base_test_runner::BaseTestRunner;

pub struct MongoTestRunner {
    pub base: BaseTestRunner,
    src_mongo_client: Option<Client>,
    dst_mongo_client: Option<Client>,
    router: Option<RdbRouter>,
}

pub const SRC: &str = "src";
pub const DST: &str = "dst";

#[allow(dead_code)]
impl MongoTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        let mut src_mongo_client = None;
        let mut dst_mongo_client = None;

        let config = TaskConfig::new(&base.task_config_file).unwrap();
        match &config.extractor {
            ExtractorConfig::MongoSnapshot {
                url,
                connection_auth,
                is_direct_connection,
                app_name,
                ..
            }
            | ExtractorConfig::MongoCdc {
                url,
                connection_auth,
                is_direct_connection,
                app_name,
                ..
            }
            | ExtractorConfig::MongoCheck {
                url,
                connection_auth,
                is_direct_connection,
                app_name,
                ..
            } => {
                src_mongo_client = Some(
                    TaskUtil::create_mongo_client(
                        url,
                        connection_auth,
                        *is_direct_connection,
                        Some(app_name.to_owned()),
                        None,
                    )
                    .await
                    .unwrap(),
                );
            }
            _ => {}
        }

        if let SinkerConfig::Mongo {
            url,
            connection_auth,
            is_direct_connection,
            app_name,
            ..
        } = &config.sinker
        {
            dst_mongo_client = Some(
                TaskUtil::create_mongo_client(
                    url,
                    connection_auth,
                    *is_direct_connection,
                    Some(app_name.to_owned()),
                    None,
                )
                .await
                .unwrap(),
            );
        }

        if dst_mongo_client.is_none() {
            if let Some(checker_target) = config.checker_target() {
                if matches!(checker_target.db_type, DbType::Mongo) {
                    dst_mongo_client = Some(
                        TaskUtil::create_mongo_client(
                            &checker_target.url,
                            &checker_target.connection_auth,
                            None,
                            None,
                            None,
                        )
                        .await
                        .unwrap(),
                    );
                }
            }
        }

        // cleanup dbs before tests
        let mongo_dbs = Self::collect_databases(&base);
        if !mongo_dbs.is_empty() {
            if let Some(client) = src_mongo_client.as_ref() {
                Self::drop_databases(client, &mongo_dbs).await?;
            }
            if let Some(client) = dst_mongo_client.as_ref() {
                Self::drop_databases(client, &mongo_dbs).await?;
            }
        }

        let router = RdbRouter::from_config(&config.router, &DbType::Mongo).unwrap();
        Ok(Self {
            base,
            src_mongo_client,
            dst_mongo_client,
            router,
        })
    }

    pub async fn run_cdc_resume_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        // update start_timestamp to make sure the subsequent cdc task can get old events
        let start_timestamp = Utc::now().timestamp().to_string();
        let config = vec![(
            "extractor".into(),
            "start_timestamp".into(),
            start_timestamp,
        )];
        TestConfigUtil::update_task_config(
            &self.base.task_config_file,
            &self.base.task_config_file,
            &config,
        );

        // execute sqls in src before cdc task starts
        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let src_sqls = Self::slice_sqls_by_db(&self.base.src_test_sqls);
        for (db, sqls) in src_sqls.iter() {
            let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
                Self::slice_sqls_by_type(sqls);
            // insert
            self.execute_dmls(src_mongo_client, db, &src_insert_sqls)
                .await
                .unwrap();
            // update
            self.execute_dmls(src_mongo_client, db, &src_update_sqls)
                .await
                .unwrap();
            // delete
            self.execute_dmls(src_mongo_client, db, &src_delete_sqls)
                .await
                .unwrap();
        }
        TimeUtil::sleep_millis(start_millis).await;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;
        for (db, _) in src_sqls.iter() {
            self.compare_db_data(db).await;
        }

        for (db, sqls) in src_sqls.iter() {
            let (_, _, src_delete_sqls) = Self::slice_sqls_by_type(sqls);
            // delete
            self.execute_dmls(src_mongo_client, db, &src_delete_sqls)
                .await
                .unwrap();
        }
        TimeUtil::sleep_millis(parse_millis).await;
        for (db, _) in src_sqls.iter() {
            self.compare_db_data(db).await;
        }

        self.base.abort_task(&task).await
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();

        let src_sqls = Self::slice_sqls_by_db(&self.base.src_test_sqls);
        for (db, sqls) in src_sqls.iter() {
            let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
                Self::slice_sqls_by_type(sqls);
            // insert
            self.execute_dmls(src_mongo_client, db, &src_insert_sqls)
                .await
                .unwrap();
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_db_data(db).await;

            // update
            self.execute_dmls(src_mongo_client, db, &src_update_sqls)
                .await
                .unwrap();
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_db_data(db).await;

            // delete
            self.execute_dmls(src_mongo_client, db, &src_delete_sqls)
                .await
                .unwrap();
            TimeUtil::sleep_millis(parse_millis).await;
            self.compare_db_data(db).await;
        }
        self.base.abort_task(&task).await
    }

    pub async fn run_changestream_ddl_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;
        TimeUtil::sleep_millis(1200).await;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        self.execute_sqls_in_order_with_client(
            self.src_mongo_client.as_ref().unwrap(),
            &self.base.src_test_sqls,
        )
        .await?;
        TimeUtil::sleep_millis(parse_millis).await;

        self.assert_changestream_ddl_result().await?;
        self.base.abort_task(&task).await
    }

    pub async fn run_snapshot_test(&self, compare_data: bool) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;
        self.execute_test_sqls().await?;

        self.base.start_task().await?;

        let src_sqls = Self::slice_sqls_by_db(&self.base.src_test_sqls);
        if compare_data {
            for (db, _) in src_sqls.iter() {
                self.compare_db_data(db).await;
            }
        }
        Ok(())
    }

    pub async fn run_heartbeat_test(
        &self,
        start_millis: u64,
        _parse_millis: u64,
    ) -> anyhow::Result<()> {
        self.execute_prepare_sqls().await?;

        let config = TaskConfig::new(&self.base.task_config_file).unwrap();
        let (db, tb) = match config.extractor {
            ExtractorConfig::MongoCdc { heartbeat_tb, .. } => {
                let tokens: Vec<&str> = heartbeat_tb.split(".").collect();
                (tokens[0].to_string(), tokens[1].to_string())
            }
            _ => (String::new(), String::new()),
        };

        let src_data = self.fetch_data(&db, &tb, SRC).await;
        assert!(src_data.is_empty());

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        let src_data = self.fetch_data(&db, &tb, SRC).await;
        assert_eq!(src_data.len(), 1);

        self.base.abort_task(&task).await
    }

    pub async fn execute_prepare_sqls(&self) -> anyhow::Result<()> {
        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let dst_mongo_client = self.dst_mongo_client.as_ref().unwrap();

        let src_sqls = Self::slice_sqls_by_db(&self.base.src_prepare_sqls);
        let dst_sqls = Self::slice_sqls_by_db(&self.base.dst_prepare_sqls);

        for (db, sqls) in src_sqls.iter() {
            self.execute_ddls(src_mongo_client, db, sqls).await?;
            self.execute_dmls(src_mongo_client, db, sqls).await?;
        }
        for (db, sqls) in dst_sqls.iter() {
            self.execute_ddls(dst_mongo_client, db, sqls).await?;
            self.execute_dmls(dst_mongo_client, db, sqls).await?;
        }
        Ok(())
    }

    pub async fn execute_clean_sqls(&self) -> anyhow::Result<()> {
        let src_mongo_client = self.src_mongo_client.as_ref().unwrap();
        let dst_mongo_client = self.dst_mongo_client.as_ref().unwrap();

        let src_sqls = Self::slice_sqls_by_db(&self.base.src_clean_sqls);
        let dst_sqls = Self::slice_sqls_by_db(&self.base.dst_clean_sqls);

        for (db, sqls) in src_sqls.iter() {
            self.execute_ddls(src_mongo_client, db, sqls).await?;
            self.execute_dmls(src_mongo_client, db, sqls).await?;
        }
        for (db, sqls) in dst_sqls.iter() {
            self.execute_ddls(dst_mongo_client, db, sqls).await?;
            self.execute_dmls(dst_mongo_client, db, sqls).await?;
        }
        Ok(())
    }

    pub fn src_mongo_client(&self) -> &Client {
        self.src_mongo_client
            .as_ref()
            .expect("src_mongo_client is not initialized")
    }

    pub fn dst_mongo_client(&self) -> &Client {
        self.dst_mongo_client
            .as_ref()
            .expect("dst_mongo_client is not initialized")
    }

    pub async fn execute_test_sqls(&self) -> anyhow::Result<()> {
        self.execute_sqls_with_client(
            self.src_mongo_client.as_ref().unwrap(),
            &self.base.src_test_sqls,
        )
        .await?;
        self.execute_sqls_with_client(
            self.dst_mongo_client.as_ref().unwrap(),
            &self.base.dst_test_sqls,
        )
        .await?;
        Ok(())
    }

    pub async fn execute_sqls_with_client(
        &self,
        client: &Client,
        sqls: &[String],
    ) -> anyhow::Result<()> {
        let sliced_sqls = Self::slice_sqls_by_db(sqls);
        for (db, sqls) in sliced_sqls.iter() {
            self.execute_ddls(client, db, sqls).await?;
            self.execute_dmls(client, db, sqls).await?;
        }
        Ok(())
    }

    pub async fn execute_sqls_in_order_with_client(
        &self,
        client: &Client,
        sqls: &[String],
    ) -> anyhow::Result<()> {
        let mut db = String::new();
        for sql in sqls.iter() {
            if sql.starts_with("use") {
                db = Self::get_db(sql);
                continue;
            }

            if self.execute_ddl_sql(client, &db, sql).await? {
                continue;
            }
            self.execute_dml_sql(client, &db, sql).await?;
        }
        Ok(())
    }

    async fn execute_ddls(&self, client: &Client, db: &str, sqls: &[String]) -> anyhow::Result<()> {
        for sql in sqls.iter() {
            self.execute_ddl_sql(client, db, sql).await?;
        }
        Ok(())
    }

    async fn execute_dmls(&self, client: &Client, db: &str, sqls: &[String]) -> anyhow::Result<()> {
        for sql in sqls.iter() {
            self.execute_dml_sql(client, db, sql).await?;
        }
        Ok(())
    }

    async fn execute_ddl_sql(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<bool> {
        if sql.contains("dropDatabase") {
            self.execute_drop_database(client, db).await.unwrap();
            return Ok(true);
        }
        if sql.contains("renameCollection") {
            self.execute_rename_collection(client, db, sql)
                .await
                .unwrap();
            return Ok(true);
        }
        if sql.contains("createIndex") {
            self.execute_create_index(client, db, sql).await.unwrap();
            return Ok(true);
        }
        if sql.contains("dropIndex") {
            self.execute_drop_index(client, db, sql).await.unwrap();
            return Ok(true);
        }
        if sql.contains("runCommand") {
            self.execute_run_command(client, db, sql).await.unwrap();
            return Ok(true);
        }
        if sql.contains(".drop()") {
            self.execute_drop(client, db, sql).await.unwrap();
            return Ok(true);
        }
        if sql.contains("createCollection") {
            self.execute_create(client, db, sql).await.unwrap();
            return Ok(true);
        }
        Ok(false)
    }

    async fn execute_dml_sql(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        if sql.contains(".insert") {
            self.execute_insert(client, db, sql).await?;
        } else if sql.contains(".update") {
            self.execute_update(client, db, sql).await?;
        } else if sql.contains(".delete") {
            self.execute_delete(client, db, sql).await?;
        }
        Ok(())
    }

    fn get_db(sql: &str) -> String {
        let re = Regex::new(r"use[ ]+(\w+)").unwrap();
        let cap = re.captures(sql).unwrap();
        cap.get(1).unwrap().as_str().to_string()
    }

    async fn execute_drop(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).drop()").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();

        client
            .database(db)
            .collection::<Document>(tb)
            .drop()
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_drop_database(&self, client: &Client, db: &str) -> anyhow::Result<()> {
        client.database(db).drop().await.unwrap();
        Ok(())
    }

    async fn execute_create(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r#"db.createCollection\("(\w+)"(?:\s*,\s*([\w\W]+))?\)"#).unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();

        let mut command = doc! { "create": tb };
        if let Some(options) = cap.get(2) {
            let options = Self::parse_doc(options.as_str());
            for (key, value) in options {
                command.insert(key, value);
            }
        }

        client.database(db).run_command(command).await.unwrap();
        Ok(())
    }

    async fn execute_create_index(
        &self,
        client: &Client,
        db: &str,
        sql: &str,
    ) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).createIndex\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let args = Self::split_top_level_args(cap.get(2).unwrap().as_str());
        let key = Self::parse_doc(&args[0]);
        let options = if args.len() > 1 {
            Self::parse_doc(&args[1])
        } else {
            Document::new()
        };
        let name = options
            .get_str("name")
            .map(str::to_string)
            .unwrap_or_else(|_| {
                key.iter()
                    .map(|(key, value)| format!("{}_{}", key, value))
                    .collect::<Vec<_>>()
                    .join("_")
            });

        let mut index = doc! { "key": key, "name": name };
        for (key, value) in options {
            if key != "name" {
                index.insert(key, value);
            }
        }

        client
            .database(db)
            .run_command(doc! { "createIndexes": tb, "indexes": [index] })
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_drop_index(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r#"db.(\w+).dropIndex\("([^"]+)"\)"#).unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let index = cap.get(2).unwrap().as_str();

        client
            .database(db)
            .run_command(doc! { "dropIndexes": tb, "index": index })
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_rename_collection(
        &self,
        client: &Client,
        db: &str,
        sql: &str,
    ) -> anyhow::Result<()> {
        let re = Regex::new(r#"db.(\w+).renameCollection\("(\w+)"\)"#).unwrap();
        let cap = re.captures(sql).unwrap();
        let from = cap.get(1).unwrap().as_str();
        let to = cap.get(2).unwrap().as_str();

        client
            .database("admin")
            .run_command(doc! { "renameCollection": format!("{}.{}", db, from), "to": format!("{}.{}", db, to) })
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_run_command(
        &self,
        client: &Client,
        db: &str,
        sql: &str,
    ) -> anyhow::Result<()> {
        let re = Regex::new(r"db.runCommand\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let command = Self::parse_doc(cap.get(1).unwrap().as_str());

        client.database(db).run_command(command).await.unwrap();
        Ok(())
    }

    async fn execute_insert(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        // example: db.tb_2.insertOne({ "name": "a", "age": "1" })
        let re = Regex::new(r"db.(\w+).insert(One|Many)\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc_content = Self::normalize_doc_string(cap.get(3).unwrap().as_str());

        let coll = client.database(db).collection::<Document>(tb);
        let json_value: Value = serde_json::from_str(&doc_content).unwrap();
        let parsed = Self::convert_extended_json(Bson::try_from(json_value).unwrap());
        if sql.contains("insertOne") {
            let doc = match parsed {
                Bson::Document(doc) => doc,
                other => panic!("expected document for insertOne, got {:?}", other),
            };
            coll.insert_one(doc).await.unwrap();
        } else {
            let docs = match parsed {
                Bson::Array(arr) => arr
                    .into_iter()
                    .map(|item| match item {
                        Bson::Document(doc) => doc,
                        other => panic!("expected document inside array, got {:?}", other),
                    })
                    .collect::<Vec<Document>>(),
                other => panic!("expected array for insertMany, got {:?}", other),
            };
            coll.insert_many(docs).await.unwrap();
        }
        Ok(())
    }

    async fn execute_delete(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).delete(One|Many)\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc = cap.get(3).unwrap().as_str();
        let normalized_doc = Self::normalize_doc_string(doc);
        let json_value: Value = serde_json::from_str(&normalized_doc).unwrap();
        let parsed = Self::convert_extended_json(Bson::try_from(json_value).unwrap());
        let doc = match parsed {
            Bson::Document(doc) => doc,
            other => panic!("expected document for delete, got {:?}", other),
        };
        let coll = client.database(db).collection::<Document>(tb);
        if sql.contains("deleteOne") {
            coll.delete_one(doc).await.unwrap();
        } else {
            coll.delete_many(doc).await.unwrap();
        }
        Ok(())
    }

    async fn execute_update(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).update(One|Many)").unwrap();
        let cap = match re.captures(sql) {
            Some(cap) => cap,
            None => return Ok(()),
        };
        let tb = cap.get(1).unwrap().as_str();
        let args_start = sql.find('(').unwrap();
        let args_end = sql.rfind(')').unwrap();
        let args = &sql[args_start + 1..args_end];
        let (query_doc, update_doc) = Self::split_update_args(args);
        let normalized_query = Self::normalize_doc_string(&query_doc);
        let normalized_update = Self::normalize_doc_string(&update_doc);
        let json_query: Value = serde_json::from_str(&normalized_query).unwrap();
        let json_update: Value = serde_json::from_str(&normalized_update).unwrap();
        let parsed_query = Self::convert_extended_json(Bson::try_from(json_query).unwrap());
        let parsed_update = Self::convert_extended_json(Bson::try_from(json_update).unwrap());
        let query_doc = match parsed_query {
            Bson::Document(doc) => doc,
            other => panic!("expected document for update query, got {:?}", other),
        };
        let update_doc = match parsed_update {
            Bson::Document(doc) => doc,
            other => panic!("expected document for update update, got {:?}", other),
        };
        let coll = client.database(db).collection::<Document>(tb);
        if sql.contains("updateOne") {
            coll.update_one(query_doc, update_doc).await.unwrap();
        } else {
            coll.update_many(query_doc, update_doc).await.unwrap();
        }
        Ok(())
    }

    fn split_update_args(args: &str) -> (String, String) {
        let args = Self::split_top_level_args(args);
        let query = args.first().cloned().unwrap_or_default();
        let update = args.get(1).cloned().unwrap_or_default();
        (query, update)
    }

    fn split_top_level_args(args: &str) -> Vec<String> {
        let mut depth = 0;
        let mut in_string = false;
        let mut escaped = false;
        let mut start = 0;
        let mut result = Vec::new();
        for (idx, ch) in args.char_indices() {
            if in_string {
                escaped = ch == '\\' && !escaped;
                if ch == '"' && !escaped {
                    in_string = false;
                } else if ch != '\\' {
                    escaped = false;
                }
                continue;
            }

            match ch {
                '"' => in_string = true,
                '{' | '[' | '(' => depth += 1,
                '}' | ']' | ')' => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                ',' if depth == 0 => {
                    result.push(args[start..idx].trim().to_string());
                    start = idx + 1;
                }
                _ => {}
            }
        }
        if start < args.len() {
            result.push(args[start..].trim().to_string());
        }
        result
    }

    async fn compare_db_data(&self, db: &str) {
        let tbs = self.list_tb(db, SRC).await;
        for tb in tbs.iter() {
            self.compare_tb_data(db, tb).await;
        }
    }

    async fn compare_tb_data(&self, db: &str, tb: &str) {
        println!("compare tb data, db: {}, tb: {}", db, tb);
        let src_data = self.fetch_data(db, tb, SRC).await;

        let (dst_db, dst_tb) = match &self.router {
            Some(router) => router.get_tb_map(db, tb),
            None => (db, tb),
        };
        let dst_data = self.fetch_data(dst_db, dst_tb, DST).await;

        assert_eq!(src_data.len(), dst_data.len());
        for id in src_data.keys() {
            let src_value = src_data.get(id);
            let dst_value = dst_data.get(id);
            println!(
                "compare tb data, db: {}, tb: {}, src_value: {:?}, dst_value: {:?}",
                db, tb, src_value, dst_value
            );
            assert_eq!(src_value, dst_value);
        }
    }

    async fn list_tb(&self, db: &str, from: &str) -> Vec<String> {
        let client = if from == SRC {
            self.src_mongo_client.as_ref().unwrap()
        } else {
            self.dst_mongo_client.as_ref().unwrap()
        };
        client.database(db).list_collection_names().await.unwrap()
    }

    pub async fn fetch_data(&self, db: &str, tb: &str, from: &str) -> HashMap<String, Document> {
        let client = if from == SRC {
            self.src_mongo_client.as_ref().unwrap()
        } else {
            self.dst_mongo_client.as_ref().unwrap()
        };

        let collection = client.database(db).collection::<Document>(tb);
        let mut cursor = collection
            .find(doc! {})
            .sort(doc! {MongoConstants::ID: 1})
            .await
            .unwrap();

        let mut results = HashMap::new();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let id = Self::doc_id_key(&doc);
            results.insert(id, doc);
        }
        results
    }

    async fn assert_changestream_ddl_result(&self) -> anyhow::Result<()> {
        let dst = self.dst_mongo_client.as_ref().unwrap();
        let ddl_db = dst.database("ddl_db");
        let collection_names = ddl_db.list_collection_names().await.unwrap();
        assert!(collection_names.contains(&"created_coll".to_string()));
        assert!(collection_names.contains(&"renamed_coll".to_string()));
        assert!(!collection_names.contains(&"rename_me".to_string()));
        assert!(!collection_names.contains(&"dropped_coll".to_string()));
        assert!(!collection_names.contains(&"ignored_coll".to_string()));

        let renamed_data = self.fetch_data("ddl_db", "renamed_coll", DST).await;
        assert_eq!(renamed_data.len(), 1);

        let created_data = self.fetch_data("ddl_db", "created_coll", DST).await;
        assert_eq!(created_data.len(), 1);

        assert!(dst
            .database("ddl_drop_db")
            .list_collection_names()
            .await
            .unwrap()
            .is_empty());
        Ok(())
    }

    fn doc_id_key(doc: &Document) -> String {
        let id = doc.get(MongoConstants::ID).unwrap_or_else(|| {
            panic!(
                "Mongo document missing `_id`, doc: {}",
                Bson::Document(doc.clone()).into_canonical_extjson()
            )
        });
        // use canonical extended JSON to ensure consistent representation of the _id value.
        id.clone().into_canonical_extjson().to_string()
    }

    fn slice_sqls_by_db(sqls: &[String]) -> HashMap<String, Vec<String>> {
        let mut db = String::new();
        let mut sliced_sqls: HashMap<String, Vec<String>> = HashMap::new();
        for sql in sqls.iter() {
            if sql.starts_with("use") {
                db = Self::get_db(sql);
                continue;
            }

            if let Some(sqls) = sliced_sqls.get_mut(&db) {
                sqls.push(sql.into());
            } else {
                sliced_sqls.insert(db.clone(), vec![sql.into()]);
            }
        }
        sliced_sqls
    }

    fn slice_sqls_by_type(sqls: &[String]) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut insert_sqls = Vec::new();
        let mut update_sqls = Vec::new();
        let mut delete_sqls = Vec::new();
        for sql in sqls.iter() {
            if sql.contains("insert") {
                insert_sqls.push(sql.clone());
            }
            if sql.contains("update") {
                update_sqls.push(sql.clone());
            }
            if sql.contains("delete") {
                delete_sqls.push(sql.clone());
            }
        }
        (insert_sqls, update_sqls, delete_sqls)
    }

    fn collect_databases(base: &BaseTestRunner) -> HashSet<String> {
        let mut dbs = HashSet::new();
        let sections = [
            &base.src_prepare_sqls,
            &base.dst_prepare_sqls,
            &base.src_test_sqls,
            &base.dst_test_sqls,
            &base.src_clean_sqls,
            &base.dst_clean_sqls,
        ];
        for sqls in sections.iter() {
            Self::add_dbs_from_sqls(sqls, &mut dbs);
        }
        dbs
    }

    fn add_dbs_from_sqls(sqls: &[String], dbs: &mut HashSet<String>) {
        for sql in sqls.iter() {
            if sql.trim_start().starts_with("use ") {
                let db = Self::get_db(sql);
                if !db.is_empty() {
                    dbs.insert(db);
                }
            }
        }
    }

    async fn drop_databases(client: &Client, dbs: &HashSet<String>) -> anyhow::Result<()> {
        for db in dbs.iter() {
            if db.is_empty() {
                continue;
            }
            client.database(db).drop().await?;
        }
        Ok(())
    }

    fn normalize_doc_string(doc: &str) -> String {
        let oid_re = Regex::new(r#"ObjectId\("([a-fA-F0-9]{24})"\)"#).unwrap();
        let doc = oid_re.replace_all(doc, r#"{"$$oid":"$1"}"#).to_string();
        let number_long_re = Regex::new(r#"NumberLong\(\s*"?(-?\d+)"?\s*\)"#).unwrap();
        let doc = number_long_re.replace_all(&doc, "$1").to_string();

        let re =
            Regex::new(r"(?P<prefix>[\{\[,]\s*)(?P<key>[$A-Za-z_][A-Za-z0-9_$]*)\s*:").unwrap();
        re.replace_all(&doc, |caps: &Captures| {
            format!("{}\"{}\":", &caps["prefix"], &caps["key"])
        })
        .to_string()
    }

    fn parse_doc(doc: &str) -> Document {
        let normalized_doc = Self::normalize_doc_string(doc);
        let json_value: Value = serde_json::from_str(&normalized_doc).unwrap();
        match Self::convert_extended_json(Bson::try_from(json_value).unwrap()) {
            Bson::Document(doc) => doc,
            other => panic!("expected document, got {:?}", other),
        }
    }

    fn convert_extended_json(value: Bson) -> Bson {
        match value {
            Bson::Document(doc) => {
                if doc.len() == 1 {
                    if let Some(Bson::String(s)) = doc.get("$oid") {
                        if let Ok(oid) = ObjectId::parse_str(s) {
                            return Bson::ObjectId(oid);
                        }
                    }
                }
                let mut normalized = Document::new();
                for (k, v) in doc.into_iter() {
                    normalized.insert(k, Self::convert_extended_json(v));
                }
                Bson::Document(normalized)
            }
            Bson::Array(arr) => Bson::Array(
                arr.into_iter()
                    .map(Self::convert_extended_json)
                    .collect::<Vec<Bson>>(),
            ),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MongoTestRunner;
    use serde_json::Value;

    #[test]
    fn normalize_doc_string_quotes_dollar_prefixed_keys() {
        let doc = r#"{ $set: { name: "a" }, age: 1 }"#;
        let normalized = MongoTestRunner::normalize_doc_string(doc);
        assert_eq!(normalized, r#"{ "$set": { "name": "a" }, "age": 1 }"#);
        serde_json::from_str::<Value>(&normalized).unwrap();
    }

    #[test]
    fn normalize_doc_string_leaves_quoted_keys_untouched() {
        let doc = r#"{ "$inc": { "count": 1 } }"#;
        let normalized = MongoTestRunner::normalize_doc_string(doc);
        assert_eq!(normalized, doc);
        serde_json::from_str::<Value>(&normalized).unwrap();
    }

    #[test]
    fn normalize_doc_string_normalizes_numberlong_literal() {
        let doc = r#"{ _id: NumberLong(9999999), value: NumberLong("123") }"#;
        let normalized = MongoTestRunner::normalize_doc_string(doc);
        assert_eq!(normalized, r#"{ "_id": 9999999, "value": 123 }"#);
        serde_json::from_str::<Value>(&normalized).unwrap();
    }
}
