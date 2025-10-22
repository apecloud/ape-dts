use std::collections::HashMap;

use dt_common::meta::mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey};
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
    bson::{doc, Document},
    options::FindOptions,
    Client,
};
use regex::Regex;
use sqlx::types::chrono::Utc;

use crate::test_config_util::TestConfigUtil;

use super::base_test_runner::BaseTestRunner;

pub struct MongoTestRunner {
    pub base: BaseTestRunner,
    src_mongo_client: Option<Client>,
    dst_mongo_client: Option<Client>,
    router: RdbRouter,
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
        match config.extractor {
            ExtractorConfig::MongoSnapshot { url, app_name, .. }
            | ExtractorConfig::MongoCdc { url, app_name, .. }
            | ExtractorConfig::MongoCheck { url, app_name, .. } => {
                src_mongo_client = Some(
                    TaskUtil::create_mongo_client(&url, &app_name, 10)
                        .await
                        .unwrap(),
                );
            }
            _ => {}
        }

        match config.sinker {
            SinkerConfig::Mongo { url, app_name, .. }
            | SinkerConfig::MongoCheck { url, app_name, .. } => {
                dst_mongo_client = Some(
                    TaskUtil::create_mongo_client(&url, &app_name, 10)
                        .await
                        .unwrap(),
                );
            }
            _ => {}
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

    pub async fn execute_test_sqls(&self) -> anyhow::Result<()> {
        let sqls = MongoTestRunner::slice_sqls_by_db(&self.base.src_test_sqls);
        for (db, sqls) in sqls.iter() {
            self.execute_dmls(self.src_mongo_client.as_ref().unwrap(), db, sqls)
                .await
                .unwrap();
        }

        let sqls = MongoTestRunner::slice_sqls_by_db(&self.base.dst_test_sqls);
        for (db, sqls) in sqls.iter() {
            self.execute_dmls(self.dst_mongo_client.as_ref().unwrap(), db, sqls)
                .await
                .unwrap();
        }
        Ok(())
    }

    async fn execute_ddls(&self, client: &Client, db: &str, sqls: &[String]) -> anyhow::Result<()> {
        for sql in sqls.iter() {
            if sql.contains("dropDatabase") {
                self.execute_drop_database(client, db).await.unwrap();
            } else if sql.contains("drop") {
                self.execute_drop(client, db, sql).await.unwrap();
            } else if sql.contains("createCollection") {
                self.execute_create(client, db, sql).await.unwrap();
            }
        }
        Ok(())
    }

    async fn execute_dmls(&self, client: &Client, db: &str, sqls: &[String]) -> anyhow::Result<()> {
        for sql in sqls.iter() {
            if sql.contains("insert") {
                self.execute_insert(client, db, sql).await?;
            }
            if sql.contains("update") {
                self.execute_update(client, db, sql).await?;
            }
            if sql.contains("delete") {
                self.execute_delete(client, db, sql).await?;
            }
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
            .drop(None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_drop_database(&self, client: &Client, db: &str) -> anyhow::Result<()> {
        client.database(db).drop(None).await.unwrap();
        Ok(())
    }

    async fn execute_create(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r#"db.createCollection\("(\w+)"\)"#).unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();

        client
            .database(db)
            .create_collection(tb, None)
            .await
            .unwrap();
        Ok(())
    }

    async fn execute_insert(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        // example: db.tb_2.insertOne({ "name": "a", "age": "1" })
        let re = Regex::new(r"db.(\w+).insert(One|Many)\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc_content = cap.get(3).unwrap().as_str();

        let coll = client.database(db).collection::<Document>(tb);
        if sql.contains("insertOne") {
            let doc: Document = serde_json::from_str(doc_content).unwrap();
            coll.insert_one(doc, None).await.unwrap();
        } else {
            let docs: Vec<Document> = serde_json::from_str(doc_content).unwrap();
            coll.insert_many(docs, None).await.unwrap();
        }
        Ok(())
    }

    async fn execute_delete(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).delete(One|Many)\(([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let doc = cap.get(3).unwrap().as_str();

        let doc: Document = serde_json::from_str(doc).unwrap();
        let coll = client.database(db).collection::<Document>(tb);
        if sql.contains("deleteOne") {
            coll.delete_one(doc, None).await.unwrap();
        } else {
            coll.delete_many(doc, None).await.unwrap();
        }
        Ok(())
    }

    async fn execute_update(&self, client: &Client, db: &str, sql: &str) -> anyhow::Result<()> {
        let re = Regex::new(r"db.(\w+).update(One|Many)\(([\w\W]+),([\w\W]+)\)").unwrap();
        let cap = re.captures(sql).unwrap();
        let tb = cap.get(1).unwrap().as_str();
        let query_doc = cap.get(3).unwrap().as_str();
        let update_doc = cap.get(4).unwrap().as_str();

        let query_doc: Document = serde_json::from_str(query_doc).unwrap();
        let update_doc: Document = serde_json::from_str(update_doc).unwrap();
        let coll = client.database(db).collection::<Document>(tb);
        if sql.contains("updateOne") {
            coll.update_one(query_doc, update_doc, None).await.unwrap();
        } else {
            coll.update_many(query_doc, update_doc, None).await.unwrap();
        }
        Ok(())
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

        let (dst_db, dst_tb) = self.router.get_tb_map(db, tb);
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

    pub async fn list_tb(&self, db: &str, from: &str) -> Vec<String> {
        let client = if from == SRC {
            self.src_mongo_client.as_ref().unwrap()
        } else {
            self.dst_mongo_client.as_ref().unwrap()
        };
        client
            .database(db)
            .list_collection_names(None)
            .await
            .unwrap()
    }

    pub async fn fetch_data(&self, db: &str, tb: &str, from: &str) -> HashMap<MongoKey, Document> {
        let client = if from == SRC {
            self.src_mongo_client.as_ref().unwrap()
        } else {
            self.dst_mongo_client.as_ref().unwrap()
        };

        let collection = client.database(db).collection::<Document>(tb);
        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();
        let mut cursor = collection.find(None, find_options).await.unwrap();

        let mut results = HashMap::new();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let id = MongoKey::from_doc(&doc).unwrap();
            results.insert(id, doc);
        }
        results
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
}
