use dt_common::{config::task_config::TaskConfig, utils::time_util::TimeUtil};
use dt_connector::data_marker::DataMarker;
use dt_task::task_runner::TaskRunner;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
};
use tokio::task::JoinHandle;

use crate::test_config_util::TestConfigUtil;

#[derive(Default)]
pub struct BaseTestRunner {
    pub test_dir: String,
    pub task_config_file: String,
    pub struct_task_config_file: String,
    pub src_test_sqls: Vec<String>,
    pub dst_test_sqls: Vec<String>,
    pub src_prepare_sqls: Vec<String>,
    pub dst_prepare_sqls: Vec<String>,
    pub src_clean_sqls: Vec<String>,
    pub dst_clean_sqls: Vec<String>,
    pub meta_center_prepare_sqls: Vec<String>,
}

#[allow(dead_code)]
impl BaseTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let test_dir = TestConfigUtil::get_absolute_path(relative_test_dir);

        let dst_task_config_file =
            Self::generate_tmp_task_config_file(relative_test_dir, "task_config.ini");
        let dst_struct_task_config_file =
            Self::generate_tmp_task_config_file(relative_test_dir, "struct_task_config.ini");

        let (
            src_test_sqls,
            dst_test_sqls,
            src_prepare_sqls,
            dst_prepare_sqls,
            src_clean_sqls,
            dst_clean_sqls,
            meta_center_prepare_sqls,
        ) = Self::load_sqls(&test_dir);

        Ok(Self {
            task_config_file: dst_task_config_file,
            struct_task_config_file: dst_struct_task_config_file,
            test_dir,
            src_test_sqls,
            dst_test_sqls,
            src_prepare_sqls,
            dst_prepare_sqls,
            src_clean_sqls,
            dst_clean_sqls,
            meta_center_prepare_sqls,
        })
    }

    pub fn generate_tmp_task_config_file(
        relative_test_dir: &str,
        task_config_file: &str,
    ) -> String {
        let project_root = TestConfigUtil::get_project_root();
        let test_dir = TestConfigUtil::get_absolute_path(relative_test_dir);
        let src_task_config_file = format!("{}/{}", test_dir, task_config_file);

        if !Self::check_path_exists(&src_task_config_file) {
            return String::new();
        }

        let tmp_dir = format!("{}/tmp/{}", project_root, relative_test_dir);
        let dst_task_config_file = format!("{}/{}", tmp_dir, task_config_file);

        // update relative path to absolute path in task_config.ini
        TestConfigUtil::update_file_paths_in_task_config(
            &src_task_config_file,
            &dst_task_config_file,
            &project_root,
        );

        // update extractor / sinker urls from .env
        TestConfigUtil::update_task_config_from_env(&dst_task_config_file, &dst_task_config_file);
        dst_task_config_file
    }

    pub fn get_config(&self) -> TaskConfig {
        TaskConfig::new(&self.task_config_file).unwrap()
    }

    pub async fn start_task(&self) -> anyhow::Result<()> {
        TaskRunner::new(&self.task_config_file)?.start_task().await
    }

    pub async fn spawn_task(&self) -> anyhow::Result<JoinHandle<()>> {
        let task_runner = TaskRunner::new(&self.task_config_file)?;
        let task = tokio::spawn(async move { task_runner.start_task().await.unwrap() });
        Ok(task)
    }

    pub async fn abort_task(&self, task: &JoinHandle<()>) -> anyhow::Result<()> {
        task.abort();
        while !task.is_finished() {
            TimeUtil::sleep_millis(1).await;
        }
        Ok(())
    }

    pub async fn wait_task_finish(&self, task: &JoinHandle<()>) -> anyhow::Result<()> {
        while !task.is_finished() {
            TimeUtil::sleep_millis(1).await;
        }
        Ok(())
    }

    pub fn load_file(file_path: &str) -> Vec<String> {
        if fs::metadata(file_path).is_err() {
            return Vec::new();
        }

        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        for line in reader.lines().map_while(Result::ok) {
            lines.push(line);
        }
        lines
    }

    #[allow(clippy::type_complexity)]
    fn load_sqls(
        test_dir: &str,
    ) -> (
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
        Vec<String>,
    ) {
        let load = |sql_file: &str| -> Vec<String> {
            let full_sql_path = format!("{}/{}", test_dir, sql_file);
            if !Self::check_path_exists(&full_sql_path) {
                return Vec::new();
            }
            Self::load_sql_file(&full_sql_path)
        };

        (
            load("src_test.sql"),
            load("dst_test.sql"),
            load("src_prepare.sql"),
            load("dst_prepare.sql"),
            load("src_clean.sql"),
            load("dst_clean.sql"),
            load("meta_center_prepare.sql"),
        )
    }

    /// Simplified SQL parser based on line aggregation.
    /// 1. Handles multi-line SQLs automatically.
    /// 2. Handles standard SQLs split across lines (e.g. INSERT VALUES ...) by waiting for a semicolon ';'.
    /// 3. Ignores lines starting with '--'.
    fn load_sql_file(sql_file: &str) -> Vec<String> {
        let lines = Self::load_file(sql_file);
        let mut sqls = Vec::new();
        let mut current_sql = String::new();
        let mut in_backtick_block = false;
        let mut dollar_tag: Option<String> = None;

        for line in lines {
            let trimmed_line = line.trim();

            // 1. Handle ``` wrapped blocks
            if trimmed_line.starts_with("```") {
                if in_backtick_block {
                    in_backtick_block = false;
                    if !current_sql.is_empty() {
                        sqls.push(Self::flush_sql(&mut current_sql));
                    }
                } else {
                    in_backtick_block = true;
                    current_sql.clear();
                }
                continue;
            }

            // 2. In ``` block: keep everything untouched
            if in_backtick_block {
                current_sql.push_str(&line);
                current_sql.push('\n');
                continue;
            }

            // 3. Inside PostgreSQL dollar-quoted blocks, ignore inner semicolons
            if let Some(tag) = &dollar_tag {
                current_sql.push_str(&line);
                current_sql.push('\n');

                if trimmed_line.contains(tag) {
                    dollar_tag = None;
                    if trimmed_line.ends_with(';') {
                        sqls.push(Self::flush_sql(&mut current_sql));
                    }
                }
                continue;
            }

            // 4. Normal mode: strip inline comments
            let line_content = if let Some(idx) = line.find("--") {
                &line[..idx]
            } else {
                &line
            };

            let trimmed_content = line_content.trim();

            if trimmed_content.is_empty() {
                continue;
            }

            if trimmed_content.starts_with("use ") {
                if !current_sql.trim().is_empty() {
                    sqls.push(Self::flush_sql(&mut current_sql));
                }
                let use_stmt = trimmed_content.trim_end_matches(';').to_string();
                sqls.push(use_stmt);
                continue;
            }

            // Detect start of dollar-quoted blocks like $$ ... $$ or $BODY$ ... $BODY$
            if let Some(tag) = Self::extract_dollar_tag(trimmed_content) {
                let tag_count = trimmed_content.matches(&tag).count();
                current_sql.push_str(trimmed_content);
                current_sql.push('\n');

                if tag_count >= 2 {
                    if trimmed_content.ends_with(';') {
                        sqls.push(Self::flush_sql(&mut current_sql));
                    }
                    continue;
                }

                dollar_tag = Some(tag);
                continue;
            }

            current_sql.push_str(trimmed_content);
            current_sql.push(' ');

            // If this line ends with a semicolon, the statement is finished
            if trimmed_content.ends_with(';') {
                sqls.push(Self::flush_sql(&mut current_sql));
            }
        }

        // Push any remaining SQL (e.g., file ends without semicolon)
        if !current_sql.trim().is_empty() {
            sqls.push(Self::flush_sql(&mut current_sql));
        }

        sqls
    }

    fn flush_sql(current_sql: &mut String) -> String {
        let sql = current_sql.trim().trim_end_matches(';').to_string();
        current_sql.clear();
        sql
    }

    fn extract_dollar_tag(line: &str) -> Option<String> {
        let mut start = None;
        for (idx, ch) in line.char_indices() {
            if ch == '$' {
                if let Some(s) = start {
                    return Some(line[s..=idx].to_string());
                }
                start = Some(idx);
            }
        }
        None
    }

    pub fn check_path_exists(file: &str) -> bool {
        fs::metadata(file).is_ok()
    }

    pub fn get_data_marker(&self) -> Option<DataMarker> {
        let config = self.get_config();
        if let Some(data_marker_config) = config.data_marker {
            let data_marker =
                DataMarker::from_config(&data_marker_config, &config.extractor_basic.db_type)
                    .unwrap();
            return Some(data_marker);
        }
        None
    }
}
