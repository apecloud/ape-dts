use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
};

use dt_common::config::{
    extractor_config::ExtractorConfig, ini_loader::IniLoader, resumer_config::ResumerConfig,
    sinker_config::SinkerConfig, task_config::TaskConfig,
};

pub struct TestConfigUtil {}

const EXTRACTOR: &str = "extractor";
const SINKER: &str = "sinker";
const CHECKER_OUTPUT: &str = "checker_output";
const RUNTIME: &str = "runtime";
const RESUMER: &str = "resumer";
const PROCESSOR: &str = "processor";
const TEST_PROJECT: &str = "dt-tests";

#[allow(dead_code)]
impl TestConfigUtil {
    pub fn get_project_root() -> String {
        project_root::get_project_root()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    pub fn get_absolute_path(relative_path: &str) -> String {
        format!(
            "{}/{}/tests/{}",
            project_root::get_project_root().unwrap().to_str().unwrap(),
            TEST_PROJECT,
            relative_path
        )
    }

    // result: (absolute_sub_path, sub_path_dir_name)
    pub fn get_absolute_sub_dir(relative_dir: &str) -> Vec<(String, String)> {
        let mut result_dir: Vec<(String, String)> = vec![];

        let absolute_dir = TestConfigUtil::get_absolute_path(relative_dir);
        let path = PathBuf::from(absolute_dir.as_str());

        let entries = fs::read_dir(path).unwrap();
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let sub_path_dir = entry.file_name().to_string_lossy().to_string();
                result_dir.push((format!("{}/{}", absolute_dir, sub_path_dir), sub_path_dir));
            }
        }

        result_dir
    }

    pub fn update_task_config_from_env(src_task_config_file: &str, dst_task_config_file: &str) {
        let env_local_file = TestConfigUtil::get_absolute_path(".env.local");
        let env_file = TestConfigUtil::get_absolute_path(".env");

        // environment variable settings in .env.local have higher priority
        if fs::metadata(&env_local_file).is_ok() {
            dotenv::from_path(&env_local_file).unwrap();
        }
        dotenv::from_path(&env_file).unwrap();

        let mut update_configs = Vec::new();
        let ini = IniLoader::new(src_task_config_file)
            .expect("source test task config should load")
            .ini;
        for (section, kvs) in ini.get_map().unwrap() {
            for (k, v) in kvs.iter() {
                if v.is_none() {
                    continue;
                }
                for (env_k, env_v) in env::vars() {
                    if *v.as_ref().unwrap() == format!("{{{}}}", env_k) {
                        update_configs.push((section.clone(), k.clone(), env_v.clone()));
                        break;
                    }
                }
            }
        }

        TestConfigUtil::update_task_config(
            src_task_config_file,
            dst_task_config_file,
            &update_configs,
        );
    }

    pub fn update_file_paths_in_task_config(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        project_root: &str,
    ) {
        let ini = IniLoader::new(src_task_config_file)
            .expect("source test task config should load")
            .ini;
        let mut processor_updates = Vec::new();
        if let Some(lua_code_file) = ini.get(PROCESSOR, "lua_code_file") {
            if !lua_code_file.is_empty() {
                let lua_code_file = Path::new(&lua_code_file);
                let absolute_path = if lua_code_file.is_absolute() {
                    lua_code_file.to_path_buf()
                } else {
                    Path::new(project_root).join(lua_code_file)
                };
                processor_updates.push((
                    PROCESSOR.to_string(),
                    "lua_code_file".to_string(),
                    absolute_path.to_string_lossy().into_owned(),
                ));
            }
        }
        Self::update_task_config(
            src_task_config_file,
            dst_task_config_file,
            &processor_updates,
        );

        let config = TaskConfig::new(dst_task_config_file).unwrap();
        let mut update_configs = Vec::new();

        // runtime/log4rs_file
        let log4rs_file = format!("{}/{}", project_root, config.runtime.log4rs_file);
        update_configs.push((RUNTIME.to_string(), "log4rs_file".to_string(), log4rs_file));

        // runtime/log_dir
        let log_dir = format!("{}/{}", project_root, config.runtime.log_dir);
        update_configs.push((RUNTIME.to_string(), "log_dir".to_string(), log_dir.clone()));

        // resumer/resume_log_dir
        if let ResumerConfig::FromLog {
            log_dir,
            config_file,
        } = config.resumer
        {
            let resume_log_dir = format!("{}/{}", project_root, log_dir);
            update_configs.push((RESUMER.to_string(), "log_dir".to_string(), resume_log_dir));
            // resumer/resume_config_file
            let resume_config_file = format!("{}/{}", project_root, config_file);
            update_configs.push((
                RESUMER.to_string(),
                "config_file".to_string(),
                resume_config_file,
            ));
        }

        // extractor/check_log_dir
        match config.extractor {
            ExtractorConfig::MysqlCheck { check_log_dir, .. }
            | ExtractorConfig::PgCheck { check_log_dir, .. }
            | ExtractorConfig::MongoCheck { check_log_dir, .. } => {
                let extractor_check_log_dir = format!("{}/{}", project_root, check_log_dir);
                update_configs.push((
                    EXTRACTOR.to_string(),
                    "check_log_dir".to_string(),
                    extractor_check_log_dir,
                ));
            }

            ExtractorConfig::RedisSnapshotFile { file_path } => {
                let file_path = format!("{}/{}", project_root, file_path);
                update_configs.push((EXTRACTOR.to_string(), "file_path".to_string(), file_path));
            }

            _ => {}
        }

        if let Some(checker) = &config.checker {
            let checker_check_log_dir = if !checker.log_dir().is_empty() {
                format!("{}/{}", project_root, checker.log_dir())
            } else {
                format!("{}/check", log_dir)
            };
            update_configs.push((
                CHECKER_OUTPUT.to_string(),
                "check_log_dir".to_string(),
                checker_check_log_dir,
            ));
        }

        match config.sinker {
            // sinker/statistic_log_dir
            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => {
                let sinker_statistic_log_dir = if !statistic_log_dir.is_empty() {
                    format!("{}/{}", project_root, statistic_log_dir)
                } else {
                    format!("{}/statistic", log_dir)
                };
                update_configs.push((
                    SINKER.to_string(),
                    "statistic_log_dir".to_string(),
                    sinker_statistic_log_dir,
                ));
            }

            _ => {}
        }

        TestConfigUtil::update_task_config(
            dst_task_config_file,
            dst_task_config_file,
            &update_configs,
        );
    }

    pub fn update_task_config(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        config: &[(String, String, String)],
    ) {
        let mut ini = IniLoader::new(src_task_config_file)
            .expect("source test task config should load")
            .ini;
        for (section, key, value) in config.iter() {
            ini.set(section, key, Some(value.to_string()));
        }

        let path = Path::new(&dst_task_config_file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        File::create(dst_task_config_file)
            .unwrap()
            .set_len(0)
            .unwrap();
        ini.write(dst_task_config_file).unwrap();
    }

    pub fn update_task_config_2<'a>(
        src_task_config_file: &str,
        dst_task_config_file: &str,
        config: &[(&'a str, &'a str, &'a str)],
    ) {
        let config: Vec<(String, String, String)> = config
            .iter()
            .map(|i| (i.0.to_string(), i.1.to_string(), i.2.to_string()))
            .collect();
        Self::update_task_config(src_task_config_file, dst_task_config_file, &config);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn resolves_processor_path_before_loading_task_config() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let project_root =
            env::temp_dir().join(format!("ape-dts-test-config-{}-{unique}", process::id()));
        let lua_code_file = project_root.join("fixtures/lua_code.lua");
        let src_config_file = project_root.join("task_config.ini");
        let dst_config_file = project_root.join("tmp/task_config.ini");

        fs::create_dir_all(lua_code_file.parent().unwrap()).unwrap();
        fs::write(&lua_code_file, "return {}\n").unwrap();
        fs::write(
            &src_config_file,
            r#"[extractor]
db_type=mysql
extract_type=snapshot
url=mysql://127.0.0.1:3306/test

[sinker]
db_type=mysql
sink_type=dummy

[parallelizer]
parallel_type=snapshot
parallel_size=1

[pipeline]
buffer_size=1
checkpoint_interval_secs=1

[processor]
lua_code_file=fixtures/lua_code.lua
"#,
        )
        .unwrap();

        TestConfigUtil::update_file_paths_in_task_config(
            src_config_file.to_str().unwrap(),
            dst_config_file.to_str().unwrap(),
            project_root.to_str().unwrap(),
        );

        let config = TaskConfig::new(dst_config_file.to_str().unwrap()).unwrap();
        let processor = config.processor.unwrap();
        assert_eq!(Path::new(&processor.lua_code_file), lua_code_file);
        assert_eq!(processor.lua_code, "return {}\n");

        fs::remove_dir_all(project_root).unwrap();
    }
}
