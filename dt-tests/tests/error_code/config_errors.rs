use super::{assert_error_output, config_path};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn missing_config_file_output_is_stable() {
    let path = config_path("does-not-exist.ini");
    assert_error_output(
        &path,
        &format!(
            "ERROR [CF001]: failed to open config file\n\
             PHASE: loading task configuration\n\
             DETAIL: path: {}\n\
             HINT: Provide an existing task config with --config <CONFIG> or as a positional path.",
            path.display()
        ),
    )
    .await;
}

#[tokio::test]
#[serial]
async fn missing_required_item_output_is_stable() {
    assert_error_output(
        &config_path("missing_required.ini"),
        "ERROR [CF003]: required config value is missing\n\
         PHASE: loading task configuration\n\
         DETAIL: config [extractor].db_type does not exist or is empty\n\
         HINT: Add the reported configuration item and start the task again.",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn invalid_type_output_is_stable() {
    assert_error_output(
        &config_path("invalid_type.ini"),
        "ERROR [CF002]: config value has an invalid type\n\
         PHASE: loading task configuration\n\
         DETAIL: config [extractor].batch_size=invalid can not be parsed as usize\n\
         HINT: Correct the reported configuration value and start the task again.",
    )
    .await;
}
