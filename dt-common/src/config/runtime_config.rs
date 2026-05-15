#[derive(Clone)]
pub struct RuntimeConfig {
    pub log_level: String,
    pub log_dir: String,
    pub log4rs_file: String,
    pub check_result_stdout_only: bool,
    pub tb_parallel_size: usize,
}
