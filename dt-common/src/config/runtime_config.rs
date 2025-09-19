#[derive(Clone)]
pub struct RuntimeConfig {
    pub log_level: String,
    pub log_dir: String,
    pub log4rs_file: String,
    pub tb_parallel_size: usize,
    pub task_parallel_size: usize,
    pub tb_batch_size: usize,
    pub db_batch_size: usize,
}
