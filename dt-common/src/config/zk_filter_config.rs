#[derive(Clone, Default, Hash)]
pub struct ZkFilterConfig {
    pub do_paths: String,
    pub ignore_paths: String,
    pub include_ephemeral: bool,
}
