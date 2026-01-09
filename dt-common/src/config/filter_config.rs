#[derive(Clone, Default, Hash)]
pub struct FilterConfig {
    pub do_schemas: String,
    pub ignore_schemas: String,
    pub do_tbs: String,
    pub ignore_tbs: String,
    pub ignore_cols: String,
    pub do_events: String,
    pub do_structures: String,
    pub do_ddls: String,
    pub do_dcls: String,
    pub ignore_cmds: String,
    pub where_conditions: String,
    pub partition_cols: String,
}
