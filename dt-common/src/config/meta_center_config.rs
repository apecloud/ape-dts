use crate::config::connection_auth_config::ConnectionAuthConfig;

use super::config_enums::ConflictPolicyEnum;

#[derive(Clone, Debug, Default)]
pub enum MetaCenterConfig {
    #[default]
    Basic,
    MySqlDbEngine {
        url: String,
        connection_auth: ConnectionAuthConfig,
        // ddl_conflict_policy:
        //   ignore: when sinker execute DDL failed, will ignore the error
        //   [default] interrupt: when sinker execute DDL failed, will interrupt the processor with this error
        ddl_conflict_policy: ConflictPolicyEnum,
    },
}
