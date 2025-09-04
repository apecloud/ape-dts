#[derive(Debug, Clone)]
pub struct PgRole {
    pub name: String,
    pub password: String,
    pub rol_super: bool,
    pub rol_inherit: bool,
    pub rol_createrole: bool,
    pub rol_createdb: bool,
    pub rol_can_login: bool,
    pub rol_replication: bool,
    pub rol_conn_limit: String,
    pub rol_valid_until: String,
    pub rol_by_passrls: bool,
    pub rol_configs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PgRoleMember {
    pub role: String,
    pub member: String,
    pub admin_option: bool,
    // Todo: inherit_option and set_option are not supported before 16.0
    // pub inherit_option: bool,
    // pub set_option: bool,
}

#[derive(Debug, Clone)]
pub struct PgPrivilege {
    pub origin: String,
}
