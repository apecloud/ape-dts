use crate::rdb_filter::RdbFilter;

use crate::meta::struct_meta::structure::{
    rbac::PgPrivilege, rbac::PgRole, rbac::PgRoleMember, structure_type::StructureType,
};

#[derive(Debug, Clone)]
pub struct PgCreateRbacStatement {
    pub roles: Vec<PgRole>,
    pub members: Vec<PgRoleMember>,
    pub privileges: Vec<PgPrivilege>,
}

impl PgCreateRbacStatement {
    pub fn to_sqls(&self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        let mut sqls = Vec::new();
        if filter.filter_structure(&StructureType::Rbac) {
            return Ok(sqls);
        }

        let mut role_map: std::collections::HashMap<String, &PgRole> =
            std::collections::HashMap::new();
        for role in &self.roles {
            role_map.insert(role.name.clone(), role);

            let mut sql = format!("CREATE ROLE \"{}\"", role.name);
            let mut options = Vec::new();

            if role.rol_super {
                options.push("SUPERUSER".to_string());
            }

            if role.rol_createdb {
                options.push("CREATEDB".to_string());
            }

            if role.rol_createrole {
                options.push("CREATEROLE".to_string());
            }

            if !role.rol_inherit {
                // inherit is default
                options.push("NOINHERIT".to_string());
            }

            if role.rol_can_login {
                options.push("LOGIN".to_string());
            }

            if role.rol_replication {
                options.push("REPLICATION".to_string());
            }

            if role.rol_by_passrls {
                options.push("BYPASSRLS".to_string());
            }

            if !role.rol_conn_limit.is_empty() && role.rol_conn_limit != "-1" {
                options.push(format!("CONNECTION LIMIT {}", role.rol_conn_limit));
            }

            if !role.password.is_empty() {
                options.push(format!("PASSWORD '{}'", role.password));
            }

            if !role.rol_valid_until.is_empty() {
                options.push(format!("VALID UNTIL '{}'", role.rol_valid_until));
            }

            if !options.is_empty() {
                sql = format!("{} WITH {}", sql, options.join(" "));
            }

            sqls.push((String::new(), sql));

            if !role.rol_configs.is_empty() {
                for config in &role.rol_configs {
                    if let Some(pos) = config.find('=') {
                        let param = &config[..pos].trim();
                        let value = &config[pos + 1..].trim();
                        if param.is_empty() || value.is_empty() {
                            continue;
                        }
                        let alter_sql =
                            format!("ALTER ROLE \"{}\" SET {} TO '{}'", role.name, param, value);
                        sqls.push((String::new(), alter_sql));
                    }
                }
            }
        }

        for member in &self.members {
            if role_map.contains_key(&member.member) {
                let mut sql = format!("GRANT \"{}\" TO \"{}\"", member.role, member.member);
                if member.admin_option {
                    sql = format!("{} WITH ADMIN OPTION", sql);
                }
                sqls.push((String::new(), sql));
            }
        }

        for privilege in &self.privileges {
            if !privilege.origin.is_empty() {
                sqls.push((String::new(), privilege.origin.clone()));
            }
        }
        Ok(sqls)
    }
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;

    use super::*;
    use crate::config::config_enums::DbType;
    use crate::meta::struct_meta::structure::rbac::{PgPrivilege, PgRole, PgRoleMember};
    use crate::rdb_filter::RdbFilter;
    use std::collections::{HashMap, HashSet};

    fn build_filter() -> RdbFilter {
        let mut filter = RdbFilter {
            db_type: DbType::Pg,
            do_structures: HashSet::new(),
            cache: DashMap::new(),
            do_schemas: HashSet::new(),
            ignore_schemas: HashSet::new(),
            do_tbs: HashSet::new(),
            ignore_tbs: HashSet::new(),
            ignore_cols: HashMap::new(),
            do_events: HashSet::new(),
            do_dcls: HashSet::new(),
            do_ddls: HashSet::new(),
            ignore_cmds: HashSet::new(),
            where_conditions: HashMap::new(),
        };
        filter.do_structures.insert(StructureType::Rbac.to_string());
        filter
    }

    #[test]
    fn test_to_sqls_basic_role() -> anyhow::Result<()> {
        let role = PgRole {
            name: "test_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role],
            members: vec![],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 1);
        assert_eq!(sqls[0].1, "CREATE ROLE \"test_role\" WITH LOGIN");

        Ok(())
    }

    #[test]
    fn test_to_sqls_role_with_options() -> anyhow::Result<()> {
        // role with validate time
        let role = PgRole {
            name: "admin_role".to_string(),
            password: "secure_password".to_string(),
            rol_super: true,
            rol_inherit: true,
            rol_createrole: true,
            rol_createdb: true,
            rol_can_login: true,
            rol_replication: true,
            rol_conn_limit: "10".to_string(),
            rol_valid_until: "2025-12-31".to_string(),
            rol_by_passrls: true,
            rol_configs: vec![],
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role],
            members: vec![],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0].1,
            "CREATE ROLE \"admin_role\" WITH SUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 10 PASSWORD 'secure_password' VALID UNTIL '2025-12-31'"
        );

        Ok(())
    }

    #[test]
    fn test_to_sqls_role_with_configs() -> anyhow::Result<()> {
        // role with configs
        let role = PgRole {
            name: "config_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![
                "search_path=public".to_string(),
                "statement_timeout=5000".to_string(),
            ],
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role],
            members: vec![],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 3);
        assert_eq!(sqls[0].1, "CREATE ROLE \"config_role\" WITH LOGIN");
        assert_eq!(
            sqls[1].1,
            "ALTER ROLE \"config_role\" SET search_path TO 'public'"
        );
        assert_eq!(
            sqls[2].1,
            "ALTER ROLE \"config_role\" SET statement_timeout TO '5000'"
        );

        Ok(())
    }

    #[test]
    fn test_to_sqls_multiple_roles() -> anyhow::Result<()> {
        // multiple roles
        let role1 = PgRole {
            name: "role1".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let role2 = PgRole {
            name: "role2".to_string(),
            password: "pwd2".to_string(),
            rol_super: false,
            rol_inherit: false,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: false,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role1, role2],
            members: vec![],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 2);
        assert_eq!(sqls[0].1, "CREATE ROLE \"role1\" WITH LOGIN");
        assert_eq!(
            sqls[1].1,
            "CREATE ROLE \"role2\" WITH NOINHERIT PASSWORD 'pwd2'"
        );

        Ok(())
    }

    #[test]
    fn test_to_sqls_role_members() -> anyhow::Result<()> {
        // role members
        let role1 = PgRole {
            name: "parent_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: false,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let role2 = PgRole {
            name: "child_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let member1 = PgRoleMember {
            role: "parent_role".to_string(),
            member: "child_role".to_string(),
            admin_option: false,
        };
        let member2 = PgRoleMember {
            role: "parent_role".to_string(),
            member: "child_role".to_string(),
            admin_option: true,
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role1, role2],
            members: vec![member1, member2],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 4);
        assert_eq!(sqls[0].1, "CREATE ROLE \"parent_role\"");
        assert_eq!(sqls[1].1, "CREATE ROLE \"child_role\" WITH LOGIN");
        assert_eq!(sqls[2].1, "GRANT \"parent_role\" TO \"child_role\"");
        assert_eq!(
            sqls[3].1,
            "GRANT \"parent_role\" TO \"child_role\" WITH ADMIN OPTION"
        );
        Ok(())
    }

    #[test]
    fn test_to_sqls_privileges() -> anyhow::Result<()> {
        // privileges
        let role = PgRole {
            name: "test_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: true,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let privilege = PgPrivilege {
            origin: "GRANT SELECT ON TABLE public.test_table TO \"test_role\"".to_string(),
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role],
            members: vec![],
            privileges: vec![privilege],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 2);
        assert_eq!(sqls[0].1, "CREATE ROLE \"test_role\" WITH LOGIN");
        assert_eq!(
            sqls[1].1,
            "GRANT SELECT ON TABLE public.test_table TO \"test_role\""
        );

        Ok(())
    }

    #[test]
    fn test_to_sqls_invalid_member() -> anyhow::Result<()> {
        // invalid role member
        let role = PgRole {
            name: "existing_role".to_string(),
            password: "".to_string(),
            rol_super: false,
            rol_inherit: false,
            rol_createrole: false,
            rol_createdb: false,
            rol_can_login: true,
            rol_replication: false,
            rol_conn_limit: "".to_string(),
            rol_valid_until: "".to_string(),
            rol_by_passrls: false,
            rol_configs: vec![],
        };

        let member = PgRoleMember {
            role: "existing_role".to_string(),
            member: "non_existing_role".to_string(),
            admin_option: false,
        };

        let statement = PgCreateRbacStatement {
            roles: vec![role],
            members: vec![member],
            privileges: vec![],
        };

        let filter = build_filter();
        let sqls = statement.to_sqls(&filter)?;

        assert_eq!(sqls.len(), 1);
        assert_eq!(
            sqls[0].1,
            "CREATE ROLE \"existing_role\" WITH NOINHERIT LOGIN"
        );

        Ok(())
    }
}
