#[derive(Debug, Clone)]
pub struct PgUdf {
    pub schema_name: String,
    pub function_name: String,
    pub lanname: String,
    pub create_statement: String,
}

#[derive(Debug, Clone)]
pub struct PgUdt {
    pub schema_name: String,
    pub typ_name: String,
    pub typ_type: PgUdtType,
    pub create_statement: String,
}

#[derive(Debug, Clone)]
pub enum PgUdtType {
    Composite,
    Enum,
    Range,
    Domain,
}
