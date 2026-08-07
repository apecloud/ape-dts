use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct MssqlColType {
    pub type_name: String,
    pub max_length: i16,
    pub precision: u8,
    pub scale: u8,
    pub nullable: bool,
    pub identity: bool,
    pub computed: bool,
    pub generated_always_type: u8,
}

impl MssqlColType {
    pub fn can_be_splitted(&self) -> bool {
        todo!("mssql snapshot split type validation is not implemented")
    }

    pub fn is_integer(&self) -> bool {
        todo!("mssql integer type classification is not implemented")
    }
}
