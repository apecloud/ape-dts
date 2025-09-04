#[allow(dead_code)]
pub enum ConstraintTypeEnum {
    // p -> 112;u -> 117;f -> 102;c -> 99
    Primary,
    Unique,
    Foreign,
    Check,
}

impl ConstraintTypeEnum {
    pub fn to_charval(&self) -> Option<String> {
        Some(match self {
            ConstraintTypeEnum::Primary => String::from("112"),
            ConstraintTypeEnum::Unique => String::from("117"),
            ConstraintTypeEnum::Foreign => String::from("102"),
            ConstraintTypeEnum::Check => String::from("99"),
        })
    }

    pub fn to_str(&self) -> Option<String> {
        Some(match self {
            ConstraintTypeEnum::Primary => String::from("p"),
            ConstraintTypeEnum::Unique => String::from("u"),
            ConstraintTypeEnum::Foreign => String::from("f"),
            ConstraintTypeEnum::Check => String::from("c"),
        })
    }
}
