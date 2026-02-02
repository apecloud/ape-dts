use std::fmt;

use fake::{Dummy, Fake, Faker};

pub struct TypeUtil {}

impl TypeUtil {
    pub fn fake_str<T: Dummy<Faker> + fmt::Display>() -> String {
        Faker.fake::<T>().to_string()
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_serde_json() {
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<serde_json::Value>())
        }
    }

    #[test]
    fn test_uuid() {
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<Uuid>())
        }
    }

    #[test]
    fn test_decimal() {
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<rust_decimal::Decimal>())
        }
    }
}
