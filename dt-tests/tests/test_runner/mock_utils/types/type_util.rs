use std::fmt;

use fake::{Dummy, Fake, Faker};

use crate::test_runner::mock_utils::random::Random;

pub struct TypeUtil {}

impl TypeUtil {
    pub fn fake_str<T: Dummy<Faker> + fmt::Display>(rand: &mut Random) -> String {
        Faker.fake_with_rng::<T, _>(&mut rand.rng).to_string()
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_serde_json() {
        let mut rand = Random::new(Some(777));
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<serde_json::Value>(&mut rand));
        }
    }

    #[test]
    fn test_uuid() {
        let mut rand = Random::new(Some(777));
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<Uuid>(&mut rand));
        }
    }

    #[test]
    fn test_decimal() {
        let mut rand = Random::new(Some(777));
        for _i in 0..10 {
            println!("{}", TypeUtil::fake_str::<rust_decimal::Decimal>(&mut rand));
        }
    }
}
