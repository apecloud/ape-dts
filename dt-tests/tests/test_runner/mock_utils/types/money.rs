use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};
use std::fmt;

/// PostgreSQL money: currency amount with fixed fractional precision
/// Range: -92233720368547758.08 to +92233720368547758.07
/// Output format: $1,234.56 (locale-dependent)
pub struct Money(pub i64); // stored as cents (fractional digits = 2)

impl Money {
    pub const MIN: i64 = -9223372036854775808; // -92233720368547758.08 in cents
    pub const MAX: i64 = 9223372036854775807; // +92233720368547758.07 in cents

    pub fn new(cents: i64) -> Self {
        Self(cents)
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let is_negative = self.0 < 0;
        let abs_value = self.0.abs();
        let dollars = abs_value / 100;
        let cents = abs_value % 100;

        if is_negative {
            write!(f, "-{}.{:02}", dollars, cents)
        } else {
            write!(f, "{}.{:02}", dollars, cents)
        }
    }
}

impl RandomValue for Money {
    fn next_value(_random: &mut Random) -> String {
        // Generate random money value within reasonable range
        // Using i32 range to avoid overflow issues and keep values realistic
        let dollars: i64 = _random.next_i32() as i64;
        let cents: i64 = (_random.next_u8() % 100) as i64;
        let total_cents = dollars * 100 + if dollars < 0 { -cents } else { cents };
        Money::new(total_cents).to_string()
    }
}

impl ConstantValues for Money {
    fn next_values() -> Vec<String> {
        [
            "0.00",                  // zero
            "1.00",                  // one dollar
            "-1.00",                 // negative one dollar
            "0.01",                  // one cent
            "-0.01",                 // negative one cent
            "1000.00",               // thousand
            "1000000.00",            // million
            "-1000000.00",           // negative million
            "92233720368547758.07",  // max value
            "-92233720368547758.08", // min value
            "123.45",                // typical value
            "999.99",                // just under thousand
            "0.99",                  // under one dollar
            "-999999999.99",         // large negative
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_next_values() {
        let mut random = Random::new(None);

        for _ in 0..5 {
            let money = Money::next_value(&mut random);
            println!("Money: {}", money);
            // money is a decimal number with 2 fractional digits
            assert!(money.contains('.'));
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Money constants: {:?}", Money::next_values());
    }

    #[test]
    fn test_money_display() {
        assert_eq!(Money::new(0).to_string(), "0.00");
        assert_eq!(Money::new(100).to_string(), "1.00");
        assert_eq!(Money::new(-100).to_string(), "-1.00");
        assert_eq!(Money::new(12345).to_string(), "123.45");
        assert_eq!(Money::new(100000).to_string(), "1000.00");
        assert_eq!(Money::new(100000000).to_string(), "1000000.00");
        assert_eq!(Money::new(-100000000).to_string(), "-1000000.00");
    }

    #[test]
    fn test_money_boundaries() {
        // Test max value: 92233720368547758.07
        let max = Money::new(9223372036854775807);
        println!("Max money: {}", max);
        assert_eq!(max.to_string(), "92233720368547758.07");

        // Test min value: -92233720368547758.08
        let min = Money::new(-9223372036854775808);
        println!("Min money: {}", min);
        assert_eq!(min.to_string(), "-92233720368547758.08");
    }
}
