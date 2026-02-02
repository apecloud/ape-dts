use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};

/// PostgreSQL bytea: binary data (byte array)
/// Output format: hex string (e.g., "deadbeef")
pub struct Bytea(pub Vec<u8>);

impl Bytea {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }
}

impl std::fmt::Display for Bytea {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl RandomValue for Bytea {
    fn next_value(_random: &mut Random) -> String {
        // Generate random bytes (4-16 bytes)
        let len = (_random.next_u8() % 13 + 4) as usize;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            bytes.push(_random.next_u8());
        }
        Bytea::new(bytes).to_string()
    }
}

impl ConstantValues for Bytea {
    fn next_values() -> Vec<String> {
        [
            // --- 1. Empty & Basic ---
            r#""#,   // Empty Binary (0 bytes)
            r#"00"#, // Null Byte (0x00)
            r#"ff"#, // Max Byte Value (0xFF)
            // --- 2. SQL Special Characters (Hex Encoded) ---
            // 27 = Single Quote ('), 5c = Backslash (\)
            // Encoded to ensure safety, but represents dangerous SQL chars
            r#"275c"#,
            // --- 3. Invalid UTF-8 Sequences ---
            // c0, c1 are invalid in UTF-8. Ensures data is treated as binary.
            r#"c0c1"#,
            // --- 4. Magic Numbers / File Signatures ---
            r#"89504e470d0a1a0a"#, // PNG Header (Common image test)
            r#"cafebabe"#,         // Java Class Magic / Mach-O Binary
            // --- 5. Pattern Filling ---
            r#"00000000"#, // Sequence of Zeros
            r#"ffffffff"#, // Sequence of Ones
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
            let bytea = Bytea::next_value(&mut random);
            println!("Bytea: {}", bytea);
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Bytea constants: {:?}", Bytea::next_values());
    }

    #[test]
    fn test_bytea_display() {
        let bytea = Bytea::new(vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(bytea.to_string(), "deadbeef");

        let empty = Bytea::new(vec![]);
        assert_eq!(empty.to_string(), "");

        let single = Bytea::new(vec![0xff]);
        assert_eq!(single.to_string(), "ff");
    }
}
