pub struct Constants {}

pub trait ConstantValues {
    fn next_values() -> Vec<String>;
}

impl Constants {
    const NEXT_I8: &[i8] = &[i8::MIN, 0, i8::MAX];

    #[allow(dead_code)]
    const NEXT_U8: &[u8] = &[u8::MIN, u8::MAX];

    const NEXT_I16: &[i16] = &[i16::MIN, 0, i16::MAX];

    #[allow(dead_code)]
    const NEXT_U16: &[u16] = &[u16::MIN, u16::MAX];

    const NEXT_I32: &[i32] = &[i32::MIN, -1, 0, 1, i32::MAX];

    #[allow(dead_code)]
    const NEXT_U32: &[u32] = &[u32::MIN, 1, u32::MAX];

    const NEXT_I64: &[i64] = &[i64::MIN, -1, 0, 1, i64::MAX];

    #[allow(dead_code)]
    const NEXT_U64: &[u64] = &[u64::MIN, 1, u64::MAX];

    const NEXT_F32: &[f32] = &[
        f32::MIN,
        -1.0,
        0.0,
        1.0,
        f32::MAX,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NAN,
        f32::EPSILON,
    ];

    const NEXT_F64: &[f64] = &[
        f64::MIN,
        -1.0,
        0.0,
        1.0,
        f64::MAX,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        f64::EPSILON,
    ];

    const NEXT_STR_UTF8MB4: &[&str] = &[
        // --- 1. Emptiness & Whitespace ---
        r#""#, // Empty String (Length 0)
        // r#"   "#, // Pure Whitespace (Tests trimming logic in application or char(n))
        " \t", // Mixed Space & Tab
        // --- 2. SQL Syntax & Injection Simulation ---
        // The single quote is the most dangerous character in SQL.
        r#"O'Neil"#,                    // Single Quote (Standard name case)
        r#"'"#,                         // Lone Single Quote (Syntax breaker)
        r#"value'); DROP TABLE x; --"#, // Classic SQL Injection payload
        // r#"$$"#,                        // Dollar signs (Tests conflict with Dollar Quoting)
        // --- 3. Special Characters & Formatting ---
        // Note: Postgres TEXT cannot store Null Bytes (\0).
        r#"Line1
Line2"#, // Multi-line string (Newlines \n)
        r#"C:\Windows\System32"#,       // Backslashes (Escaping hell)
        r#"<script>alert(1)</script>"#, // XSS Payload (Tests frontend rendering safety)
        // --- 4. Unicode & Encoding (UTF-8) ---
        r#"汉字"#,   // CJK Characters (3 bytes per char)
        r#"🔥🚀"#,   // Emoji (4 bytes per char, requires proper collation)
        r#"Z͑ͫ̓ͪ̂ͫ̽͏̴Iͦ͊̽̔͌ͬ͛̎Gͫ̎̚Zͧͬͪ͐Ȁ̉G̿"#, // Zalgo Text (Stacked diacritics, tests vertical rendering)
        r#"ﷺ"#,      // Single character expanded to wide glyph (Ligature)
        r#"مرحبا"#,  // RTL (Right-To-Left) text (Arabic)
        // --- 5. Length Boundaries ---
        r#"a"#, // Min Length (1 char)
        // A very long string (simulating TOAST entry point, usually > 2KB)
        // Shortened here for readability, but in practice, generate 2KB+
        r#"Lorem ipsum dolor sit amet, consectetur adipiscing elit..."#,
    ];

    const NEXT_STR_UTF8MB3: &[&str] = &[
        // --- 1. Emptiness & Whitespace ---
        r#""#,
        " \t",
        // --- 2. SQL Syntax & Injection Simulation ---
        r#"O'Neil"#,
        r#"'"#,
        r#"value'); DROP TABLE x; --"#,
        // --- 3. Special Characters & Formatting ---
        r#"Line1
Line2"#,
        r#"C:\Windows\System32"#,
        r#"<script>alert(1)</script>"#,
        // --- 4. Unicode & Encoding (UTF-8) ---
        r#"汉字"#,
        // BMP symbols that exercise emoji-like rendering while remaining valid utf8mb3.
        r#"☃"#,
        r#"★"#,
        r#"☕"#,
        r#"♥"#,
        r#"☺"#,
        r#"Z͑ͫ̓ͪ̂ͫ̽͏̴Iͦ͊̽̔͌ͬ͛̎Gͫ̎̚Zͧͬͪ͐Ȁ̉G̿"#,
        r#"ﷺ"#,
        r#"مرحبا"#,
        // --- 5. Length Boundaries ---
        r#"a"#,
        r#"Lorem ipsum dolor sit amet, consectetur adipiscing elit..."#,
    ];

    #[inline]
    pub fn next_i8() -> &'static [i8] {
        Self::NEXT_I8
    }

    #[inline]
    #[allow(dead_code)]
    pub fn next_u8() -> &'static [u8] {
        Self::NEXT_U8
    }

    #[inline]
    pub fn next_i16() -> &'static [i16] {
        Self::NEXT_I16
    }

    #[inline]
    #[allow(dead_code)]
    pub fn next_u16() -> &'static [u16] {
        Self::NEXT_U16
    }

    #[inline]
    pub fn next_i32() -> &'static [i32] {
        Self::NEXT_I32
    }

    #[inline]
    #[allow(dead_code)]
    pub fn next_u32() -> &'static [u32] {
        Self::NEXT_U32
    }

    #[inline]
    pub fn next_i64() -> &'static [i64] {
        Self::NEXT_I64
    }

    #[inline]
    #[allow(dead_code)]
    pub fn next_u64() -> &'static [u64] {
        Self::NEXT_U64
    }

    #[inline]
    pub fn next_f32() -> &'static [f32] {
        Self::NEXT_F32
    }

    #[inline]
    pub fn next_f64() -> &'static [f64] {
        Self::NEXT_F64
    }

    #[inline]
    pub fn next_str() -> &'static [&'static str] {
        Self::NEXT_STR_UTF8MB4
    }

    pub fn next_str_utf8mb4() -> Vec<String> {
        Self::NEXT_STR_UTF8MB4
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub fn next_str_utf8mb3() -> Vec<String> {
        Self::NEXT_STR_UTF8MB3
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_next_f64() {
        let vec = Constants::next_f64()
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();
        println!("f64 values: {:?}", vec);
    }

    #[test]
    fn test_text_i64() {
        let vec = Constants::next_i64()
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();
        println!("i64 values: {:?}", vec);
    }
}
