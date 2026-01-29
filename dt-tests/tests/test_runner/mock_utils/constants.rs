pub struct Constants {}

impl Constants {
    const NEXT_I8: &[i8] = &[i8::MIN, 0, i8::MAX];

    const NEXT_U8: &[u8] = &[u8::MIN, u8::MAX];

    const NEXT_I16: &[i16] = &[i16::MIN, 0, i16::MAX];

    const NEXT_U16: &[u16] = &[u16::MIN, u16::MAX];

    const NEXT_I32: &[i32] = &[i32::MIN, -1, 0, 1, i32::MAX];

    const NEXT_U32: &[u32] = &[u32::MIN, 1, u32::MAX];

    const NEXT_I64: &[i64] = &[i64::MIN, -1, 0, 1, i64::MAX];

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

    #[inline]
    pub fn next_i8() -> &'static [i8] {
        Self::NEXT_I8
    }

    #[inline]
    pub fn next_u8() -> &'static [u8] {
        Self::NEXT_U8
    }

    #[inline]
    pub fn next_i16() -> &'static [i16] {
        Self::NEXT_I16
    }

    #[inline]
    pub fn next_u16() -> &'static [u16] {
        Self::NEXT_U16
    }

    #[inline]
    pub fn next_i32() -> &'static [i32] {
        Self::NEXT_I32
    }

    #[inline]
    pub fn next_u32() -> &'static [u32] {
        Self::NEXT_U32
    }

    #[inline]
    pub fn next_i64() -> &'static [i64] {
        Self::NEXT_I64
    }

    #[inline]
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
