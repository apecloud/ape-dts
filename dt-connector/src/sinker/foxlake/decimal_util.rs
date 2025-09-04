use std::io::{Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use dt_common::error::Error;

const DIG_PER_DEC: usize = 9;
const COMPRESSED_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

pub struct DecimalUtil {}

impl DecimalUtil {
    /// decimal value (in mysql binlog binary format) -> string
    pub fn mysql_binlog_to_string(
        buf: &[u8],
        precision: usize,
        scale: usize,
    ) -> Result<String, Error> {
        let mut cursor = Cursor::new(buf);
        // Given a column to be DECIMAL(13,4), the numbers mean:
        // 13: precision, the maximum number of digits, the maximum precision for DECIMAL is 65.
        // 4: scale, the number of digits to the right of the decimal point.
        // 13 - 4: integral, the maximum number of digits to the left of the decimal point.
        let integral = precision - scale;

        // A decimal is stored in binlog like following:
        // ([compressed bytes, 1-4]) ([fixed bytes: 4] * n) . ([fixed bytes: 4] * n) ([compressed bytes, 1-4])
        // Both integral and scale are stored in BigEndian.
        // refer: https://github.com/mysql/mysql-server/blob/8.0/strings/decimal.cc#L1488
        // Examples:
        // DECIMAL(10,4): [3 bytes] . [2 bytes]
        // DECIMAL(18,9): [4 bytes] . [4 bytes]
        // DECIMAL(27,13): [3 bytes][4 bytes] . [4 bytes][2 bytes]
        // DECIMAL(47,25): [2 bytes][4 bytes][4 bytes] . [4 bytes][4 bytes][4 bytes]
        // DIG_PER_DEC = 9: each 4 bytes represent 9 digits in a decimal number.
        // COMPRESSED_BYTES = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]: bytes needed to compress n digits.
        let uncomp_intg = integral / DIG_PER_DEC;
        let uncomp_frac = scale / DIG_PER_DEC;
        let comp_intg = integral - (uncomp_intg * DIG_PER_DEC);
        let comp_frac = scale - (uncomp_frac * DIG_PER_DEC);

        let comp_frac_bytes = COMPRESSED_BYTES[comp_frac];
        let comp_intg_bytes = COMPRESSED_BYTES[comp_intg];

        let total_bytes = 4 * uncomp_intg + 4 * uncomp_frac + comp_frac_bytes + comp_intg_bytes;
        let mut buf = vec![0u8; total_bytes];
        cursor.read_exact(&mut buf)?;

        // handle negative
        let is_negative = (buf[0] & 0x80) == 0;
        buf[0] ^= 0x80;
        if is_negative {
            for i in 0..buf.len() {
                buf[i] ^= 0xFF;
            }
        }

        // negative sign
        let mut intg_str = String::new();
        if is_negative {
            intg_str = "-".to_string();
        }

        let mut decimal_cursor = Cursor::new(buf);
        let mut is_intg_empty = true;
        // compressed integral
        if comp_intg_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_intg_bytes)?;
            if value > 0 {
                intg_str += value.to_string().as_str();
                is_intg_empty = false;
            }
        }

        // uncompressed integral
        for _ in 0..uncomp_intg {
            let value = decimal_cursor.read_u32::<BigEndian>()?;
            if is_intg_empty {
                if value > 0 {
                    intg_str += value.to_string().as_str();
                    is_intg_empty = false;
                }
            } else {
                intg_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
            }
        }

        if is_intg_empty {
            intg_str += "0";
        }

        let mut frac_str = String::new();
        // uncompressed fractional
        for _ in 0..uncomp_frac {
            let value = decimal_cursor.read_u32::<BigEndian>()?;
            frac_str += format!("{value:0size$}", value = value, size = DIG_PER_DEC).as_str();
        }

        // compressed fractional
        if comp_frac_bytes > 0 {
            let value = decimal_cursor.read_uint::<BigEndian>(comp_frac_bytes)?;
            frac_str += format!("{value:0size$}", value = value, size = comp_frac).as_str();
        }

        if frac_str.is_empty() {
            Ok(intg_str)
        } else {
            Ok(intg_str + "." + frac_str.as_str())
        }
    }

    /// decimal value (string) -> mysql binlog binary
    pub fn string_to_mysql_binlog(
        decimal_str: &str,
        precision: usize,
        scale: usize,
    ) -> Result<Vec<u8>, Error> {
        let is_negative = decimal_str.starts_with('-');
        let (mut intg_str, frac_str) = decimal_str.split_once('.').unwrap_or((decimal_str, ""));
        if is_negative {
            intg_str = &intg_str[1..]
        };

        // len for each part of Decimal(precision, scale)
        // example: when Decimal(26, 14), decimal_str == "123456789.12345678966666", then
        // comp_intg_len == 3, uncomp_intg_len == 9, uncomp_frac_len == 9, comp_frac_len == 5
        let intg_len = precision - scale;

        let comp_intg_len = intg_len % DIG_PER_DEC;
        let uncomp_intg_len = intg_len - comp_intg_len;

        let comp_frac_len = scale % DIG_PER_DEC;
        let uncomp_frac_len = scale - comp_frac_len;

        // total bytes used Decimal(precision, scale)
        let uncomp_intg_bytes = intg_len / DIG_PER_DEC * 4;
        let uncomp_frac_bytes = scale / DIG_PER_DEC * 4;

        let comp_intg_bytes = COMPRESSED_BYTES[comp_intg_len];
        let comp_frac_bytes = COMPRESSED_BYTES[comp_frac_len];

        let total_bytes =
            4 * uncomp_intg_bytes + 4 * uncomp_frac_bytes + comp_intg_bytes + comp_frac_bytes;

        let mut buf = Vec::with_capacity(total_bytes);
        let mut cursor = Cursor::new(&mut buf);

        // comp_intg_str, uncomp_intg_str
        let (comp_intg_str, uncomp_intg_str) = if intg_str.len() > uncomp_intg_len {
            (
                &intg_str[0..intg_str.len() - uncomp_intg_len],
                &intg_str[intg_str.len() - uncomp_intg_len..],
            )
        } else {
            ("", intg_str)
        };

        // comp_frac_str, uncomp_frac_str
        let (uncomp_frac_str, comp_frac_str) = if frac_str.len() > uncomp_frac_len {
            (&frac_str[0..uncomp_frac_len], &frac_str[uncomp_frac_len..])
        } else {
            (frac_str, "")
        };

        // write comp_intg_str
        if comp_intg_bytes > 0 {
            if comp_intg_str.is_empty() {
                cursor.write_uint::<BigEndian>(0, comp_intg_bytes)?;
            } else {
                let value = comp_intg_str.parse::<u64>().unwrap();
                cursor.write_uint::<BigEndian>(value, comp_intg_bytes)?;
            }
        }

        // write uncomp_intg_str
        let uncomp_count = uncomp_intg_bytes / 4;
        if uncomp_count > 0 {
            let mut uncomp_values = vec![0; uncomp_count];

            let mut start;
            let mut end = uncomp_intg_str.len();
            for i in 0..uncomp_count {
                if end == 0 {
                    uncomp_values[uncomp_count - i - 1] = 0;
                    continue;
                }

                start = if end > DIG_PER_DEC {
                    end - DIG_PER_DEC
                } else {
                    0
                };
                let value_str = &uncomp_intg_str[start..end];
                uncomp_values[uncomp_count - i - 1] = value_str.parse::<u64>().unwrap();

                end = start;
            }

            for value in uncomp_values {
                cursor.write_uint::<BigEndian>(value, 4)?;
            }
        }

        // write uncomp_frac_str
        let uncomp_count = uncomp_frac_bytes / 4;
        if uncomp_count > 0 {
            let mut start = 0;
            let mut end;

            for _ in 0..uncomp_count {
                if start >= uncomp_frac_str.len() {
                    cursor.write_uint::<BigEndian>(0, 4)?;
                    continue;
                }

                end = if uncomp_frac_str.len() > start + DIG_PER_DEC {
                    start + DIG_PER_DEC
                } else {
                    uncomp_frac_str.len()
                };

                let value_str = &uncomp_frac_str[start..end];
                let value_str = format!("{:0<size$}", value_str, size = DIG_PER_DEC);
                let value = value_str.parse::<u64>().unwrap();
                cursor.write_uint::<BigEndian>(value, 4)?;

                start = end;
            }
        }

        // write comp_frac_str
        if comp_frac_bytes > 0 {
            if comp_frac_str.is_empty() {
                cursor.write_uint::<BigEndian>(0, comp_frac_bytes)?;
            } else {
                let value_str = format!("{:0<size$}", comp_frac_str, size = comp_frac_len);
                let value = value_str.parse::<u64>().unwrap();
                cursor.write_uint::<BigEndian>(value, comp_frac_bytes)?;
            }
        }

        if is_negative {
            for i in 0..buf.len() {
                buf[i] ^= 0xFF;
            }
        }
        buf[0] ^= 0x80;

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn test_decimal_4_0() {
        // DECIMAL(4,0), binlog: [2 bytes] . [0 bytes]
        run_decimal_tests(4, 0);
    }

    #[test]
    fn test_decimal_4_4() {
        // DECIMAL(4,4), binlog: [0 bytes] . [2 bytes]
        run_decimal_tests(4, 4);
    }

    #[test]
    fn test_decimal_10_0() {
        // DECIMAL(10,0), binlog: [1 byte][4 bytes] . [0 bytes]
        run_decimal_tests(10, 0);
    }

    #[test]
    fn test_decimal_10_10() {
        // DECIMAL(10,10), binlog: [0 bytes] . [4 bytes][1 byte]
        run_decimal_tests(10, 10);
    }

    #[test]
    fn test_decimal_10_4() {
        // DECIMAL(10,4), binlog: [3 bytes] . [2 bytes]
        run_decimal_tests(10, 4);
    }

    #[test]
    fn test_decimal_18_9() {
        // DECIMAL(18,9), binlog: [4 bytes] . [4 bytes]
        run_decimal_tests(18, 9);
    }

    #[test]
    fn test_decimal_27_13() {
        // DECIMAL(27,13), binlog: [3 bytes][4 bytes] . [4 bytes][2 bytes]
        run_decimal_tests(27, 13);
    }

    #[test]
    fn test_decimal_47_25() {
        // DECIMAL(47,25), binlog: [2 bytes][4 bytes][4 bytes] . [4 bytes][4 bytes][4 bytes]
        run_decimal_tests(47, 25);
    }

    fn run_decimal_tests(precision: usize, scale: usize) {
        let values = generate_decimal_values(precision as u8, scale as u8);
        for value in values {
            println!(
                "precision: {}, scale: {}, decimal1: {}",
                precision, scale, value
            );
            let buf = DecimalUtil::string_to_mysql_binlog(&value, precision, scale).unwrap();
            let decimal_str = DecimalUtil::mysql_binlog_to_string(&buf, precision, scale).unwrap();

            let decimal1 = Decimal::from_str(&value).unwrap();
            let decimal2 = Decimal::from_str(&decimal_str).unwrap();
            println!("decimal2: {}", decimal2);
            assert_eq!(decimal1, decimal2);
        }
    }

    fn generate_decimal_values(precision: u8, scale: u8) -> Vec<String> {
        // given precision = 10, scale = 4, integral = 6
        let integral = precision - scale;
        let mut tmp_values = Vec::new();

        let n_digit_str = |c: char, n: u8| -> String {
            let mut res = String::new();
            for _ in 0..n {
                res.push(c);
            }
            res
        };

        // 9, 99, ... 999999
        for i in 0..integral {
            let intg = n_digit_str('9', i + 1);
            tmp_values.push(intg.clone());
        }

        // 0.9, 0.99, ... 0.9999
        for j in 0..scale {
            let frac = n_digit_str('9', j + 1);
            tmp_values.push("0.".to_string() + &frac);
        }

        // 9.9, 9.99, 99.9, 99.99 ... 999999.9999
        for i in 0..integral {
            let intg = n_digit_str('9', i + 1);
            for j in 0..scale {
                let frac = n_digit_str('9', j + 1);
                tmp_values.push(intg.clone() + "." + &frac);
            }
        }

        // 9.9, 90.09, ... 900000.0009
        for i in 0..integral {
            let intg = n_digit_str('0', i);
            for j in 0..scale {
                let frac = n_digit_str('0', j);
                tmp_values.push("9".to_string() + &intg + "." + &frac + "9");
            }
        }

        // negative values
        let mut values = tmp_values.clone();
        for i in 0..tmp_values.len() {
            values.push("-".to_string() + &tmp_values[i]);
        }

        // 0
        values.push("0".to_string());

        values
    }
}
