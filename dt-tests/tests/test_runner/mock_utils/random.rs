use rand::{rngs::SmallRng, Rng, SeedableRng};

const ALPHANUMERIC_SPECIALCHAR_ALPHABET: &[u8] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#<>/.,~-+'*()[]{} ^*?%_\t\n\r|&\\@";

pub trait RandomValue {
    fn next_value(random: &mut Random) -> String;
}

#[derive(Debug, Clone)]
pub struct Random {
    pub rng: SmallRng,
}

impl Random {
    pub fn new(seed: Option<u64>) -> Self {
        let seed = seed.unwrap_or_else(|| {
            let start = std::time::SystemTime::now();
            let since_the_epoch = start
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards");
            since_the_epoch.as_secs_f64().to_bits()
        });
        Random {
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    #[allow(dead_code)]
    pub fn next_i8(&mut self) -> i8 {
        self.rng.random::<i8>()
    }

    pub fn next_u8(&mut self) -> u8 {
        self.rng.random()
    }

    pub fn next_i16(&mut self) -> i16 {
        self.rng.random::<i16>()
    }

    #[allow(dead_code)]
    pub fn next_u16(&mut self) -> u16 {
        self.rng.random::<u16>()
    }

    pub fn next_i32(&mut self) -> i32 {
        self.rng.random::<i32>()
    }

    pub fn next_u32(&mut self) -> u32 {
        self.rng.random::<u32>()
    }

    pub fn next_i64(&mut self) -> i64 {
        self.rng.random::<i64>()
    }

    #[allow(dead_code)]
    pub fn next_u64(&mut self) -> u64 {
        self.rng.random::<u64>()
    }

    pub fn next_f32(&mut self) -> f32 {
        self.rng.random::<f32>()
    }

    pub fn next_f64(&mut self) -> f64 {
        self.rng.random::<f64>()
    }

    pub fn next_str(&mut self) -> String {
        let len = ALPHANUMERIC_SPECIALCHAR_ALPHABET.len();
        let random_len = self.random_range(0..10);
        (0..random_len)
            .map(|_| {
                let idx = self.random_range(0..len as i32);
                ALPHANUMERIC_SPECIALCHAR_ALPHABET[idx as usize] as char
            })
            .collect()
    }

    pub fn random_range(&mut self, range: std::ops::Range<i32>) -> i32 {
        self.rng.random_range(range)
    }

    pub fn next_null(&mut self) -> bool {
        self.rng.random_bool(0.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed() {
        let mut rand = Random::new(Some(7));
        let mut vec: Vec<i32> = Vec::with_capacity(100);
        for _ in 0..100 {
            vec.push(rand.next_i32());
        }
        let expected  = "[237771264, 739231965, -1213001293, 1834852202, -156081166, 2000182114, -1185809997, 1416649562, -75923635, 314751485, 490662379, 738597540, -1143218426, 485699677, 2124441841, 417798290, 699280262, 792112711, 472187858, -902211082, -1393202401, 1789501035, -890969798, -2014915034, -674759587, 24867006, 23271840, 517758961, 1652633554, 1227698302, -845606690, 356789780, -2114907363, -717063462, 1464841181, -1643135263, 1086603155, -1773231510, -698087324, -1115711526, -112131525, -1642743398, 259881389, 1715301801, 613806994, -1799545304, 1325623922, 380962850, 1277712414, -653616360, 235033415, -2011460659, 943374440, -237672300, -1863503228, -785485222, 775933338, 1241005070, -153615121, 1611990283, 732979265, -1210615832, 325814512, -518109528, -1426344321, 1260754464, 809236849, 977623197, -1061395087, 209339364, -1980437853, -1533028137, -1178472767, -630431393, -2022983217, -682547457, 905147446, -31206503, 1956762053, 960659339, -549273699, 2013846028, 1872942446, -868753897, 2106089592, 1848274029, 1482906938, 382095821, 14326962, -132353939, 859826073, -1429314448, 357472750, -1862995772, -837065148, 2040180983, 536613643, -445221248, 2112197413, -588375482]";
        assert_eq!(format!("{:?}", vec), expected);
    }
}
