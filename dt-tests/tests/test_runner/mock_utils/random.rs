use rand::{rngs::ThreadRng, Rng, RngCore};

#[derive(Debug, Clone)]
pub struct Random {
    seed: u64,
    pub rng: ThreadRng,
}

impl Random {
    pub fn new(seed: Option<u64>) -> Self {
        Random {
            seed: seed.unwrap_or_else(|| {
                let start = std::time::SystemTime::now();
                let since_the_epoch = start
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards");
                since_the_epoch.as_secs_f64().to_bits()
            }),
            rng: rand::rng(),
        }
    }

    // todo(wl): use trait or generics to reach this.
    pub fn next_i8(&mut self) -> i8 {
        self.rng.random::<i8>()
    }

    pub fn next_u8(&mut self) -> u8 {
        self.rng.random()
    }

    pub fn next_i16(&mut self) -> i16 {
        self.rng.random::<i16>()
    }

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

    pub fn next_u64(&mut self) -> u64 {
        // let mut rng: rand::rngs::SmallRng = rand::SeedableRng::seed_from_u64(self.seed);
        self.rng.random()
    }

    pub fn next_f32(&mut self) -> f32 {
        self.rng.random::<f32>()
    }

    pub fn next_f64(&mut self) -> f64 {
        self.rng.random::<f64>()
    }

    pub fn random_range(&mut self, range: std::ops::Range<i32>) -> i32 {
        self.rng.random_range(range)
    }

    pub fn next_null(&mut self) -> bool {
        self.rng.random_bool(0.1)
    }
}
