use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};
use fake::faker::internet::raw::{IPv4, IPv6};
use fake::locales::EN;
use fake::Fake;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// PostgreSQL inet: IPv4 or IPv6 host address with optional subnet
/// Format: address/y where y is the netmask bits (optional)
pub struct Inet {
    pub addr: IpAddr,
    pub prefix: Option<u8>,
}

impl Inet {
    pub fn new(addr: IpAddr, prefix: Option<u8>) -> Self {
        Self { addr, prefix }
    }
}

impl fmt::Display for Inet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.prefix {
            Some(p) => write!(f, "{}/{}", self.addr, p),
            None => write!(f, "{}", self.addr),
        }
    }
}

impl RandomValue for Inet {
    fn next_value(random: &mut Random) -> String {
        let use_ipv6 = random.next_u8() % 2 == 0;
        let use_prefix = random.next_u8() % 2 == 0;

        if use_ipv6 {
            let ip: String = IPv6(EN).fake_with_rng(&mut random.rng);
            let addr: Ipv6Addr = ip.parse().unwrap_or(Ipv6Addr::LOCALHOST);
            let prefix = if use_prefix {
                Some(random.random_range(0..129) as u8)
            } else {
                None
            };
            Inet::new(IpAddr::V6(addr), prefix).to_string()
        } else {
            let ip: String = IPv4(EN).fake_with_rng(&mut random.rng);
            let addr: Ipv4Addr = ip.parse().unwrap_or(Ipv4Addr::LOCALHOST);
            let prefix = if use_prefix {
                Some(random.random_range(0..33) as u8)
            } else {
                None
            };
            Inet::new(IpAddr::V4(addr), prefix).to_string()
        }
    }
}

impl ConstantValues for Inet {
    fn next_values() -> Vec<String> {
        [
            "0.0.0.0",            // IPv4 any
            "127.0.0.1",          // IPv4 localhost
            "255.255.255.255",    // IPv4 broadcast
            "192.168.0.1/24",     // IPv4 with netmask
            "10.0.0.0/8",         // IPv4 private network
            "172.16.0.0/12",      // IPv4 private network
            "192.168.100.128/25", // IPv4 subnet
            "::1",                // IPv6 localhost
            "::",                 // IPv6 any
            "::ffff:192.168.1.1", // IPv4-mapped IPv6
            "2001:db8::1",        // IPv6 documentation
            "fe80::1/64",         // IPv6 link-local with prefix
            "2001:4f8:3:ba::/64", // IPv6 network
            "::ffff:1.2.3.0/120", // IPv4-mapped with prefix
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL cidr: IPv4 or IPv6 network specification
/// Must have zero bits to the right of the netmask
pub struct Cidr {
    pub addr: IpAddr,
    pub prefix: u8,
}

impl Cidr {
    pub fn new(addr: IpAddr, prefix: u8) -> Self {
        Self { addr, prefix }
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix)
    }
}

impl RandomValue for Cidr {
    fn next_value(random: &mut Random) -> String {
        let use_ipv6 = random.next_u8() % 2 == 0;

        if use_ipv6 {
            let prefix = random.random_range(0..129) as u8;
            // Generate network address with zero bits after prefix
            let mut bytes = [0u8; 16];
            let full_bytes = (prefix / 8) as usize;
            for byte in bytes.iter_mut().take(full_bytes) {
                *byte = random.next_u8();
            }
            if prefix % 8 != 0 && full_bytes < 16 {
                let mask = 0xFF << (8 - prefix % 8);
                bytes[full_bytes] = random.next_u8() & mask;
            }
            let addr = Ipv6Addr::from(bytes);
            Cidr::new(IpAddr::V6(addr), prefix).to_string()
        } else {
            let prefix = random.random_range(0..33) as u8;
            // Generate network address with zero bits after prefix
            let mut bytes = [0u8; 4];
            let full_bytes = (prefix / 8) as usize;
            for byte in bytes.iter_mut().take(full_bytes) {
                *byte = random.next_u8();
            }
            if prefix % 8 != 0 && full_bytes < 4 {
                let mask = 0xFF << (8 - prefix % 8);
                bytes[full_bytes] = random.next_u8() & mask;
            }
            let addr = Ipv4Addr::from(bytes);
            Cidr::new(IpAddr::V4(addr), prefix).to_string()
        }
    }
}

impl ConstantValues for Cidr {
    fn next_values() -> Vec<String> {
        [
            "0.0.0.0/0",          // default route
            "10.0.0.0/8",         // class A private
            "172.16.0.0/12",      // class B private
            "192.168.0.0/16",     // class C private
            "192.168.100.128/25", // subnet
            "192.168.0.0/24",     // /24 network
            "128.0.0.0/16",       // class B
            "10.1.2.3/32",        // single host
            "::/0",               // IPv6 default
            "::1/128",            // IPv6 localhost
            "2001:db8::/32",      // IPv6 documentation
            "fe80::/10",          // IPv6 link-local
            "2001:4f8:3:ba::/64", // IPv6 network
            "::ffff:1.2.3.0/120", // IPv4-mapped
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL macaddr: 6-byte MAC address
/// Output format: 08:00:2b:01:02:03
pub struct MacAddr(pub [u8; 6]);

impl MacAddr {
    pub fn new(bytes: [u8; 6]) -> Self {
        Self(bytes)
    }
}

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

impl RandomValue for MacAddr {
    fn next_value(random: &mut Random) -> String {
        let bytes: [u8; 6] = [
            random.next_u8(),
            random.next_u8(),
            random.next_u8(),
            random.next_u8(),
            random.next_u8(),
            random.next_u8(),
        ];
        MacAddr::new(bytes).to_string()
    }
}

impl ConstantValues for MacAddr {
    fn next_values() -> Vec<String> {
        [
            "00:00:00:00:00:00", // all zeros
            "ff:ff:ff:ff:ff:ff", // broadcast
            "08:00:2b:01:02:03", // example from docs
            "01:00:5e:00:00:01", // multicast
            "02:00:00:00:00:01", // locally administered
            "00:1a:2b:3c:4d:5e", // random vendor
            "a0:b1:c2:d3:e4:f5", // uppercase hex digits
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL macaddr8: 8-byte MAC address (EUI-64 format)
/// Output format: 08:00:2b:01:02:03:04:05
/// 6-byte MAC stored with 4th and 5th bytes set to FF:FE
pub struct MacAddr8(pub [u8; 8]);

impl MacAddr8 {
    pub fn new(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Convert 6-byte MAC to 8-byte EUI-64 format
    /// Inserts FF:FE between the 3rd and 4th bytes
    pub fn from_mac48(mac: [u8; 6]) -> Self {
        Self([mac[0], mac[1], mac[2], 0xff, 0xfe, mac[3], mac[4], mac[5]])
    }

    /// Convert to modified EUI-64 (flip 7th bit for IPv6)
    pub fn set_7bit(&self) -> Self {
        let mut bytes = self.0;
        bytes[0] ^= 0x02; // flip the 7th bit (universal/local bit)
        Self(bytes)
    }
}

impl fmt::Display for MacAddr8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7]
        )
    }
}

impl RandomValue for MacAddr8 {
    fn next_value(random: &mut Random) -> String {
        // Randomly choose between full 8-byte or 6-byte converted to EUI-64
        if random.next_u8() % 2 == 0 {
            let bytes: [u8; 8] = [
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
            ];
            MacAddr8::new(bytes).to_string()
        } else {
            let mac6: [u8; 6] = [
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
                random.next_u8(),
            ];
            MacAddr8::from_mac48(mac6).to_string()
        }
    }
}

impl ConstantValues for MacAddr8 {
    fn next_values() -> Vec<String> {
        [
            "00:00:00:00:00:00:00:00", // all zeros
            "ff:ff:ff:ff:ff:ff:ff:ff", // all ones
            "08:00:2b:01:02:03:04:05", // example from docs
            "08:00:2b:ff:fe:01:02:03", // EUI-64 from 6-byte MAC
            "0a:00:2b:ff:fe:01:02:03", // modified EUI-64 (7th bit set)
            "02:00:00:ff:fe:00:00:01", // locally administered EUI-64
            "a0:b1:c2:d3:e4:f5:a6:b7", // random
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
            let inet = Inet::next_value(&mut random);
            println!("Inet: {}", inet);
            // inet can be IPv4 or IPv6, with or without prefix
            assert!(!inet.is_empty());

            let cidr = Cidr::next_value(&mut random);
            println!("Cidr: {}", cidr);
            // cidr always has prefix
            assert!(cidr.contains('/'));

            let macaddr = MacAddr::next_value(&mut random);
            println!("MacAddr: {}", macaddr);
            // 6-byte MAC has 5 colons
            assert_eq!(macaddr.matches(':').count(), 5);

            let macaddr8 = MacAddr8::next_value(&mut random);
            println!("MacAddr8: {}", macaddr8);
            // 8-byte MAC has 7 colons
            assert_eq!(macaddr8.matches(':').count(), 7);

            println!("---");
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Inet constants: {:?}", Inet::next_values());
        println!("Cidr constants: {:?}", Cidr::next_values());
        println!("MacAddr constants: {:?}", MacAddr::next_values());
        println!("MacAddr8 constants: {:?}", MacAddr8::next_values());
    }

    #[test]
    fn test_macaddr8_from_mac48() {
        // Example from PostgreSQL docs: 08:00:2b:01:02:03 -> 08:00:2b:ff:fe:01:02:03
        let mac6 = [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03];
        let mac8 = MacAddr8::from_mac48(mac6);
        assert_eq!(mac8.to_string(), "08:00:2b:ff:fe:01:02:03");
    }

    #[test]
    fn test_macaddr8_set7bit() {
        // Example from PostgreSQL docs: macaddr8_set7bit('08:00:2b:01:02:03') -> 0a:00:2b:ff:fe:01:02:03
        let mac6 = [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03];
        let mac8 = MacAddr8::from_mac48(mac6).set_7bit();
        assert_eq!(mac8.to_string(), "0a:00:2b:ff:fe:01:02:03");
    }
}
