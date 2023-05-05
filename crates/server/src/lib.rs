// persistence
use std::{sync::Arc, time::SystemTime};

#[derive(Debug)]
pub struct Commit {
    pub key: u64,
    pub value: Arc<String>,
    pub timestamp: SystemTime, // when it was received on the server, for testing
}

// antientropy
pub mod map;
use map::LockFreeMap;
use std::{net::{Ipv4Addr, SocketAddrV4}, sync::{Mutex}};
use secko_messages::DigestPair;

pub fn create_digest(map: Arc<LockFreeMap<Mutex<Vec<u64>>>>) -> Vec<DigestPair> {
    let mut result: Vec<DigestPair> = Vec::new();

    for peer in map.iter() {
        result.push(DigestPair { replica_id: *peer.key(), keys: peer.val().lock().unwrap().len() })
    }

    result
}

pub fn u64_to_socketaddr(addr: u64) -> SocketAddrV4 {
    let port: u16 = (addr & 65535) as u16;
    let ip: Ipv4Addr = Ipv4Addr::from(((addr >> 16) as u32).to_be_bytes());

    SocketAddrV4::new(ip, port)
}

pub fn socketaddr_to_u64(addr: &SocketAddrV4) -> u64 {
    let ip = addr.ip();
    let port = addr.port();

    // parse ip into a u32
    let octets = ip.octets();

    let mut result: u64 = 0;

    result += u32::from_be_bytes(octets) as u64;

    result <<= 16;
    result += port as u64;

    result
}