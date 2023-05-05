use serde::{Serialize, Deserialize};
use bincode::{serialize, deserialize};
use std::collections::HashMap;
use std::net::TcpStream;
use std::io::{Write, Read};


pub type ReplicaId = u64;
pub type Key = u64;

#[derive(Serialize, Deserialize, Debug)]
pub struct DigestPair {
    pub replica_id: ReplicaId,
    pub keys: usize
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateMessage {
    pub sending_rate: f64,
    pub replica_keys: HashMap<ReplicaId, Vec<(Key, usize)>>, // (key, order)
    pub key_values: Vec<KVPair>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {

    RetrieveReq{ key: u64 },
    RetrieveResp{ result: FoundValue },

    PushReq(KVPair),
    PushResp{ success: bool },

    DumpReq,
    DumpResp(Vec<KVPair>),
    
    DumpLenReq,
    DumpLenResp(usize),

    ClusterReq,
    ClusterResp(Vec<ClusterNode>),

    Error(String),
    ConnectionClosed,

    DigestMessage(ReplicaId, Vec<DigestPair>),
    UpdateMessage(ReplicaId, UpdateMessage)
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Message::RetrieveReq { key } => write!(f, "Message::RetrieveReq {{ key: {} }}", key)?,
            Message::RetrieveResp { result } => write!(f, "Message::RetrieveReq {{ result: {} }}", result)?,
            Message::PushReq(pair) => write!(f, "Message::PushReq ({})", pair)?,
            Message::PushResp { success } => write!(f, "Message::PushResp {{ success: {} }}", success)?,
            Message::DumpReq => write!(f, "Message::DumpReq")?,
            Message::DumpResp(v) => write!(f, "Message::DumpResp({:?})", v)?,
            Message::DumpLenReq => write!(f, "Message::DumpLenReq")?,
            Message::DumpLenResp(l) => write!(f, "Message::DumpLenResp({})", l)?,
            Message::ClusterReq => write!(f, "Message::ClusterReq")?,
            Message::ClusterResp(v) => write!(f, "Message::ClusterResp({:?})", v)?,
            Message::Error(s) => write!(f, "Message::Error({})", s)?,
            Message::ConnectionClosed => write!(f, "Message::ConnectionClosed")?,
            Message::UpdateMessage(id, msg) => write!(f, "Message::UpdateMessage{{from: {}, sending_rate: {}, replica_keys: {:?}, key_values: {:?}}}", id, msg.sending_rate, msg.replica_keys, msg.key_values)?,
            Message::DigestMessage(id, pairs) => write!(f, "Message::DigestMessage{{from: {}, pairs: {:?}}}", id, pairs)?,
        };
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterNode {
    pub replica_id: String
}

impl std::fmt::Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ClusterNode {{ replica_id: {} }}", self.replica_id)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KVPair {
    pub key: u64,
    pub value: String,
}

impl std::fmt::Display for KVPair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "KVPair {{ key: {}, value: {} }}", self.key, self.value)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FoundValue {
    Success { value: String },
    Failure,
}

impl std::fmt::Display for FoundValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FoundValue::Success { value } => write!(f, "FoundValue::Success {{ value: {} }}", value)?,
            FoundValue::Failure => write!(f, "FoundValue::Failure")?,
        };
        Ok(())
    }
}

pub fn send_message(stream: &mut TcpStream, msg: Message) -> Result<(), String> {
    // first, serialize the request
    let data = match serialize(&msg) {
        Ok(data) => data,
        Err(e) => return Err(e.to_string()),
    };

    // then, get the length of that serialized data
    let len: [u8; 8] = data.len().to_be_bytes(); // NOTE: works if usize is 64 bytes.

    // now, write the message's length...
    let _: Result<(), String> = match stream.write(&len) {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    
    // ...followed by the message itself.
    let _: Result<(), String> = match stream.write(&data) {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    // and finally flush
    let _: Result<(), String> = match stream.flush() {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    Ok(())
    
}

pub fn receive_message(stream: &mut TcpStream) -> Result<Message, String> {
    // first, get the size
    // https://users.rust-lang.org/t/reading-length-payload-from-a-tcpstream/51211
    let mut size_buffer = [0; 8];
    let _: Result<(), String> = match stream.read(&mut size_buffer) {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let bincode_size: usize = usize::from_be_bytes(size_buffer);

    if bincode_size == 0 { // catch EOF
        return Ok(Message::ConnectionClosed);
    }

    // now, read in the request
    let mut request = vec![0; bincode_size];
    let _: Result<(), String> = match stream.read(&mut request) {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    // and return the deserialized result
    match deserialize(&request) {
        Ok(msg) => Ok(msg),
        Err(e) => return Err(e.to_string()),
    }
}