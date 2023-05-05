use bincode::{serialize, deserialize};
use rand::{Rng, distributions::Alphanumeric};
use secko_messages::Message;
use serde::{Serialize, Deserialize};
use std::{
    fmt::{self},
    process::{Command},
    time::{Duration, SystemTime}
};
use chrono::offset::Utc;
use chrono::DateTime; // https://stackoverflow.com/questions/45386585/how-to-format-systemtime-to-string

// to send a bunch of messages and handle responses later, not sequentially
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// need struct for datapoint, used in workload struct and is what is stored on the server
#[derive(Serialize, Deserialize, Debug)]
pub struct DataPoint {
    pub value: String,
    pub timestamp: SystemTime, // time at which it was sent
}

impl fmt::Display for DataPoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let datetime: DateTime<Utc> = self.timestamp.into();
        write!(f, "DataPoint (sent at: {})", datetime.format("%d/%m/%Y %T.%f"))
    }
}

// need struct for just parameters
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Param {
    pub ai_send_rate: f64, // per second
    pub client_send_rate: f64, // per second
    pub value_size: usize, // in bytes
    pub num_values: u64
}

impl fmt::Display for Param {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Param {{ai_send_rate: {}, client_send_rate: {}, value_size: {}, num_values: {}}}", self.ai_send_rate, self.client_send_rate, self.value_size, self.num_values)
    }
}

// need struct for key-parameter mapping, stores results of a datapoint on a client
#[derive(Serialize, Deserialize, Debug)]
pub struct KeyParam {
    pub key: u64,
    pub duration: Duration, // how long it took to hear back
    pub params: Param, 
    pub timestamp: SystemTime // when it was sent
}

impl fmt::Display for KeyParam {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let datetime: DateTime<Utc> = self.timestamp.into();
        write!(f, "KeyParam {{key: {}, duration (ms): {}, params: {}, timestamp: {}}}", self.key, self.duration.as_millis(), self.params, datetime.format("%d/%m/%Y %T.%f"))
    }
}

// need struct for server
#[derive(Debug)]
pub struct Server {
    pub handle: Command,
    pub id: u16,
    pub creation_time: SystemTime
}

// need struct for client
#[derive(Debug)]
pub struct Client {
    pub handle: Command,
    pub id: u16,
    pub creation_time: SystemTime
}

// need struct for workload
#[derive(Serialize, Deserialize, Debug)]
pub struct Workload {
    pub data: Vec<String>, // the value is a string. wrapped with a time in DataPoint for when we actually send it.
    pub params: Param
}

// need struct for server/client pair
#[derive(Debug)]
pub struct SCPair {
    pub client: Client,
    pub server: Server
}

// need function to spawn server
pub fn spawn_server(id: u16, ai_port: u16, neighbor: String, client_port: u16, commit: String, snapshot: String, parameters: &Param) -> Server{
    let ai_ip: String = format!("127.0.0.1:{}", ai_port);
    let binding: String = format!("127.0.0.1:{}", client_port);
   
    // run command
    let mut c = Command::new("secko_server");
    
    // add args
    c.args([ai_ip, 
                neighbor, 
                format!("-b {}", binding), 
                format!("-r {}", parameters.ai_send_rate as f64), 
                format!("-c {}", commit), 
                format!("-s {}", snapshot)]);

    // return the handle, paired with the id
    Server { handle: c, id: id, creation_time: SystemTime::now()}
}

// need function to spawn client
pub fn spawn_client(id: u16, server_ip: String, workload_loc: String, _: &Param, test_type: String) -> Client{
    // run command
    let mut c = Command::new("/bin/test_client"); // client will be written here

    // add args
    c.args([id.to_string(), workload_loc, server_ip, test_type]);

    // return the handle, paired with the id
    Client { handle: c, id: id, creation_time: SystemTime::now()}
}

// need function to spawn pairs
pub fn spawn_pair(id: u16, ai_neighbor: String, parameters: Param, test_type: String, workload_loc: String) -> SCPair{
    // make versions of all relevant variables
    let ai_port: u16 = 8000+id;
    let client_port: u16 = 9000+id;

    let commit_loc = format!("/tmp/secko_testing/{}-secko_commit_log_{}.txt", test_type, id);
    let snapshot_loc = format!("/tmp/secko_testing/{}-secko_snapshot_{}", test_type, id);
    // let workload_loc = format!("/tmp/secko_testing/{}-secko_test_workload_{}", test_type, id);

    // call the above
    let c = spawn_client(id, "127.0.0.1".to_string(), workload_loc, &parameters, test_type);
    let s = spawn_server(id, ai_port, ai_neighbor, client_port, commit_loc, snapshot_loc, &parameters); 

    SCPair { client: c, server: s }
}

// need function to generate single message
pub fn generate_message(id: u16, bsize: usize) -> String {
    // https://rust-lang-nursery.github.io/rust-cookbook/algorithms/randomness.html
    let value: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(bsize)
        .map(char::from)
        .collect();

    // format id-value, so 1-a7ryw87...
    format!("{}-{}", id, value)
}

// need function to generate batch of messages?
pub fn generate_batch(id: u16, parameters: &Param) -> Vec<String> {
    let mut v: Vec<String> = Vec::new();

    for _ in 0..parameters.num_values {
        // if i%1000 == 0 {
            // println!("{}", i);
        // }

        v.push(generate_message(id, parameters.value_size));
    }

    v
}

// async sending and receiving
pub async fn async_send_message(stream: &mut TcpStream, msg: Message) -> Result<(), String> {
    // first, serialize the request
    let data = match serialize(&msg) {
        Ok(data) => data,
        Err(e) => return Err(e.to_string()),
    };

    // then, get the length of that serialized data
    let len: [u8; 8] = data.len().to_be_bytes(); // NOTE: works if usize is 64 bytes.

    // now, write the message's length...
    let _: Result<(), String> = match stream.write(&len).await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    
    // ...followed by the message itself.
    let _: Result<(), String> = match stream.write(&data).await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    // and finally flush
    let _: Result<(), String> = match stream.flush().await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    Ok(())
    
}

pub async fn async_receive_message(stream: &mut TcpStream) -> Result<Message, String> {
    // first, get the size
    // https://users.rust-lang.org/t/reading-length-payload-from-a-tcpstream/51211
    let mut size_buffer = [0; 8];
    let _: Result<(), String> = match stream.read(&mut size_buffer).await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };
    let bincode_size: usize = usize::from_be_bytes(size_buffer);

    if bincode_size == 0 { // catch EOF
        return Ok(Message::ConnectionClosed);
    }

    // now, read in the request
    let mut request = vec![0; bincode_size];
    let _: Result<(), String> = match stream.read(&mut request).await {
        Ok(_) => Ok(()),
        Err(e) => return Err(e.to_string()),
    };

    // and return the deserialized result
    match deserialize(&request) {
        Ok(msg) => Ok(msg),
        Err(e) => return Err(e.to_string()),
    }
}