use std::net::TcpStream;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::env;

use std::io::{stdin,stdout,Write};
use secko_messages::{Message, KVPair, FoundValue, send_message, receive_message};

// cli front provided here, rest of library elsewhere. this is mainly for testing
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Insufficient arguments. Please provide \"<ip>:<port>\".");
        return;
    }

    // let mut stream = TcpStream::connect("127.0.0.1:6359").unwrap();
    let mut stream = TcpStream::connect(&args[1]).unwrap();
    let mut s = String::new();

    loop {
        // take input: https://users.rust-lang.org/t/how-to-get-user-input/5176/2
        print!("Enter a command: "); 
        let _ = stdout().flush();
        stdin().read_line(&mut s).expect("Did not enter a correct string");
        s = s.trim().to_string();

        // https://stackoverflow.com/questions/34559640/what-is-the-correct-idiomatic-way-to-check-if-a-string-starts-with-a-certain-c
        match s.get(..1) {
            Some("P") => {
                // get the value specified after "POST" -> not super sanitized
                let (_, value) = s.split_once(' ').unwrap(); 

                // push
                match push_req(&mut stream, value.to_string()) {
                    Message::Error(e) => println!("Push failed with error: {}", e),
                    Message::PushResp{success: true} => println!("Key pushed successfully."),
                    _ => println!("Execution should not have reached this point.")
                }
            },

            Some("G") => {
                // get the value specified after "GET"
                let key: &str = s.split_once(' ').unwrap().1;
                
                let key: u64 = match key.parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        println!("Please provide a valid u64 value.");
                        return
                    }
                };

                // get
                match get_req(&mut stream, key) {
                    Message::RetrieveResp{ result: FoundValue::Success { value }} => println!("Value found: {}",value),
                    Message::RetrieveResp{ result: FoundValue::Failure} => println!("Value not found."),
                    _ => println!("Execution should not have reached this point.")
                }
            },

            Some("D") => {
                // dump
                match dump_req(&mut stream) {
                    Message::DumpResp(dumped) => println!("{:#?}", dumped),
                    _ => println!("Execution should not have reached this point.")
                }
            },

            Some("L") => {
                // get cluster
                match cluster_req(&mut stream) {
                    Message::ClusterResp(cluster) => println!("{:#?}", cluster),
                    _ => println!("Execution should not have reached this point.")
                }
            },
            
            Some("S") => {
                // get the value specified after "SELECT"
                let (_, new_address) = s.split_once(' ').unwrap(); 
                println!("{}", new_address);

                // create a new connection
                stream = match TcpStream::connect(new_address) {
                    Ok(s) => {
                        println!("New connection successful.");
                        s
                    },
                    Err(_) => {
                        println!("Invalid address format. Please use form \"SELECT <ip>:<port>\".");
                        continue
                    }
                };
            },

            _ => println!("Invalid command. Please enter either \"POST <value>\", \"GET <key>\", \"DUMP\", \"LISTCLUSTER\", or \"SELECT <ip>:<port>\".")
        };

        s.clear();
    }
}

// functions unfortunately repeated here because other ones have specific ocaml return values. a refactor could be done but it wouldn't be to too much benefit as a lot of the logic in the other methods is dependent on ocaml encoding issues
fn push_req(stream: &mut TcpStream, value: String) -> Message {
    // make key for it
    let mut hash = DefaultHasher::new();
    value.to_string().hash(&mut hash);
    let hashed: u64 = hash.finish(); 

    let data = Message::PushReq ( KVPair {key: hashed, value: value} );
    send_message(stream, data).unwrap();

    let result: Message = receive_message(stream).unwrap();
    // println!("Response: {:#?}", result);
    
    result
}

fn get_req(stream: &mut TcpStream, key: u64) -> Message {
    let data = Message::RetrieveReq { key: key };
    send_message(stream, data).unwrap();
    
    let result: Message = receive_message(stream).unwrap();
    
    result
}

fn dump_req(stream: &mut TcpStream) -> Message {
    let data = Message::DumpReq;
    send_message(stream, data).unwrap();
    
    let result: Message = receive_message(stream).unwrap();
    
    result
}

fn cluster_req(stream: &mut TcpStream) -> Message {
    let data = Message::ClusterReq;
    send_message(stream, data).unwrap();
    
    let result: Message = receive_message(stream).unwrap();
    
    result
}