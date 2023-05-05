// needs to be run as an executable - pregenerated workload passed from tests, and simply runs through it by writing and requesting
// saves statistics 

use std::collections::hash_map::DefaultHasher;
use std::fs::{File};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddrV4, TcpStream};

use std::time::{SystemTime, Duration};
use std::{env, thread};

use std::io::{BufReader};
use bincode::deserialize_from;
use secko_messages::{Message, KVPair, send_message, receive_message};
use secko_tests::{KeyParam, Workload, DataPoint};

fn main() {
    
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        println!("Insufficient arguments. Please provide \"<id> <workload-location> <server-ip:port>\".");
        return;
    }

    // parse arguments
    let id = match args[1].trim().parse::<u16>() {
        Ok(v) => v,
        Err(_) => {
            println!("Please pass a valid u64 value for the id (parameter 1).");
            return
        }
    };

    let workload: Vec<Workload>  = match File::open(&args[2].trim()) {
        Ok(f) => {
            match deserialize_from(&mut BufReader::new(f)) {
                Ok(workload) => workload,
                Err(e) => {
                    println!("Workload file {} deserialization failed with error: {}", &args[2], e);
                    return; // fail, as this is unexpected behavior
                }
            }
        },
        Err(e) => {
            println!("Please pass a valid file location for the client workload (parameter 3). - {}", e);
            return
        }
    };

    let server_address = match args[3].trim().parse::<SocketAddrV4>() {
        Ok(v) => v,
        Err(_) => {
            println!("Please pass a valid IPv4 value (ip:port) for the server address (parameter 4).");
            return
        }
    };

    // create a vector that will store key->parameter mappings (timestamp, send rate, value size)
    let mut keys_sent: Vec<KeyParam> = Vec::new();

    for w in workload {
        // we have a given set of parameters for this workload - a send rate, value size, and number of values
        let send_rate = w.params.client_send_rate;

        // make connection to the server
        let mut conn = match TcpStream::connect(server_address) {
            Ok(c) => c,
            Err(_) => {
                panic!("Failed to establish connection to server, line 104");
            }
        };

        // let mut ctr = 0;s

        for data in w.data {
            // println!("{}", ctr);
            // ctr += 1;
                    
            // create data
            let to_send = DataPoint{value: data, timestamp: SystemTime::now()}; // includes net latency both ways and hashing time on client side as thats all overhead
            let as_str = to_send.to_string();

            // make key for it
            let mut hash = DefaultHasher::new();
            as_str.to_string().hash(&mut hash);
            let hashed: u64 = hash.finish(); 

            // create message
            let req = Message::PushReq ( KVPair {key: hashed, value: as_str} );

            // send it
            match send_message(&mut conn, req) {
                Ok(_) => (),
                Err(_) => {
                    panic!("Failed to send message, line 126");
                }
            };

            // get response so we know its actually in there
            // wait on a response
            let resp = match receive_message(&mut conn) {
                Ok(r) => r,
                Err(e) => {
                    println!("Message receipt error! {}", e);
                    continue;
                }
            };

            // handle the response
            match resp {
                Message::Error(e) => println!("Push {} failed with error: {}", hashed, e),
                Message::PushResp{success: true} => {
                    // println!("Key {} pushed successfully.", hashed);

                    // save time it took to send
                    let return_time = SystemTime::now();

                    // add this datapoint to our vec
                    keys_sent.push(KeyParam { key: hashed, duration: return_time.duration_since(to_send.timestamp).expect("duration error"), params: w.params.clone(), timestamp: to_send.timestamp })

                },
                _ => println!("Execution should not have reached this point."),
            }
                    
            // wait before sending next one
            thread::sleep(Duration::from_millis(((1.0/send_rate)*1000.0) as u64));
        }
    }

    println!("{} - Failure focused client finished.", id);
} 