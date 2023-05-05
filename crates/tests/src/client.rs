// needs to be run as an executable - pregenerated workload passed from tests, and simply runs through it by writing and requesting
// saves statistics 

use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddrV4, TcpStream};

use std::time::{SystemTime, Duration};
use std::{env, thread};

use std::io::{BufReader, BufWriter};
use std::sync::Arc;
use bincode::deserialize_from;
use secko_messages::{Message, KVPair, FoundValue, send_message, receive_message};
use secko_tests::{KeyParam, Workload, DataPoint, async_send_message, async_receive_message, Param};

use rand_distr::{Distribution, Beta};

fn main() {
    
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        println!("Insufficient arguments. Please provide \"<id> <workload-location> <server-ip:port> <test-type>\".");
        return;
    }

    println!("args: {:?}", args);

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

    let test_type = &args[4];

    println!("arguments given: id: {}, workload_file: {}, server_address: {}, test_type: {}", id, &args[2].trim(), server_address, test_type);
    println!("send_rate: {}", workload[0].params.client_send_rate);

    // create a vector that will store key->parameter mappings (timestamp, send rate, value size)
    let mut keys_sent: Vec<KeyParam> = Vec::new();
    let mut keys_requested: Vec<KeyParam> = Vec::new();

    

    // make connection to the server
    {
        let mut conn = match TcpStream::connect(server_address) {
            Ok(c) => c,
            Err(_) => {
                panic!("Failed to establish connection to server, line 104");
            }
        };

        for w in workload {
            // we have a given set of parameters for this workload - a send rate, value size, and number of values
            let send_rate = w.params.client_send_rate;            

            for (i, data) in w.data.iter().enumerate() {   
                // if i%10 == 0 { 
                    println!("{}", i);
                // }

                // create data
                let to_send = DataPoint{value: data.to_string(), timestamp: SystemTime::now()}; // includes net latency both ways and hashing time on client side as thats all overhead
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
                let result: Message =  match receive_message(&mut conn) {
                    Ok(res) => res,
                    Err(_) => {
                        panic!("Failed to receive message from server, line 207");
                    }
                };

                match result {
                    Message::Error(e) => println!("Push {} failed with error: {}", hashed, e),
                    Message::PushResp{success: true} => {
                        // println!("Key pushed successfully.");

                        // save time it took to send
                        let return_time = SystemTime::now();

                        // add this datapoint to our vec
                        keys_sent.push(KeyParam { key: hashed, duration: return_time.duration_since(to_send.timestamp).expect("fail"), params: w.params.clone(), timestamp: to_send.timestamp })

                    },
                    _ => println!("Execution should not have reached this point."),
                }

                
                // wait before sending next one
                thread::sleep(Duration::from_millis(((1.0/send_rate)*1000.0) as u64));
            }

            // wait for next servers to come up before starting more sends, for elasticity test
            // thread::sleep(Duration::from_secs(30 as u64));

        }
    }

    println!("Finished posts.");

    {
        // create thread for GETs
        let beta = Beta::new(5.0, 1.0).expect("177 err"); // biases towards end
        let mut conn = match TcpStream::connect(server_address) {
            Ok(c) => c,
            Err(_) => {
                panic!("Failed to establish connection to server, line 181");
            }
        };

        let len = keys_sent.len();


        for i in 0..len {
            println!("get {}!", i);
        // loop {

            // pick random element close to end of keys_sent
            // let mut a = rand::thread_rng();

            let index = len - 3;// (beta.sample(&mut a) * (len as f64)) as usize;
            let kp = &keys_sent[index];

            // start timer
            let start = SystemTime::now();

            // send request to retrieve
            let data = Message::RetrieveReq { key: kp.key };
            match send_message(&mut conn, data) {
                Ok(res) => res,
                Err(_) => {
                    panic!("Failed to send message to server, line 199");
                }
            };

            // receive the result (also as a keyparam), except this time num_values is the length of the vector
            let result: Message =  match receive_message(&mut conn) {
                Ok(res) => res,
                Err(_) => {
                    panic!("Failed to receive message from server, line 207");
                }
            };

            match result {
                Message::RetrieveResp{ result: FoundValue::Success {..}} => {
                    // end timer
                    let end = SystemTime::now();

                    // log/store
                    keys_requested.push(KeyParam {
                        key: kp.key,
                        duration: end.duration_since(start).expect("duration error 219"),
                        params: Param {
                            ai_send_rate: kp.params.ai_send_rate,
                            client_send_rate: kp.params.client_send_rate,
                            value_size: kp.params.value_size,
                            num_values: len as u64
                        },
                        timestamp: start
                    });
                },
                _ => ()
            }

            // wait before sending next one
            thread::sleep(Duration::from_millis(((1.0/kp.params.client_send_rate)*1000.0) as u64));
        }
    }

    println!("Finished gets.");

    // start timer for the dump
    let dump_start: SystemTime = SystemTime::now(); 

    // dump
    let serialize_ks = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(format!("/tmp/secko_testing/{}-{}-secko_test_results_client_send", test_type, id))
            .expect("Client keys-sent dump failed.");

    match serde_json::to_writer(BufWriter::new(serialize_ks),  &keys_sent) {
        Ok(()) => (),
        _ => println!("Serialization of results failed. Printing instead\n{:?}", keys_sent)
    };

    // dump
    let serialize_kr = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(format!("/tmp/secko_testing/{}-{}-secko_test_results_client_get", test_type, id))
            .expect("Client keys-requested dump failed.");
    match serde_json::to_writer(BufWriter::new(serialize_kr),  &keys_requested) {
        Ok(()) => (),
        _ => println!("Serialization of results failed. Printing instead\n{:?}", keys_requested)
    };

    // print timer result for the dump so we know to subtract that. or somehow return that value?
    let dump_duration: Duration = SystemTime::now().duration_since(dump_start).expect("duration 264");
    println!("Dump duration - {} ms", dump_duration.as_millis());
} 