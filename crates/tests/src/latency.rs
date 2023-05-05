use std::{fs::{File, OpenOptions}, io::BufWriter, net::TcpStream, process::Command, time::{SystemTime, Duration}, thread};

use bincode::serialize_into;
use secko_messages::{Message, send_message, receive_message};
use secko_tests::{generate_batch, Param, Workload, spawn_server, spawn_client, Client};

use serde::{Serialize, Deserialize};

// generic test function
pub fn test_generic(id: u16, params: Param, workload_loc: String, workloads: Vec<Workload>, test_type: String) {
    
    // serialize workload
    serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
    
    // spawn pair of servers
    // secko_server 127.0.0.1:8000 127.0.0.1:8001 -b 127.0.0.1:9000 -r 1 -c /tmp/secko_commits1 -s /tmp/secko_snaps1
    let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                    format!("/tmp/secko_testing/{}-{}-secko_commits1", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-{}-secko_snaps1", test_type, id).to_string(),
                    &params);
    // secko_server 127.0.0.1:8001 127.0.0.1:8000 -b 127.0.0.1:9001 -r 1 -c /tmp/secko_commits2 -s /tmp/secko_snaps2
    let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                    format!("/tmp/secko_testing/{}-{}-secko_commits2", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-{}-secko_snaps2", test_type, id).to_string(),
                    &params);
    
    // spawn client 
    // <id> <workload-location> <server-ip:port>
    let server = "127.0.0.1:9000".to_string();
    let mut c = spawn_client(id, server, workload_loc, &params, test_type);

    // start all
    let mut s1_handle = s1.handle.spawn().unwrap();
    let mut s2_handle = s2.handle.spawn().unwrap();
    let mut c_handle = c.handle.spawn().unwrap();

    // // wait on c, then kill servers
    c_handle.wait().unwrap();
    s1_handle.kill().unwrap();
    s2_handle.kill().unwrap();
}

// need function to test operation size
pub fn test_size() {
    let test_type = "size_test";

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let client_send_rate: f64 = 100.0;
    let num_values: u64 = 500;

    // parameter that we do tweak (value size)
    let sizes: Vec<usize> = vec![10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 25000, 50000, 75000, 1000000, 2500000, 5000000]; // going larger creates multi gigabyte workloads which we didnt have disk space for!
    
    // for each one of them
    for (i, value_size) in sizes.iter().enumerate() {

        println!("(id {}) Working on size: {}", i, value_size);

        // set up test parameters
        let id: u16 = i as u16;
        let params = Param {ai_send_rate, client_send_rate, value_size: *value_size, num_values};

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});

        println!("(id {}) Workload generated...", i);

        test_generic(id, params, workload_loc, workloads, test_type.to_string());
    }

    let num_values: u64 = 50;

    // parameter that we do tweak (value size)
    let sizes: Vec<usize> = vec![10000000, 25000000]; // going larger creates multi gigabyte workloads which we didnt have disk space for!
    
    // for each one of them
    for (i, value_size) in sizes.iter().enumerate() {

        println!("(id {}) Working on size: {}", i, value_size);

        // set up test parameters
        let id: u16 = (i+18) as u16;
        let params = Param {ai_send_rate, client_send_rate, value_size: *value_size, num_values};

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});

        println!("(id {}) Workload generated...", i);

        test_generic(id, params, workload_loc, workloads, test_type.to_string());
    }
}

// need function to test number of keys
pub fn test_num_keys() {
    let test_type = "num_keys";

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let client_send_rate: f64 = 100.0;
    let value_size: usize = 100; 

    // parameter that we do tweak (num values)
    let num_values_total: Vec<u64> = vec![10, 100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 1000000];
    
    // for each one of them
    for (i, num_values) in num_values_total.iter().enumerate() {
        println!("(id {}) Working on num_values: {}", i, num_values);

        // set up test parameters
        let id: u16 = (i+12) as u16;
        let params = Param {ai_send_rate, client_send_rate, value_size, num_values: *num_values};

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});

        println!("(id {}) Workload generated...", i);

        test_generic(id, params, workload_loc, workloads, test_type.to_string());
    }
}

// need function to test rate
pub fn test_rate() {
    let test_type = "rate";

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let value_size: usize = 1000; 
    let num_values: u64 = 500;

    // parameter that we do tweak (rate) - expect to plateau. would help to do awaits here but didn't get it to work right.
    let client_send_rates = vec![10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 10000000.0];
    
    // for each one of them
    for (i, client_send_rate) in client_send_rates.iter().enumerate() {
        println!("(id {}) Working on rate: {}", i, client_send_rate);

        // set up test parameters
        let id: u16 = i as u16;//*client_send_rate as u16;
        let params = Param {ai_send_rate, client_send_rate: *client_send_rate, value_size, num_values};

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});

        println!("(id {}) Workload generated...", i);

        test_generic(id, params, workload_loc, workloads, test_type.to_string());
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct FailData {
    rate: f64,
    expected_length: u64,
    length_1: u64,
    length_2: u64
}

// need function to test failures
pub fn test_failure() {
    let mut res: Vec<FailData> = Vec::new();

    let test_type = "failure";

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let value_size: usize = 1000; 
    let num_values: u64 = 50;

    // parameter that we do tweak (rate)
    let client_send_rates = vec![50000.0, 75000.0, 100000.0, 10000000.0];//vec![10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 10000000.0];
    
    // for each one of them
    for (i, client_send_rate) in client_send_rates.iter().enumerate() {
        let i = i+14;

        println!("(id {}) Working on rate: {}", i, client_send_rate);

        // set up test parameters
        let id = *client_send_rate as u16;
        let params = Param {ai_send_rate, client_send_rate: *client_send_rate, value_size, num_values: num_values*(*client_send_rate/10.0) as u64};

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});

        println!("(id {}) Workload generated...", i);

        // serialize workload
        serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
        
        // spawn pair of servers
        // secko_server 127.0.0.1:8000 127.0.0.1:8001 -b 127.0.0.1:9000 -r 1 -c /tmp/secko_commits1 -s /tmp/secko_snaps1
        let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                        format!("/tmp/secko_testing/{}-{}-secko_commits1", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-{}-secko_snaps1", test_type, id).to_string(),
                        &params);
        // secko_server 127.0.0.1:8001 127.0.0.1:8000 -b 127.0.0.1:9001 -r 1 -c /tmp/secko_commits2 -s /tmp/secko_snaps2
        let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                        format!("/tmp/secko_testing/{}-{}-secko_commits2", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-{}-secko_snaps2", test_type, id).to_string(),
                        &params);
        
        // spawn client 
        // <id> <workload-location> <server-ip:port>
        let server = "127.0.0.1:9000".to_string();
        let mut c = Command::new("/bin/fail_client"); // client will be written here
        // add args
        c.args([id.to_string(), workload_loc, server]);
        // return the handle, paired with the id
        let mut c = Client { handle: c, id: id, creation_time: SystemTime::now()};

        // start all
        let mut s1_handle = s1.handle.spawn().unwrap();
        let mut s2_handle = s2.handle.spawn().unwrap();
        let mut c_handle = c.handle.spawn().unwrap();

        // // wait on c, then kill servers
        c_handle.wait().unwrap();
        s1_handle.kill().unwrap();
        s2_handle.kill().unwrap();
        
        println!("(id {}) Killed all...", i);

        // bring servers back up, to see recovery
        let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                        format!("/tmp/secko_testing/{}-{}-secko_commits1", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-{}-secko_snaps1", test_type, id).to_string(),
                        &params);
        let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                        format!("/tmp/secko_testing/{}-{}-secko_commits2", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-{}-secko_snaps2", test_type, id).to_string(),
                        &params);

        // start all
        let mut s1_handle = s1.handle.spawn().unwrap();
        let mut s2_handle = s2.handle.spawn().unwrap();

        thread::sleep(Duration::from_secs(5));

        // send a dump
        let server = "127.0.0.1:9000".to_string();
        let mut conn = match TcpStream::connect(server) {
            Ok(c) => c,
            Err(_) => {
                panic!("Failed to establish connection to server, line 104");
            }
        };
        
        println!("(id {}) DumpReq sent...", i);

        let d = match dump_req(&mut conn) {
            Message::DumpResp(dumped) => dumped.len(),
            _ => 0
        };

        // send another dump
        let server = "127.0.0.1:9001".to_string();
        let mut conn = match TcpStream::connect(server) {
            Ok(c) => c,
            Err(_) => {
                panic!("Failed to establish connection to server, line 104");
            }
        };
        
        println!("(id {}) DumpReq sent...", i);

        let d2 = match dump_req(&mut conn) {
            Message::DumpResp(dumped) => dumped.len(),
            _ => 0
        };

        // get length 
        println!("(id {}) Length expected: {}; Length found: {}, and {}", id, num_values*(*client_send_rate/10.0) as u64, d, d2);
        s1_handle.kill().unwrap();
        s2_handle.kill().unwrap();

        // log results
        res.push(FailData { rate: *client_send_rate, expected_length: num_values*(*client_send_rate/10.0) as u64, length_1: d as u64, length_2: d2 as u64} );

        // dump
        let serialize_res = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(format!("/tmp/secko_testing/failure_results{}", i))
            .expect("Failure results dump failed.");

        match serde_json::to_writer(BufWriter::new(serialize_res),  &res) {
        Ok(()) => (),
        _ => println!("Serialization of results failed. Printing instead\n{:?}", res)
        };

        // wait again for everything to be totally killed
        thread::sleep(Duration::from_secs(5));
    }

    // dump
    let serialize_res = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open("/tmp/secko_testing/failure_results")
            .expect("Failure results dump failed.");

    match serde_json::to_writer(BufWriter::new(serialize_res),  &res) {
        Ok(()) => (),
        _ => println!("Serialization of results failed. Printing instead\n{:?}", res)
    };
}

fn dump_req(stream: &mut TcpStream) -> Message {
    let data = Message::DumpReq;
    send_message(stream, data).unwrap();
    
    let result: Message = receive_message(stream).unwrap();
    // println!("Response: {:#?}", result);
    
    result
}