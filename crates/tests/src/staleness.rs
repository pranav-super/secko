use std::{fs::File, io::BufWriter, net::TcpStream, time::Duration, thread};

use bincode::serialize_into;
use secko_messages::{Message, receive_message, send_message};
use secko_tests::{generate_batch, Param, Workload, spawn_server, spawn_client};

// need function for base staleness
pub fn test_generic(n: u16, ai_send_rate: f64, client_send_rate: f64) {
    // let n = 4;
    
    // create a workload, something on the order of 500 elements, small size
    let test_type = "generic_staleness";

    // set up base parameters/things that don't vary
    // let ai_send_rate: f64 = 0.5;
    // let client_send_rate: f64 = 100.0;
    let num_values: u64 = 250;
    let value_size: usize = 1000;
    let params = Param {ai_send_rate, client_send_rate, value_size, num_values};


    // spawn N servers
    // spawn 2 to kickstart cluster
    // bring servers back up, to see recovery
    let mut servers = Vec::new();
    let mut s_handles = Vec::new();
    let id = 0;
    let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                &params);
    s_handles.push(s1.handle.spawn().unwrap());
    servers.push(s1);

    let id = 1;
    let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                &params);
    s_handles.push(s2.handle.spawn().unwrap());
    servers.push(s2);

    // spawn remaining N-2 and have them point to first one
    for id in 2..n {
        let mut s = spawn_server(id, 8000+id, "127.0.0.1:8000".to_string(), 9000+id, 
                    format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                    &params);
        s_handles.push(s.handle.spawn().unwrap());
        servers.push(s);
    }

    // create N workloads for each client
    let mut workload_locs = Vec::new();
    for i in 0..n {

        // set up test parameters
        let id: u16 = i as u16;

        // generate workloads (only 1 for now)
        let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
        let mut workloads = Vec::new();
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});
        workload_locs.push(workload_loc.clone());

        // serialize workload
        serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
    }

    println!("MASTER - workloads made.");

    // spawn N clients with said workloads
    let mut clients = Vec::new();
    for id in 0..n {
        let port = 9000 + id;
        clients.push(
            spawn_client(id, format!("127.0.0.1:{}",port).to_string(), workload_locs[id as usize].to_string(), 
                            &params, test_type.to_string()).handle.spawn().unwrap()
        );
    }

    // join all clients
    for c in 0..n {
        clients[c as usize].wait().unwrap();
    }

    println!("All clients joined.");

    // go through each server, send a dump, if length is right, then join, else continue
    // do until list of servers is empty
    let mut server_conns = Vec::new();
    for id in 0..n {
        let conn = TcpStream::connect(format!("127.0.0.1:{}", 9000+id)).unwrap(); // declared out here so connection not dropped too quickly and we don't get err - connection reset by peer.
        server_conns.push(conn);
    }

    while servers.len() > 0 {
        // send dump
        let id = servers[0].id;
        
        println!("sending dump for id: {}", id);
        // get length
        let resp = dump_len_req(&mut server_conns[0]);

        println!("handling dump for id: {}", id);

        let len: usize = match resp {
            Message::DumpLenResp(len) => {
                println!("extracting length of dump for id: {}, len: {}", id, len);
                len
            },
            _ => {
                println!("Execution should not have reached this point.");
                continue;
            }
        };

        // join or continue based on length
        if len >= (n*(num_values as u16)) as usize {
            println!("DUMPS SUCCESSFUL - REACHED DESIRED LENGTH OF {}.", len);

            servers.remove(0);
            server_conns.remove(0);
        }

        thread::sleep(Duration::from_secs(1));
    }

    for mut server in s_handles {
        server.kill().unwrap();
    }

}

fn dump_len_req(stream: &mut TcpStream) -> Message {
    let data = Message::DumpLenReq;
    send_message(stream, data).unwrap();
    
    let result: Message = receive_message(stream).unwrap();
    // println!("Response: {:#?}", result);
    
    result
}

// need function to vary client sending rate
pub fn test_client_rate() {
    let n = 8;

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let num_values: u64 = 250;
    let value_size: usize = 1000;
    
    for client_send_rate in [10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 10000000.0] {
        let test_type = format!("client_rate_{}_staleness", client_send_rate);
        let params = Param {ai_send_rate, client_send_rate, value_size, num_values};
        
        // spawn N servers
        // spawn 2 to kickstart cluster
        // bring servers back up, to see recovery
        let mut servers = Vec::new();
        let mut s_handles = Vec::new();
        let id = 0;
        let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                    format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                    &params);
        s_handles.push(s1.handle.spawn().unwrap());
        servers.push(s1);

        let id = 1;
        let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                    format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                    &params);
        s_handles.push(s2.handle.spawn().unwrap());
        servers.push(s2);

        // spawn remaining N-2 and have them point to first one
        for id in 2..n {
            let mut s = spawn_server(id, 8000+id, "127.0.0.1:8000".to_string(), 9000+id, 
                        format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                        &params);
            s_handles.push(s.handle.spawn().unwrap());
            servers.push(s);
        }

        // create N workloads for each client
        let mut workload_locs = Vec::new();
        for i in 0..n {

            // set up test parameters
            let id: u16 = i as u16;

            // generate workloads (only 1 for now)
            let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
            let mut workloads = Vec::new();
            workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});
            workload_locs.push(workload_loc.clone());

            // serialize workload
            serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
        }

        println!("MASTER - workloads made.");

        // spawn N clients with said workloads
        let mut clients = Vec::new();
        for id in 0..n {
            let port = 9000 + id;
            clients.push(
                spawn_client(id, format!("127.0.0.1:{}",port).to_string(), workload_locs[id as usize].to_string(), 
                                &params, test_type.to_string()).handle.spawn().unwrap()
            );
        }

        // join all clients
        for c in 0..n {
            clients[c as usize].wait().unwrap();
        }

        println!("All clients joined.");

        // go through each server, send a dump, if length is right, then join, else continue
        // do until list of servers is empty
        let mut server_conns = Vec::new();
        for id in 0..n {
            let conn = TcpStream::connect(format!("127.0.0.1:{}", 9000+id)).unwrap(); // declared out here so connection not dropped too quickly and we don't get err - connection reset by peer.
            server_conns.push(conn);
        }

        while servers.len() > 0 {
            // send dump
            let id = servers[0].id;
            
            println!("sending dump for id: {}", id);
            // get length
            let resp = dump_len_req(&mut server_conns[0]);

            println!("handling dump for id: {}", id);

            let len: usize = match resp {
                Message::DumpLenResp(len) => {
                    println!("extracting length of dump for id: {}, len: {}", id, len);
                    len
                },
                _ => {
                    println!("Execution should not have reached this point.");
                    continue;
                }
            };

            // join or continue based on length
            if len == (n*(num_values as u16)) as usize {
                println!("DUMPS SUCCESSFUL - REACHED DESIRED LENGTH OF {}.", len);

                // s_handles[0].kill().unwrap();
                // s_handles.remove(0);
                servers.remove(0);
                server_conns.remove(0);
            }

            thread::sleep(Duration::from_secs(1));
        }
    
        for mut server in s_handles {
            server.kill().unwrap();
        }

        thread::sleep(Duration::from_secs(5));
    }
}

// need function to vary AI rate
pub fn test_ai_rate() {
    let n = 8;

    // set up base parameters/things that don't vary
    let client_send_rate: f64 = 100.0;
    let num_values: u64 = 250;
    let value_size: usize = 1000;
    
    for ai_send_rate in [0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 10000.0, 25000.0, 10000000.0] {
        let test_type = format!("ai_rate_{}_staleness", ai_send_rate);
        let params = Param {ai_send_rate, client_send_rate, value_size, num_values};
        
        // spawn N servers
        // spawn 2 to kickstart cluster
        // bring servers back up, to see recovery
        let mut servers = Vec::new();
        let mut s_handles = Vec::new();
        let id = 0;
        let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                    format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                    &params);
        s_handles.push(s1.handle.spawn().unwrap());
        servers.push(s1);

        let id = 1;
        let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                    format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                    format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                    &params);
        s_handles.push(s2.handle.spawn().unwrap());
        servers.push(s2);

        // spawn remaining N-2 and have them point to first one
        for id in 2..n {
            let mut s = spawn_server(id, 8000+id, "127.0.0.1:8000".to_string(), 9000+id, 
                        format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                        &params);
            s_handles.push(s.handle.spawn().unwrap());
            servers.push(s);
        }

        // create N workloads for each client
        let mut workload_locs = Vec::new();
        for i in 0..n {

            // set up test parameters
            let id: u16 = i as u16;

            // generate workloads (only 1 for now)
            let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
            let mut workloads = Vec::new();
            workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});
            workload_locs.push(workload_loc.clone());

            // serialize workload
            serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
        }

        println!("MASTER - workloads made.");

        // spawn N clients with said workloads
        let mut clients = Vec::new();
        for id in 0..n {
            let port = 9000 + id;
            clients.push(
                spawn_client(id, format!("127.0.0.1:{}",port).to_string(), workload_locs[id as usize].to_string(), 
                                &params, test_type.to_string()).handle.spawn().unwrap()
            );
        }

        // join all clients
        for c in 0..n {
            clients[c as usize].wait().unwrap();
        }

        println!("All clients joined.");

        // go through each server, send a dump, if length is right, then join, else continue
        // do until list of servers is empty
        let mut server_conns = Vec::new();
        for id in 0..n {
            let conn = TcpStream::connect(format!("127.0.0.1:{}", 9000+id)).unwrap(); // declared out here so connection not dropped too quickly and we don't get err - connection reset by peer.
            server_conns.push(conn);
        }

        while servers.len() > 0 {
            // send dump
            let id = servers[0].id;
            
            println!("sending dump for id: {}", id);
            // get length
            let resp = dump_len_req(&mut server_conns[0]);

            println!("handling dump for id: {}", id);

            let len: usize = match resp {
                Message::DumpLenResp(len) => {
                    println!("extracting length of dump for id: {}, len: {}", id, len);
                    len
                },
                _ => {
                    println!("Execution should not have reached this point.");
                    continue;
                }
            };

            // join or continue based on length
            if len >= (n*(num_values as u16)) as usize {
                println!("DUMPS SUCCESSFUL - REACHED DESIRED LENGTH OF {}.", len);

                servers.remove(0);
                server_conns.remove(0);
            }

            thread::sleep(Duration::from_secs(1));
        }
    
        for mut server in s_handles {
            server.kill().unwrap();
        }

        thread::sleep(Duration::from_secs(5));
    }
}

// need function to test elastic scalability
pub fn test_elasticity() {

    let ns = vec![2, 4, 8, 12, 16, 32];
    
    // create a workload, something on the order of 500 elements, small size
    let test_type = "elastic_staleness";

    // set up base parameters/things that don't vary
    let ai_send_rate: f64 = 0.5;
    let client_send_rate: f64 = 100.0;
    let num_values: u64 = 10;
    let value_size: usize = 1000;
    let params = Param {ai_send_rate, client_send_rate, value_size, num_values};

    // spawn 2 to kickstart cluster
    let mut servers = Vec::new();
    let mut s_handles = Vec::new();
    let id = 0;
    let mut s1 = spawn_server(id, 8000, "127.0.0.1:8001".to_string(), 9000, 
                format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                &params);
    s_handles.push(s1.handle.spawn().unwrap());
    servers.push(s1);

    let id = 1;
    let mut s2 = spawn_server(id, 8001, "127.0.0.1:8000".to_string(), 9001, 
                format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                &params);
    s_handles.push(s2.handle.spawn().unwrap());
    servers.push(s2);

    // make a workload for every value of n
    let workload_loc = format!("/tmp/secko_testing/{}-{}-client_workload_test", test_type, id).to_string();
    let mut workloads = Vec::new();
    for i in 0..ns.len() {

        // set up test parameters
        let id: u16 = ns[i as usize] as u16;

        // generate workloads
        workloads.push(Workload {data: generate_batch(id, &params), params: params.clone()});
    }
    
    // serialize workload
    serialize_into(BufWriter::new(File::create(&workload_loc).unwrap()), &workloads).unwrap();
    
    // make a single client
    let server = "127.0.0.1:9000".to_string();
    let mut c = spawn_client(id, server, workload_loc, &params, test_type.to_string());
    let mut c_h = c.handle.spawn().unwrap();

    let mut total_vals = (num_values*2) as usize;

    for i in 1..ns.len() {
        let n = ns[i];

        println!("!!!!!!!!!!!!{}!!!!!!!!!!!!", n);

        // wait until full propagation (client will hang for us)
        let mut server_conns = Vec::new();
        for id in 0..ns[i-1] {
            let conn = TcpStream::connect(format!("127.0.0.1:{}", 9000+id)).unwrap(); // declared out here so connection not dropped too quickly and we don't get err - connection reset by peer.
            server_conns.push(conn);
        }

        let mut len: usize = 0;
        while len < total_vals {

            // get length
            let resp = dump_len_req(&mut server_conns[ns[i-1]-1]);

            len = match resp {
                Message::DumpLenResp(len) => {
                    len
                },
                _ => {
                    println!("Execution should not have reached this point.");
                    continue;
                }
            };

            // join or continue based on length
            if len == total_vals as usize {
                println!("DUMPS SUCCESSFUL - REACHED DESIRED LENGTH OF {}, for n={}.", len, n);
                total_vals += (n*num_values as usize) as usize;
                break;
            }

            thread::sleep(Duration::from_secs_f64(0.1));
        }

        // increase number of servers
        // spawn remaining N-2 and have them point to first one
        for id in n-1..n {
            let id = id as u16;
            let mut s = spawn_server(id, 8000+id, "127.0.0.1:8000".to_string(), 9000+id, 
                        format!("/tmp/secko_testing/{}-secko_commits{}", test_type, id).to_string(), 
                        format!("/tmp/secko_testing/{}-secko_snaps{}", test_type, id).to_string(),
                        &params);
            s_handles.push(s.handle.spawn().unwrap());
            servers.push(s);
        }

        thread::sleep(Duration::from_secs(10));
    }

    // at end, join client
    c_h.wait().unwrap();
    println!("Client joined.");

    // then kill servers
    for mut h in s_handles {
        h.kill().unwrap();
    }
    
}