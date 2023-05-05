use std::{
    io::{BufRead, BufReader, BufWriter, Write}, //to read and write from the stream
    net::{TcpListener, TcpStream, SocketAddrV4},
    sync::{mpsc::{self}, Arc, Mutex, RwLock},
    env,
    thread,
    time::{Duration, SystemTime},
    hash::{Hash, Hasher},
    collections::{hash_map::DefaultHasher, HashSet, HashMap},
    fs::{File, OpenOptions, metadata},
    os::unix::fs::FileExt, process::exit,
};
use chrono::offset::Utc;
use chrono::DateTime; // https://stackoverflow.com/questions/45386585/how-to-format-systemtime-to-string

use atomic_counter::{AtomicCounter, RelaxedCounter}; // want to effectively share a reference that can be modified by one thread and we don't care about ordering or up to date in other thread, but just using arc wont work as mutex needed, just using mut wont work as we can be interrupted mid add, so using an atomic
// generally atomic is more light weight https://stackoverflow.com/questions/15056237/which-is-more-efficient-basic-mutex-lock-or-atomic-integer
// don't require strong ordering. simply need to read a pretty recent version of the value (https://cfsamsonbooks.gitbook.io/explaining-atomics-in-rust/)
use bincode::{serialize_into, deserialize_from};

mod threadpool;
use threadpool::ThreadPool;

use secko_messages::{ClusterNode, UpdateMessage, Message, FoundValue, KVPair, DigestPair, ReplicaId, Key, send_message, receive_message};

use secko_server::{Commit, u64_to_socketaddr, socketaddr_to_u64, create_digest, map::LockFreeMap};

// https://github.com/clap-rs/clap/blob/master/examples/escaped-positional.rs
use clap::{arg, Arg, ArgAction, command, value_parser};

use rand::{seq::SliceRandom, thread_rng};

fn main() {
    // parse arguments for address, commit log filename, and snapshot filename
    let matches = command!()
        .arg(Arg::new("ai_ip")) // for antientropy
        .arg(Arg::new("neighbors").action(ArgAction::Append)) // for antientropy
        .arg(arg!(binding: -b <CLIENTBINDING>).value_parser(value_parser!(String))) // general
        .arg(arg!(rate: -r <ANTIENTROPYRATE>).value_parser(value_parser!(String))) // for antientropy
        .arg(arg!(commit: -c <COMMITLOGFILE>).value_parser(value_parser!(String))) // for persistence
        .arg(arg!(snapshot: -s <SNAPSHOTFILE>).value_parser(value_parser!(String))) // for persistence
        .get_matches();

    // save parameters pertaining to antientropy
    let myip = match matches.get_one::<String>("ai_ip").unwrap().trim().parse::<SocketAddrV4>() {
        Ok(addr) => addr,
        Err(_) => exit(1)
    };

    let send_rate_init: f64 = match matches.get_one::<String>("rate") {
        Some(c) => {
            c.trim().parse::<f64>().expect(&format!("Messed up parsing argument {}", c).to_string())
        },
        None => 1.0 // default rate
    };

    let neighbor_strs = matches
        .get_many::<String>("neighbors")
        .unwrap_or_default()
        .map(|v| v.trim().to_owned())
        .collect::<Vec<_>>();

    if neighbor_strs.len() == 0 {
        println!("Must specify at least 1 neighbor. Exiting...");
        exit(1);
    }

    let neighbors_addrs: Vec<SocketAddrV4> = neighbor_strs.iter().filter_map(|s| {
        match s.trim().parse::<SocketAddrV4>() {
            Ok(addr) => Some(addr),
            Err(_) => None
        }
    }).collect();

    // save (or set defaults for) parameters pertaining to persistence
    let binding: &str = match matches.get_one::<String>("binding") {
        Some(c) => c.trim(),
        None => "127.0.0.1:9000" // default address to receieve client updates
    };

    let commit_log_filename: &str = match matches.get_one::<String>("commit") {
        Some(c) => c.trim(),
        None => "/tmp/commit_log.txt" // default commit log destination
    };

    let snapshot_filename: String = match matches.get_one::<String>("snapshot") {
        Some(s) => s.trim().to_string(),
        None => "/tmp/secko_snapshot".to_string() // default snapshot location
    };

    // create the lock-free hashmap (effectively a ctrie afaik in that it’s implemented much like a HAMT with lock-free capabilities)
    // custom implementation allowing for serialization so that we can make snapshots
    // wrapped in an arc as its reference will be shared across threads
    let map: Arc<LockFreeMap<String>>;

    // from that, construct a default replica map, for antientropy purposes
    let replica_map: Arc<LockFreeMap<Mutex<Vec<Key>>>> = Arc::new(LockFreeMap::new());

    // either deserialize, if a backed up file exists...
    if metadata(&snapshot_filename).is_ok() {
        map = match File::open(&snapshot_filename) {
            Ok(file) => {
                match deserialize_from(&mut BufReader::new(file)) {
                    Ok(snap) => Arc::new(snap),
                    Err(e) => {
                        println!("Backup file deserialization failed with error: {}", e);
                        return; // fail, as this is unexpected behavior
                    }
                }
            },
            Err(e) => {
                println!("Backup file failed to open with error: {}", e);
                return; // fail, as this is unexpected behavior
            }
        };
        // show us what exists 
        // println!("Found entries:");
        // for ref_multi in map.iter() {
        //     println!("{} -> {}", ref_multi.key(), ref_multi.val());
        // }
    }
    else {
        println!("Creating backup file using provided name. Creating new map from scratch.");
        map = Arc::new(LockFreeMap::new());
    }

    let my_replica_id: ReplicaId = socketaddr_to_u64(&myip);

    // populate replica map with self (get vector of currently held keys and add it)
    let current_keys: Vec<Key> = map.iter().map(|x| *x.key()).collect::<Vec<_>>();
    replica_map.insert(my_replica_id, Arc::new(Mutex::new(current_keys)));

    // populate the replica map with neighbors
    for neighbor in neighbors_addrs.iter() {
        replica_map.insert(socketaddr_to_u64(&neighbor), Arc::new(Mutex::new(Vec::new())));
    }

    // open commit file, find number of existing commits
    let num_commits: usize;
    // println!("{} {}", commit_log_filename.trim(), metadata(commit_log_filename.trim()).is_ok());
    if metadata(commit_log_filename).is_ok() {
        num_commits = BufReader::new(File::open(commit_log_filename).unwrap()).lines().count()-1;
    } else {
        num_commits = 0;

        // create file
        let mut line_adder = OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(commit_log_filename)
            .unwrap();

        // add the line
        line_adder.write(&"Snapshotted Until Line: 0000000".as_bytes()).unwrap();
    }
    // if num_commits < 0 { // create file prior to running
    //     // add the line
    //     let mut line_adder = OpenOptions::new()
    //         .append(true)
    //         .open(commit_log_filename)
    //         .unwrap();
        
    //     line_adder.write(&"Snapshotted Until Line: 0000000".as_bytes()).unwrap();

    //     num_commits = 0;
    // }
    // let num_commits: usize = num_commits as usize; // will now be > 0
    println!("starting number of commits is: {}", num_commits);

    // using above info, make atomic counter for persister to use
    let counter_p: Arc<RelaxedCounter> = Arc::new(RelaxedCounter::new(num_commits)); // +1 because we want a new commit to start at. but keeping numcommits as the old value, just num lines - 1 very sufficient, as we only care in that case about the number of lines being considered
    let counter_s: Arc<RelaxedCounter> = counter_p.clone();

    // create appender handle, which is used to append to the file as often as possible. This is used by the persister thread, to add commits
    let file_appender = OpenOptions::new()
        .append(true)
        .open(&commit_log_filename)
        .unwrap();

    // create snapshotter handle, which is just used to update that number at the top.
    let commit_log_updater = OpenOptions::new()
        .write(true)
        .open(&commit_log_filename)
        .unwrap();

    // roll through from last commit
    let mut commit_unroller: BufReader<File> = BufReader::new(File::open(&commit_log_filename).unwrap());
    
    // get commit last snapshotted
    let mut first_line = String::new();
    commit_unroller.read_line(&mut first_line).unwrap();
    let last_snapshotted_commit: usize = first_line[24..].trim().parse().unwrap();
    let lines: Vec<String> = commit_unroller.lines().collect::<Result<_, _>>().unwrap();
    println!("Snapshots go up until {}", last_snapshotted_commit);

    // roll through the log from there
    for c in last_snapshotted_commit..num_commits {
        let (key, value) = lines[c].split_once(' ').unwrap();
        
        // add to map
        let parsed_key = match key.parse::<Key>() {
            Ok(k) => k,
            Err(_) => continue
        }; 
        map.insert(parsed_key, Arc::new(value.to_owned()));
    }

    println!("unrolled commits");//, now map contains:");
    // for ref_multi in map.iter() {
    //     println!("{} -> {}", ref_multi.key(), ref_multi.val());
    // }

    println!("binding: {:?}, myip: {:?}", binding, myip);

    // client listener for actual clients
    let client_listener = match TcpListener::bind(binding) {
        Ok(listen) => listen,
        Err(error) => panic!("Problem binding for client listening - {:?}", error),
    };

    // antientropy listener for other servers/members of cluster
    let ai_listener = match TcpListener::bind(myip) {
        Ok(listen) => listen,
        Err(error) => panic!("Problem binding for antientropy - {:?}", error),
    };

    // attempt connecting to one of the hosts initially specified in the replica map, because we need at least one connection going to even proceed
    let mut can_connect: bool = false;
    while !can_connect {
        for neighbor_addr in neighbors_addrs.iter() {
            match TcpStream::connect(neighbor_addr) {
                Ok(_) =>  {
                    can_connect = true;
                    break;
                },
                Err(_) => continue
            };
        }
    };

    // create client handling pool
    let client_pool = ThreadPool::new(8);
    
    // START PERSISTENCE PROCESS

    // Create mpsc
    let (tx, rx) = mpsc::channel();
    let map_snapshot_ref: Arc<LockFreeMap<String>> = map.clone();

    // dedicate one thread to committing ("persisting")
    let persister_handle = thread::Builder::new().name("p".to_string()).spawn(move || persister(counter_p, file_appender, rx));

    // dedicate one thread to snapshotting
    let snapshotter_handle = thread::Builder::new().name("p".to_string()).spawn(move || snapshotter(counter_s, commit_log_updater, map_snapshot_ref, snapshot_filename));

    // create pools for antientropy digest/update receipts 
    let digest_receipt_pool = ThreadPool::new(8);
    let update_receipt_pool = ThreadPool::new(8);

    let replica_map_ref = replica_map.clone();
    let replica_map_ref_ai = replica_map.clone();

    // fixed rate of update of every n seconds for now. mutable if we implement AIMD and transmission rate stuff in the future
    let update_rate = Arc::new(RwLock::new(send_rate_init)); // make a param

    let srd = update_rate.clone();

    // spawn thread to send digest to neighbors
    let digest_forward_handle = thread::Builder::new().name("df".to_string()).spawn(move || {
        
        // println!("here3");
        
        loop {
            let digest_forward_replica_ref = replica_map_ref.clone();

            // pick random peer - https://stackoverflow.com/questions/34215280/how-can-i-randomly-select-one-element-from-a-vector-or-array
            let peers: Vec<ReplicaId> = digest_forward_replica_ref.iter().map(|kv| kv.0).filter(|k| *k != my_replica_id).collect::<Vec<ReplicaId>>();
            // println!("peers: {:?}", peers);
            let peer = match peers.choose(&mut rand::thread_rng()) {
                Some(p) => p,
                None => {
                    thread::sleep(Duration::from_secs_f64(*srd.read().unwrap()));
                    continue; // no peers!
                }
            };

            // create digest
            let digest: Vec<DigestPair> = create_digest(digest_forward_replica_ref);
            // println!("My Digest: {:?}, for {}", digest, u64_to_socketaddr(*peer));

            // send digest
            let mut conn = match TcpStream::connect(u64_to_socketaddr(*peer)) {
                Ok(c) => c,
                Err(_) => {
                    println!("Try in a bit...");
                    thread::sleep(Duration::from_secs_f64(*srd.read().unwrap()));
                    continue;
                } 
            };
            send_message(&mut conn, Message::DigestMessage(my_replica_id, digest)).unwrap();

            // sleep for update rate seconds
            thread::sleep(Duration::from_millis(((1.0 / *srd.read().unwrap())*1000.0) as u64));
        }
    });

    let clh_tx_clone = tx.clone();
    let clh_map_clone = Arc::clone(&map);
    let client_listener_handle = thread::Builder::new().name("clh".to_string()).spawn(move || {
        // iterate through each connection, very simply!
        for stream in client_listener.incoming() {
            let stream = stream.unwrap();
            let map_clone = Arc::clone(&clh_map_clone);
            let replica_map_clone = Arc::clone(&replica_map);
            let tx_clone = clh_tx_clone.clone();

            // invoke a thread from the pool, run the closure within
            client_pool.execute(move || {
                handle_request(stream, map_clone, replica_map_clone, my_replica_id, tx_clone);
            });
        }
    });

    let alh_tx_clone = tx.clone();
    let alh_map_clone = Arc::clone(&map);
    let ai_listener_handle = thread::Builder::new().name("alh".to_string()).spawn(move || {
        // iterate through each connection, very simply!
        for stream in ai_listener.incoming() {
            // println!("here2");
            let mut stream = stream.unwrap();

            // get the value
            let message = match receive_message(&mut stream) {
                Ok(msg) => msg,
                Err(e) => {
                    println!("Digest Message Receipt failed with Error: {}", e);
                    continue;
                }
            };

            let mc = Arc::clone(&alh_map_clone);
            let rep = replica_map_ref_ai.clone();
            let sr = update_rate.clone();
            let tx_clone = alh_tx_clone.clone();

            // now act
            match message {
                // if it is a digest, then put onto appropriate pool
                Message::DigestMessage(id, digest) => {
                    // println!("received digest from {}", id);
                    digest_receipt_pool.execute(move || {
                        handle_digest(stream, mc, rep, id, my_replica_id, digest, sr);
                    });
                }
                
                // if it is an update, then put onto appropriate pool
                Message::UpdateMessage(id, update) => {
                    // println!("received update from {}", id);
                    update_receipt_pool.execute(move || {
                        handle_update(stream, mc, rep, my_replica_id, id, update, tx_clone.clone());
                    });
                }

                _ => {
                    println!("Unexpected message received.");
                    match send_message(&mut stream, Message::Error("Invalid Message Sent.".to_string())) {
                        _ => ()
                    };
                }
            };
        };
    });

    digest_forward_handle.unwrap().join().unwrap();
    persister_handle.unwrap().join().unwrap();
    snapshotter_handle.unwrap().join().unwrap();
    client_listener_handle.unwrap().join().unwrap();
    ai_listener_handle.unwrap().join().unwrap();
}

fn handle_request(mut stream: TcpStream, map: Arc<LockFreeMap<String>>, replica_map: Arc<LockFreeMap<Mutex<Vec<Key>>>>, local_replica_id: ReplicaId, queue: mpsc::Sender<Commit>) {
    loop {
        // println!("entering loop");
        let message = match receive_message(&mut stream) {
            Ok(msg) => msg,
            Err(e) => {
                println!("Failed with Error: {}", e);
                return;
            }
        };
        // println!("[{:#?}] Request: {:#?}", thread::current().id(), message);

        match message {
            Message::PushReq(KVPair {key, value}) => {
                // println!("Pushing key-value pair from client...");

                // make sure the hash is correct
                let mut hash = DefaultHasher::new();
                value.to_string().hash(&mut hash);
                let hashed: Key = hash.finish(); 

                if hashed != key {
                    // write response
                    let resp = Message::Error("Hash of value doesn't match.".to_string());
                    send_message(&mut stream, resp).unwrap();
                    // thread::sleep(time::Duration::from_secs(5)); // not a problem as it relegates this functionality to persister thread
                }
                else {
                    // add to map
                    let map_val = Arc::new(value);
                    let queue_val = map_val.clone();
                    let result = map.insert(key, map_val);

                    // write response
                    let resp = Message::PushResp{ success: true };
                    send_message(&mut stream, resp).unwrap();
                    
                    match result {
                        Some(_) => (), // already in map, don't commit
                        None => {
                            // add to commit log
                            queue.send(Commit{key: hashed, value: queue_val, timestamp: SystemTime::now()}).unwrap(); //new, so send to persister, want to do after response to reduce staleness

                            // update local replica map entry for this node
                            match replica_map.get(&local_replica_id) {
                                Some(lookup) => {
                                    lookup.val().lock().unwrap().push(hashed);
                                }
                                None => {
                                    // didn't find own key in replica map - should be impossible
                                    println!("Replica map is missing self key...returning...");
                                    return;
                                }
                            };
                        }
                    }
                }
            },

            Message::RetrieveReq { key } => {
                let lookup = map.get(&key);

                let resp = match lookup {
                    Some(v) => {
                        // expensive copy needed because cannot serialize otherwise for sending..., even with feature flags: https://serde.rs/feature-flags.html
                        Message::RetrieveResp{ result: FoundValue::Success { value: v.val().to_string() }}
                    }
                    None => {
                        Message::RetrieveResp{ result: FoundValue::Failure }
                    }
                };

                // write response
                send_message(&mut stream, resp).unwrap();

            },

            Message::DumpReq => { // can fail at a certain size on client side
                // make vector
                let mut dumped: Vec<KVPair> = Vec::new();

                // iterate through store
                for pair in map.iter() {
                    dumped.push(KVPair { key: *pair.key(), value: pair.val().to_string() })
                }

                // return a DumpResp
                let resp = Message::DumpResp(dumped);

                // write response
                send_message(&mut stream, resp).unwrap();
            },

            Message::DumpLenReq => {
                // get length
                let len = map.iter().count();

                // return a DumpLenResp
                let resp = Message::DumpLenResp(len);

                // write response
                send_message(&mut stream, resp).unwrap();
            },

            Message::ClusterReq => {
                // collect all nodes "lossily"
                let nodes: Vec<ClusterNode> = replica_map.iter().map(|x| ClusterNode{replica_id: u64_to_socketaddr(*x.key()).to_string()}).collect();

                // return a ClusterResp
                let resp = Message::ClusterResp(nodes);

                // write response
                send_message(&mut stream, resp).unwrap();
            },

            Message::ConnectionClosed => {
                return;
            },

            _ => {
                println!("unrecognized message type");
                return;
            }
        }
    }
}

// handles antientropy digests
fn handle_digest(mut _stream: TcpStream, map: Arc<LockFreeMap<String>>, replica_map: Arc<LockFreeMap<Mutex<Vec<Key>>>>, sender: ReplicaId, local_replica_id: ReplicaId, mut digest: Vec<DigestPair>, sending_rate: Arc<RwLock<f64>>) {
    let mut keys: HashSet<Key> = HashSet::new();
    let mut host_keys: HashMap<ReplicaId, Vec<(Key, usize)>> = HashMap::new();

    // shuffle the order of replicas in the digest
    digest.shuffle(&mut thread_rng());

    // now if we cut off this iteratione early due to MTU limits we don't necessarily starve anything
    for pair in digest.iter() {
        if let Some(local_copy) = replica_map.get(&pair.replica_id) { // if replica_id is in our map
            let cached = local_copy.val().lock().unwrap().clone();
            let len = cached.len(); //due to eventual consistency, race condition that follows is okay
            if len > pair.keys && pair.replica_id == sender {
                // If message is from replica and its digest says it has 0 or any value lower than we we have for it, update local replica map to say they have 0
                local_copy.val().lock().unwrap().clear();
            }
            else if len > pair.keys {
                // message cannot be too big
                if keys.len() >= 250 {
                    break;
                }

                let mut v: Vec<(Key, usize)> = Vec::new();
                // run into issues with size of message, so cap it at a point. Breaks around 330 keys being sent, we will pull this down to like 50 in an update. depends on value size, so maybe should be mtu based.
                // if len-pair.keys > 50 {
                //     for i in pair.keys..pair.keys+50 {
                //         keys.insert(cached[i]);
                //         v.push((cached[i], i));
                //     }
                // }
                // else {
                //     for i in pair.keys..len {
                //         keys.insert(cached[i]);
                //         v.push((cached[i], i));
                //     }
                // }
                for i in pair.keys..len {
                    if keys.len() >= 250 {
                        break;
                    }
                    keys.insert(cached[i]);
                    v.push((cached[i], i));
                }
                host_keys.insert(pair.replica_id, v);
            }
        }
        else {
            // add new
            replica_map.insert(pair.replica_id, Arc::new(Mutex::new(Vec::new())));
        }
    }

    // go through set and make kv pairs
    let kvpairs: Vec<KVPair> = keys.iter().filter_map(|x| {
        match map.get(x) {
            Some(val) => Some(KVPair{key: *x, value: val.val().to_string()}),
            None => {
                panic!("Messed up because of key {} while responding to digest from {}. Digest was {:?}, keys were {:?}.", *x, sender, digest, keys);
            }
        }
    }).collect(); //unwrap safe here as the key is necessarily in the map if we identified it earlier - we only go through keys we know we have and we never delete

    // construct struct
    let resp_struct: UpdateMessage = UpdateMessage { sending_rate: *sending_rate.read().unwrap() as f64, replica_keys: host_keys, key_values: kvpairs };

    // send response
    match send_message(&mut TcpStream::connect(u64_to_socketaddr(sender)).unwrap(), Message::UpdateMessage(local_replica_id, resp_struct)){
        Ok(()) => return,
        Err(s) => println!("Failed sending with {}", s)
    };
}

// handles antientropy updates
fn handle_update(mut _stream: TcpStream, map: Arc<LockFreeMap<String>>, replica_map: Arc<LockFreeMap<Mutex<Vec<u64>>>>, local_replica_id: ReplicaId, _sender: ReplicaId, update: UpdateMessage, queue: mpsc::Sender<Commit>) {
    // Add key-value pairs first, and in doing so update our replica map’s copy of self too
    for kvpair in update.key_values.iter() {
        // add to map
        let map_val = Arc::new(kvpair.value.to_string());
        let queue_val = map_val.clone();
        
        match map.insert(kvpair.key, map_val) {
            Some(_) => (),
            None => {
                // add to commit log
                queue.send(Commit{key: kvpair.key, value: queue_val, timestamp: SystemTime::now()}).unwrap(); //new, so send to persister, want to do after response to reduce staleness

                // add to local replica map's copy of self too
                replica_map.get(&local_replica_id).unwrap().val().lock().unwrap().push(kvpair.key);
            }
        }
    } 

    // Update replica map by index. So go through each replica id
    for entry in update.replica_keys.iter() {
        // check if entry exists
        if *entry.0 != local_replica_id {
            match replica_map.get(&entry.0) {
                Some(vec_guard) => {
                    // add keys by index, so go to end of list, then iterate through the received keys which should be around n-2 until you find one we don’t have, then add add add
                    let mut vec = vec_guard.val().lock().unwrap();
                    for (k, i) in entry.1 {
                        if i >= &vec.len() {
                            vec.push(*k);
                        }
                    }
                },
                None => {
                    // If there is a host we don’t recognize, add it here with its elements, and a lock. Make a whole struct and add to the replica map. we would necessarily have added all keys...
                    replica_map.insert(*entry.0, Arc::new(Mutex::new(entry.1.iter().map(|x| x.0).collect()))); // useful to hear about strangers/distant people if we don't hear directly yet
                }
            }
        }
    }

}

// persists to commit log really taking advantage of the lockfree + add-only semantics
fn persister(counter: Arc<RelaxedCounter>, mut f: File, queue: mpsc::Receiver<Commit>) {
    for commit in queue {
        // println!("just committed {:#?}", commit);
        // let datetime: DateTime<Utc> = commit.timestamp.into();
        f.write(&format!("\n{} -> {}", commit.key, commit.value).as_bytes()).unwrap();
        counter.inc();
    }
}

// persists to a full copy every n seconds or so. really taking advantage of the lockfree + add-only semantics
fn snapshotter(counter: Arc<RelaxedCounter>, f: File, map: Arc<LockFreeMap<String>>, path: String) {
    loop {
        thread::sleep(Duration::from_secs(5));

        // save current commit id or nearest one prior to serializing
        let str_counter: String = counter.get().to_string();
        
        // seralize it
        let mut ser = BufWriter::new(File::create(&path).unwrap());
        match serialize_into(&mut ser, &(*map)) {
            Ok(_) => (), //println!("Snapshot successfully written!"),
            Err(e) => println!("{}", e)
        };
        drop(ser); // need to do this otherwise it won't work right!

        // update log saying how much has been persisted
        f.write_at(&str_counter.as_bytes(), (31-str_counter.len()).try_into().unwrap()).unwrap();
        // println!("Snapshotted: {}", str_counter);
    }
}