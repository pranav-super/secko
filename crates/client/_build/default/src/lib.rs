// use std::io::Cursor;
// use prost::Message;
extern crate ocaml;
use std::net::TcpStream;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// #[ocaml::func]   // This is needed to make the function compatible with OCaml
// #[ocaml::sig("string -> string")] /// This is used to generate the OCaml bindings
// pub unsafe fn read_my_very_custom_file(filename: String) -> String {
//     std::fs::read_to_string(filename).unwrap()
// }

use secko_messages::{FoundValue, Message, KVPair, send_message, receive_message};
use crate::ocaml::{ToValue, FromValue};

// https://zshipko.github.io/ocaml-rs/03_writing_ocaml_functions_in_rust.html#opaque-types
#[ocaml::sig]
pub struct Db {
    con: TcpStream
}
ocaml::custom!(Db);

pub struct KeyVal(KVPair);

#[derive(ToValue, FromValue)]
#[ocaml::sig("Present of string | NotPresent")]
pub enum Lookup {
    Present(String),
    NotPresent,
}

// #[ocaml::sig]
// pub struct CamlKV {
//     key_low_32: i64,
//     key_high_32: i64,
//     value: ocaml::Pointer<Val>, //ocaml-rs func macro doesn't allow lifetimes! need to copy :). in any case: 'ToValue for str and &[u8] creates a new value' so i guess it was inevitable
// }
// ocaml::custom!(CamlKV);

// #[ocaml::sig]
// pub struct Val {
//     value: String,
// }
// ocaml::custom!(Val);

unsafe impl ocaml::ToValue for KeyVal {
    fn to_value(&self, gc: &ocaml::Runtime) -> ocaml::Value {
        unsafe { 
            let mut tup = ocaml::Value::alloc_tuple(2);
            // first entry is first 32 bits, then second 32 bits, then the value. ocaml cannot handle u64 without complaining and this is our workaround.

            // let bytes = ocaml::Value::bytes::<[u8; 8]>(self.0.key.to_ne_bytes());
            // println!("{:#?}", self.0.key.to_ne_bytes());
            // println!("{:#?}", bytes);
            // keys come from hasher as u64. ocaml can only represent i64, so if all bits are filled we have 2's complement overflow which causes an error
            // safest thing to do is to transfer between the two as bytes 
            // tup.store_field(gc, 0, bytes);
            // again, needs usize to be 64 bits
            // let low32: i64 = (self.0.key as i32) as i64;
            // let high32: i64 = (self.0.key >> 32) as i64;

            // println!("{}, {}\n", high32, low32);

            tup.store_field(gc, 0, ocaml::Value::string::<&str>(&self.0.key.to_string()));
            tup.store_field(gc, 1, ocaml::Value::string::<&str>(&self.0.value)); // i spent several hours trying to get this to work with int64. unfortunately there is no documentation on how to unpack it in ocaml correctly. strings are the only thing that work so i use that
            // tup.store_field(gc, 2, ocaml::Value::int64(low32));
            // tup.store_field(gc, 3, ocaml::Value::int64(high32));

            // println!("{:#?}", tup);

            tup
        }
    }
  }
  
unsafe impl ocaml::FromValue for KeyVal {
    fn from_value(value: ocaml::Value) -> KeyVal {
        unsafe { 
            // let key = (value.field(2).int64_val() << 32) + (value.field(1).int64_val());
            KeyVal(KVPair {
                // key: (key as u64), 
                key: value.field(0).string_val().to_string().parse().unwrap(), // will always be a u64 as we are using this only in dump
                value: value.field(1).string_val().to_string() 
            }) 
        }
    }
}


#[ocaml::func]
#[ocaml::sig("string -> int -> db")]
pub unsafe fn init(address: &str, port: ocaml::Int) -> Result<ocaml::Pointer<Db>, ocaml::Error> {
    let stream = TcpStream::connect(address.to_owned() + ":" + &port.to_string())?;
    Ok(Db { con: stream }.into())
}

#[ocaml::func]
#[ocaml::sig("db -> string -> unit")]
pub unsafe fn push(db: &mut Db, value: String) -> Result<(), ocaml::Error> {

    // make key for it
    let mut hash = DefaultHasher::new();
    value.to_string().hash(&mut hash);
    let hashed: u64 = hash.finish(); 

    // send the message
    let data = Message::PushReq ( KVPair {key: hashed, value: value} );
    match send_message(&mut db.con, data) {
        Ok(()) => (),
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),
    };

    // get the response
    let result: Message = match receive_message(&mut db.con) {
        Ok(msg) => msg,
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
    };

    // check message type
    match result {
        Message::ConnectionClosed => Err(ocaml::Error::Message("Remote closed unexpectedly.")),
        Message::Error(e) => Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
        Message::PushResp{success: true} => Ok(()),
        Message::PushResp{success: false} => Err(ocaml::Error::Message("Failed to push keys successfully")),
        other => Err(ocaml::Error::Message(string_to_static_str(format!("Unexpected response received from remote: {}", other)))),
    }

}

#[ocaml::func]
#[ocaml::sig("db -> string -> lookup")]
pub unsafe fn get(db: &mut Db, key: String) -> Result<ocaml::Pointer<Lookup>, ocaml::Error> {
    // convert the string to a u64. if not a u64, throw error
    let key: u64 = match key.parse() {
        Ok(v) => v,
        Err(_) => return Err(ocaml::Error::Message("Please pass a valid u64 as a string.")),
    };

    // send the message
    let data = Message::RetrieveReq { key: key };
    match send_message(&mut db.con, data) {
        Ok(()) => (),
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),
    };

    // get the response
    let result: Message = match receive_message(&mut db.con) {
        Ok(msg) => msg,
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
    };

    // check message type
    match result {
        Message::ConnectionClosed => Err(ocaml::Error::Message("Remote closed unexpectedly.")),
        Message::Error(e) => Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
        Message::RetrieveResp{ result: FoundValue::Success { value } } => Ok(Lookup::Present(value).to_value(gc).into()),
        Message::RetrieveResp{ result: FoundValue::Failure} => Ok(Lookup::NotPresent.to_value(gc).into()),//Err(ocaml::Error::Message("Key not found.")),
        other => Err(ocaml::Error::Message(string_to_static_str(format!("Unexpected response received from remote: {:#?}", other)))),
    }

}

#[ocaml::func]
#[ocaml::sig("db -> 'keyval array")]
pub unsafe fn dump(db: &mut Db) -> Result<Vec<ocaml::Pointer<KeyVal>>, ocaml::Error> {

    // send the message
    let data = Message::DumpReq;
    match send_message(&mut db.con, data) {
        Ok(()) => (),
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),
    };

    // get the response 
    let result: Message = match receive_message(&mut db.con) {
        Ok(msg) => msg,
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
    };

    // check message type
    match result {
        Message::ConnectionClosed => Err(ocaml::Error::Message("Remote closed unexpectedly.")),
        Message::Error(e) => Err(ocaml::Error::Message(string_to_static_str(e))),//Err(ocaml::Error::Message(&e)),
        Message::DumpResp(v) => {
            let mut returned: Vec<ocaml::Pointer<KeyVal>> = Vec::new();
            for elem in v {
                returned.push(KeyVal(KVPair { key: elem.key, value: elem.value }).to_value(gc).into());
                //returned.push(CamlKV{ key_low_32: (elem.key as i32) as i64, key_high_32: (elem.key >> 32) as i64, value: Val { value: elem.value }.into() }.into());
            }
            Ok(returned)
        },
        other => Err(ocaml::Error::Message(string_to_static_str(format!("Unexpected response received from remote: {:#?}", other)))),
    }

}

// for error messages: https://stackoverflow.com/questions/23975391/how-to-convert-a-string-into-a-static-str
// i do not like this but ocaml-rs has put me in this position...hopefully ocaml's garbage collection deals with this well?
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}


/*// Include the `items` module, which is generated from items.proto.
pub mod clientserver {
    include!(concat!(env!("OUT_DIR"), "/clientserver.rs"));
}
use clientserver::*;

pub mod antientropy {
    include!(concat!(env!("OUT_DIR"), "/antientropy.rs"));

    struct Node {
        id: i32,
        ip: String,
    }

    impl Node {
        fn new(id: i32, ip: String) {
            // Node { id, ip }
        }

        // fn convert_to_protobuf
    }

    pub fn getDigest(map: LockFreeMap) {
        let digest_data = Vec::new();
        // for pair in map.iter() {
        //     key: ?, value: Vec<u64> = pair.key(), pair.value();
        //     digest_data.push
        // }
    }

    // pub impl DigestFwd {
    //     pub fn new() 

    //     pub fn convert_to_protobuf

    //     pub fn serialize

    //     pub fn deserialize
    // }
}*/
// use antientropy::*;

// // fn create_replica_info(id: i32, ip: String) -> digest_fwd::ReplicaInfo {
// //     let mut node = Node::default();
// //     node.id = id;
// //     node.ip = ip;
// //     let mut ri = digest_fwd::ReplicaInfo::default();
// //     ri.replica = Some(node);
// //     ri.numkeys = 27;
// //     ri
// // }

// // pub fn create_digest_forward() -> DigestFwd {
// //     let mut fwd = DigestFwd::default();
// //     fwd.digest_data = vec![create_replica_info(123, "10.0.0.5".to_string()), 
// //                             create_replica_info(124, "10.0.0.6".to_string()),
// //                             create_replica_info(125, "10.0.0.7".to_string())];
// //     fwd
// // }

// // pub fn create_cluster_req() -> ClusterReq {
// //     let req = ClusterReq::default();
// //     req
// // }

// // pub fn serialize_req(req: &ClusterReq) -> Vec<u8> {
// //     let mut buf = Vec::new();
// //     buf.reserve(req.encoded_len());
// //     // Unwrap is safe, since we have reserved sufficient capacity in the vector.
// //     req.encode(&mut buf).unwrap();
// //     buf
// // }

// // pub fn deserialize_req(buf: &[u8]) -> Result<ClusterReq, prost::DecodeError> {
// //     ClusterReq::decode(&mut Cursor::new(buf))
// // }