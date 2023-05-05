extern crate ocaml;
use std::net::TcpStream;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

unsafe impl ocaml::ToValue for KeyVal {
    fn to_value(&self, gc: &ocaml::Runtime) -> ocaml::Value {
        unsafe { 
            let mut tup = ocaml::Value::alloc_tuple(2);
            
            tup.store_field(gc, 0, ocaml::Value::string::<&str>(&self.0.key.to_string()));
            tup.store_field(gc, 1, ocaml::Value::string::<&str>(&self.0.value)); // i spent several hours trying to get this to work with int64. unfortunately there is no documentation on how to unpack it in ocaml correctly. strings are the only thing that work so i use that
            
            tup
        }
    }
  }
  
unsafe impl ocaml::FromValue for KeyVal {
    fn from_value(value: ocaml::Value) -> KeyVal {
        unsafe { 
            KeyVal(KVPair {
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
        Err(e) => return Err(ocaml::Error::Message(string_to_static_str(e))),
    };

    // check message type
    match result {
        Message::ConnectionClosed => Err(ocaml::Error::Message("Remote closed unexpectedly.")),
        Message::Error(e) => Err(ocaml::Error::Message(string_to_static_str(e))),
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