use autocxx::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;
use redis_protocol::resp3::{types::BytesFrame, types::DecodedFrame};

include_cpp!{
    #include "kv_store.h"
    safety!(unsafe_ffi)
    generate!("KVStore")
    // generate!("KVStore::set")
    // generate!("KVStore::get")
    // generate!("KVStore::del")
}

fn kv_handler(
    decoded: DecodedFrame<BytesFrame>,
    conn: &mut makocon::Conn,
    db: &Mutex<HashMap<Vec<u8>, Vec<u8>>>,
) {
    let frame = match decoded.into_complete_frame() {
        Ok(f) => f,
        Err(_) => {
            return;
        }
    };

    let parts = match frame {
        BytesFrame::Array { data, .. } => data,
        _ => {
            conn.write_error("ERR expected Array frame");
            return;
        }
    };

    let cmd = match parts.get(0) {
        Some(BytesFrame::BlobString { data, .. })
        | Some(BytesFrame::SimpleString { data, .. }) => data.clone().to_ascii_lowercase(),
        _ => {
            conn.write_error("ERR invalid command type");
            return;
        }
    };

    let mut store = ffi::KVStore::new().within_unique_ptr();

    match &cmd[..] {
        b"get" if parts.len() == 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let db = db.lock().unwrap();
                let key_vec: Vec<u8> = key.clone().into();
                match db.get(&key_vec) {
                    Some(val) => conn.write_bulk(val),
                    None => conn.write_null(),
                }

                // kv store related
                let key_string = String::from_utf8(key_vec).expect("UTF-8 error");
                cxx::let_cxx_string!(key_cxx = key_string);
                let mut val = ffi::make_string("");
                let found = unsafe {
                    store.pin_mut().get(&key_cxx, val.as_mut().unwrap())
                };
                if found {
                    let val_pin = val.as_mut().unwrap();
                    println!("value = {}", val_pin.to_string_lossy());
                }
            } else {
                conn.write_error("ERR invalid GET args");
            }
        }
        b"set" if parts.len() == 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: val, .. }) =
                    (&parts[1], &parts[2]) {
                let mut db = db.lock().unwrap();
                let key_vec: Vec<u8> = key.clone().into();
                let val_vec: Vec<u8> = val.clone().into();
                db.insert(key_vec.clone(), val_vec.clone());
                conn.write_string("OK");

                //kv store related
                let key_string = String::from_utf8(key_vec).expect("UTF-8 error");
                let val_string = String::from_utf8(val_vec).expect("UTF-8 error");
                cxx::let_cxx_string!(key_cxx = key_string);
                cxx::let_cxx_string!(val_cxx = val_string);
                unsafe { store.pin_mut().set(&key_cxx, &val_cxx) };
            } else {
                conn.write_error("ERR invalid SET args");
            }
        }
        b"del" if parts.len() == 2 => {
            if let BytesFrame::BlobString { data: key, ..} = &parts[1] {
                let mut db = db.lock().unwrap();
                let key_vec: Vec<u8> = key.clone().into();
                db.remove(&key_vec);
                conn.write_string("OK");
            } else {
                conn.write_error("ERR invalid DEL args");
            }
        }
        _ => {
            conn.write_error("ERR unsupported command or wrong arg count");
        }
    }
}
fn main() -> std::io::Result<()> {
    let db: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());
    let mut server = makocon::listen("127.0.0.1:6380", db).unwrap();
    server.command = Some(kv_handler);
    println!("Listening on {}", server.local_addr());
    server.serve().unwrap();
    Ok(())
}
