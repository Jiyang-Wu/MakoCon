use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};
use std::net::TcpListener;
use std::io::{Read, Write};
use redis_protocol::resp3::{types::BytesFrame, types::DecodedFrame};

mod resp3_handler;
use resp3_handler::Resp3Handler;

// Request and Response structures to match C++ expectations
#[derive(Debug, Clone)]
pub struct RustRequest {
    pub id: u32,
    pub operation: String,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct RustResponse {
    pub id: u32,
    pub result: String,
    pub success: bool,
}

// Global state for the queues
static mut REQUEST_QUEUE: Option<Arc<Mutex<VecDeque<RustRequest>>>> = None;
static mut RESPONSE_QUEUE: Option<Arc<Mutex<HashMap<u32, RustResponse>>>> = None;
static mut NEXT_ID: Option<Arc<AtomicU32>> = None;
static mut LISTENER_HANDLE: Option<std::thread::JoinHandle<()>> = None;

// Initialize Rust socket listener and queues
#[no_mangle]
pub extern "C" fn rust_init() -> bool {
    unsafe {
        REQUEST_QUEUE = Some(Arc::new(Mutex::new(VecDeque::new())));
        RESPONSE_QUEUE = Some(Arc::new(Mutex::new(HashMap::new())));
        NEXT_ID = Some(Arc::new(AtomicU32::new(1)));
        
        let request_queue = REQUEST_QUEUE.as_ref().unwrap().clone();
        let response_queue = RESPONSE_QUEUE.as_ref().unwrap().clone();
        let next_id = NEXT_ID.as_ref().unwrap().clone();
        
        // Start socket listener thread
        let handle = std::thread::spawn(move || {
            if let Err(e) = start_socket_listener(request_queue, response_queue, next_id) {
                eprintln!("Socket listener error: {}", e);
            }
        });
        
        LISTENER_HANDLE = Some(handle);
        true
    }
}

// Retrieve request from queue (called by C++)
#[no_mangle]
pub extern "C" fn rust_retrieve_request_from_queue(id: *mut u32, operation: *mut *mut std::os::raw::c_char, key: *mut *mut std::os::raw::c_char, value: *mut *mut std::os::raw::c_char) -> bool {
    unsafe {
        if let Some(queue_ref) = &REQUEST_QUEUE {
            let mut queue = queue_ref.lock().unwrap();
            if let Some(request) = queue.pop_front() {
                *id = request.id;
                
                // Allocate C strings
                let op_cstring = std::ffi::CString::new(request.operation).unwrap();
                let key_cstring = std::ffi::CString::new(request.key).unwrap();
                let val_cstring = std::ffi::CString::new(request.value).unwrap();
                
                *operation = op_cstring.into_raw();
                *key = key_cstring.into_raw();
                *value = val_cstring.into_raw();
                
                return true;
            }
        }
        false
    }
}

// Put response back to queue (called by C++)
#[no_mangle]
pub extern "C" fn rust_put_response_back_queue(id: u32, result: *const std::os::raw::c_char, success: bool) -> bool {
    unsafe {
        if let Some(queue_ref) = &RESPONSE_QUEUE {
            let result_str = std::ffi::CStr::from_ptr(result).to_string_lossy().to_string();
            let response = RustResponse {
                id,
                result: result_str,
                success,
            };
            
            let mut queue = queue_ref.lock().unwrap();
            queue.insert(id, response);
            true
        } else {
            false
        }
    }
}

// Free C strings allocated in retrieve_request_from_queue
#[no_mangle]
pub extern "C" fn rust_free_string(ptr: *mut std::os::raw::c_char) {
    unsafe {
        if !ptr.is_null() {
            let _ = std::ffi::CString::from_raw(ptr);
        }
    }
}

fn start_socket_listener(
    request_queue: Arc<Mutex<VecDeque<RustRequest>>>,
    response_queue: Arc<Mutex<HashMap<u32, RustResponse>>>,
    next_id: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380")?;
    println!("Rust socket listener started on 127.0.0.1:6380");
    
    for stream in listener.incoming() {
        let stream = stream?;
        let request_queue = request_queue.clone();
        let response_queue = response_queue.clone();
        let next_id = next_id.clone();
        
        std::thread::spawn(move || {
            if let Err(e) = handle_client(stream, request_queue, response_queue, next_id) {
                eprintln!("Client handling error: {}", e);
            }
        });
    }
    
    Ok(())
}

fn handle_client(
    mut stream: std::net::TcpStream,
    request_queue: Arc<Mutex<VecDeque<RustRequest>>>,
    response_queue: Arc<Mutex<HashMap<u32, RustResponse>>>,
    next_id: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(4096);
    let mut read_buf = [0u8; 4096];
    
    loop {
        match stream.read(&mut read_buf) {
            Ok(0) => break, // Connection closed
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }
        
        while let Ok(opt) = resp3.next_frame() {
            if let Some(frame) = opt {
                if let Some(parsed_request) = parse_resp3_command(frame) {
                    let req_id = next_id.fetch_add(1, Ordering::SeqCst);
                    
                    let request = RustRequest {
                        id: req_id,
                        operation: parsed_request.0,
                        key: parsed_request.1,
                        value: parsed_request.2,
                    };
                    
                    // Add to request queue
                    {
                        let mut queue = request_queue.lock().unwrap();
                        queue.push_back(request);
                    }
                    
                    // Wait for response
                    let response = wait_for_response(&response_queue, req_id);
                    
                    // Send response back to client
                    let response_str = if response.success {
                        if response.result.is_empty() {
                            "$-1\r\n".to_string() // NULL bulk string
                        } else {
                            format!("${}\r\n{}\r\n", response.result.len(), response.result)
                        }
                    } else {
                        "-ERR not found\r\n".to_string()
                    };
                    
                    stream.write_all(response_str.as_bytes())?;
                } else {
                    stream.write_all(b"-ERR invalid command format\r\n")?;
                }
            } else {
                break;
            }
        }
    }
    
    Ok(())
}

fn parse_resp3_command(decoded: DecodedFrame<BytesFrame>) -> Option<(String, String, String)> {
    let frame = match decoded.into_complete_frame() {
        Ok(f) => f,
        Err(_) => return None,
    };

    let parts = match frame {
        BytesFrame::Array { data, .. } => data,
        _ => return None,
    };

    let cmd = match parts.get(0) {
        Some(BytesFrame::BlobString { data, .. })
        | Some(BytesFrame::SimpleString { data, .. }) => {
            String::from_utf8_lossy(data).to_uppercase()
        }
        _ => return None,
    };

    match &cmd[..] {
        "GET" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("get".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "SET" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: val, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let val_string = String::from_utf8_lossy(val).to_string();
                Some(("set".to_string(), key_string, val_string))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn wait_for_response(response_queue: &Arc<Mutex<HashMap<u32, RustResponse>>>, request_id: u32) -> RustResponse {
    loop {
        {
            let mut queue = response_queue.lock().unwrap();
            if let Some(response) = queue.remove(&request_id) {
                return response;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}