use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
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

// Global state - hybrid approach for FFI compatibility
static mut REQUEST_QUEUE: Option<Arc<Mutex<VecDeque<RustRequest>>>> = None;
static mut RESPONSE_CHANNELS: Option<Arc<Mutex<HashMap<u32, oneshot::Sender<RustResponse>>>>> = None;
static mut NEXT_ID: Option<Arc<AtomicU32>> = None;
static mut RUNTIME_HANDLE: Option<tokio::runtime::Handle> = None;

// Initialize Rust async runtime and channels
#[no_mangle]
pub extern "C" fn rust_init() -> bool {
    unsafe {
        // Create async runtime
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to create tokio runtime: {}", e);
                return false;
            }
        };
        
        // Get runtime handle for spawning tasks
        RUNTIME_HANDLE = Some(rt.handle().clone());
        
        // Create hybrid state - queue for FFI + channels for async
        REQUEST_QUEUE = Some(Arc::new(Mutex::new(VecDeque::new())));
        RESPONSE_CHANNELS = Some(Arc::new(Mutex::new(HashMap::new())));
        NEXT_ID = Some(Arc::new(AtomicU32::new(1)));
        
        let request_queue = REQUEST_QUEUE.as_ref().unwrap().clone();
        let response_channels = RESPONSE_CHANNELS.as_ref().unwrap().clone();
        let next_id = NEXT_ID.as_ref().unwrap().clone();
        
        // Spawn async server in background thread with runtime
        std::thread::spawn(move || {
            rt.block_on(async {
                if let Err(e) = start_async_server(request_queue, response_channels, next_id).await {
                    eprintln!("Async server error: {}", e);
                }
            });
        });
        
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

// Put response back via oneshot channel (called by C++)
#[no_mangle]
pub extern "C" fn rust_put_response_back_queue(id: u32, result: *const std::os::raw::c_char, success: bool) -> bool {
    unsafe {
        if let Some(channels_ref) = &RESPONSE_CHANNELS {
            let result_str = std::ffi::CStr::from_ptr(result).to_string_lossy().to_string();
            let response = RustResponse {
                id,
                result: result_str,
                success,
            };
            
            let mut channels = channels_ref.lock().unwrap();
            if let Some(sender) = channels.remove(&id) {
                match sender.send(response) {
                    Ok(_) => true,
                    Err(_) => {
                        eprintln!("Failed to send response for request {}", id);
                        false
                    }
                }
            } else {
                eprintln!("No response channel found for request {}", id);
                false
            }
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

// External C function to notify C++ of new requests
extern "C" {
    fn cpp_notify_request_available();
}

async fn start_async_server(
    request_queue: Arc<Mutex<VecDeque<RustRequest>>>,
    response_channels: Arc<Mutex<HashMap<u32, oneshot::Sender<RustResponse>>>>,
    next_id: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("Async Rust server started on 127.0.0.1:6380");
    
    loop {
        let (stream, _) = listener.accept().await?;
        let request_queue = request_queue.clone();
        let response_channels = response_channels.clone();
        let next_id = next_id.clone();
        
        // Spawn async task for each client (lightweight)
        tokio::spawn(async move {
            if let Err(e) = handle_client_async(stream, request_queue, response_channels, next_id).await {
                eprintln!("Client handling error: {}", e);
            }
        });
    }
}

async fn handle_client_async(
    mut stream: TcpStream,
    request_queue: Arc<Mutex<VecDeque<RustRequest>>>,
    response_channels: Arc<Mutex<HashMap<u32, oneshot::Sender<RustResponse>>>>,
    next_id: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(4096);
    let mut read_buf = [0u8; 4096];
    let mut pipeline_buffer: Vec<RustRequest> = Vec::new();
    let mut in_transaction = false;
    
    loop {
        match stream.read(&mut read_buf).await {
            Ok(0) => break, // Connection closed
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }
        
        // Process frames one by one but handle batching intelligently
        while let Ok(opt) = resp3.next_frame() {
            if let Some(frame) = opt {
                if let Some(parsed_request) = parse_resp3_command(frame) {
                    let req_id = next_id.fetch_add(1, Ordering::SeqCst);
                    
                    let request = RustRequest {
                        id: req_id,
                        operation: parsed_request.0.clone(),
                        key: parsed_request.1,
                        value: parsed_request.2,
                    };
                    
                    let operation = parsed_request.0.clone();
                    
                    // Handle transaction commands
                    if operation == "multi" {
                        in_transaction = true;
                        pipeline_buffer.clear();
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    } else if operation == "exec" {
                        in_transaction = false;
                        // Process all commands in pipeline_buffer and return array
                        let mut exec_responses = Vec::new();
                        for buffered_request in &pipeline_buffer {
                            // Add to request queue
                            {
                                let mut queue = request_queue.lock().unwrap();
                                queue.push_back(buffered_request.clone());
                            }
                            // Notify C++ of new request
                            unsafe { cpp_notify_request_available(); }
                            
                            // Create oneshot channel and wait for response
                            let (response_tx, response_rx) = oneshot::channel();
                            {
                                let mut channels = response_channels.lock().unwrap();
                                channels.insert(buffered_request.id, response_tx);
                            }
                            
                            let response = response_rx.await.map_err(|_| {
                                std::io::Error::new(std::io::ErrorKind::Other, "Response channel closed")
                            })?;
                            exec_responses.push(response);
                        }
                        pipeline_buffer.clear();
                        
                        // Send array response for EXEC
                        let exec_response_str = format_exec_response(exec_responses);
                        stream.write_all(exec_response_str.as_bytes()).await?;
                        continue;
                    } else if operation == "discard" {
                        in_transaction = false;
                        pipeline_buffer.clear();
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    } else if operation == "watch" {
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    } else if operation == "unwatch" {
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    }
                    
                    // If in transaction, buffer the command and send QUEUED
                    if in_transaction {
                        pipeline_buffer.push(request);
                        stream.write_all(b"+QUEUED\r\n").await?;
                    } else {
                        // Process command immediately
                        {
                            let mut queue = request_queue.lock().unwrap();
                            queue.push_back(request);
                        }
                        // Notify C++ of new request
                        unsafe { cpp_notify_request_available(); }
                        
                        // Create oneshot channel and wait for response
                        let (response_tx, response_rx) = oneshot::channel();
                        {
                            let mut channels = response_channels.lock().unwrap();
                            channels.insert(req_id, response_tx);
                        }
                        
                        let response = response_rx.await.map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::Other, "Response channel closed")
                        })?;
                        let response_str = format_response(&operation, &response);
                        stream.write_all(response_str.as_bytes()).await?;
                    }
                } else {
                    stream.write_all(b"-ERR invalid command format\r\n").await?;
                }
            } else {
                break;
            }
        }
    }
    
    Ok(())
}

// Old handle_client function removed - using async version

fn format_exec_response(responses: Vec<RustResponse>) -> String {
    let mut result = format!("*{}\r\n", responses.len());
    for response in responses {
        if response.success {
            if response.result.is_empty() {
                result.push_str("$-1\r\n"); // NULL
            } else {
                result.push_str(&format!("${}\r\n{}\r\n", response.result.len(), response.result));
            }
        } else {
            result.push_str("$-1\r\n"); // NULL for failed commands
        }
    }
    result
}

fn format_response(operation: &str, response: &RustResponse) -> String {
    if operation == "ping" {
        "+PONG\r\n".to_string()
    } else if operation == "multi" || operation == "discard" || 
              operation == "watch" || operation == "unwatch" {
        if response.success {
            format!("+{}\r\n", response.result)
        } else {
            "-ERR transaction command failed\r\n".to_string()
        }
    } else if response.result == "QUEUED" {
        "+QUEUED\r\n".to_string()
    } else if operation == "keys" && response.success {
        // Array response for KEYS command
        if response.result.is_empty() {
            "*0\r\n".to_string()
        } else {
            let keys: Vec<&str> = response.result.split(',').collect();
            let mut result = format!("*{}\r\n", keys.len());
            for key in keys {
                result.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
            }
            result
        }
    } else if operation == "hgetall" && response.success {
        // Array response for HGETALL command
        if response.result.is_empty() {
            "*0\r\n".to_string()
        } else {
            let pairs: Vec<&str> = response.result.split(',').collect();
            let mut result = format!("*{}\r\n", pairs.len() * 2);
            for pair in pairs {
                if let Some(colon_pos) = pair.find(':') {
                    let field = &pair[..colon_pos];
                    let value = &pair[colon_pos + 1..];
                    result.push_str(&format!("${}\r\n{}\r\n", field.len(), field));
                    result.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
            }
            result
        }
    } else if operation == "hmget" && response.success {
        // Array response for HMGET command
        if response.result.is_empty() {
            "*0\r\n".to_string()
        } else {
            let values: Vec<&str> = response.result.split(',').collect();
            let mut result = format!("*{}\r\n", values.len());
            for value in values {
                if value == "NULL" {
                    result.push_str("$-1\r\n");
                } else {
                    result.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                }
            }
            result
        }
    } else if operation == "smembers" || operation == "sinter" || operation == "sdiff" {
        // Array response for set operations
        if response.success {
            if response.result.is_empty() {
                "*0\r\n".to_string()
            } else {
                let members: Vec<&str> = response.result.split(',').collect();
                let mut result = format!("*{}\r\n", members.len());
                for member in members {
                    result.push_str(&format!("${}\r\n{}\r\n", member.len(), member));
                }
                result
            }
        } else {
            "*0\r\n".to_string()
        }
    } else if operation == "exists" || operation == "expire" || operation == "ttl" || 
              operation == "llen" || operation == "lpush" || operation == "rpush" ||
              operation == "incr" || operation == "decr" || operation == "incrby" || 
              operation == "decrby" || operation == "del" || operation == "hset" ||
              operation == "hdel" || operation == "hexists" || operation == "sadd" ||
              operation == "sismember" || operation == "scard" {
        // Integer response for commands that return numbers
        if response.success {
            format!(":{}\r\n", response.result)
        } else {
            "-ERR command failed\r\n".to_string()
        }
    } else if operation == "invalid" {
        "-ERR invalid command format\r\n".to_string()
    } else if response.success {
        if response.result.is_empty() {
            "$-1\r\n".to_string()
        } else {
            format!("${}\r\n{}\r\n", response.result.len(), response.result)
        }
    } else {
        "-ERR not found\r\n".to_string()
    }
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
        // Numeric operations
        "INCR" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("incr".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "DECR" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("decr".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "INCRBY" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: val, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let val_string = String::from_utf8_lossy(val).to_string();
                Some(("incrby".to_string(), key_string, val_string))
            } else {
                None
            }
        }
        "DECRBY" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: val, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let val_string = String::from_utf8_lossy(val).to_string();
                Some(("decrby".to_string(), key_string, val_string))
            } else {
                None
            }
        }
        // List operations
        "LPUSH" if parts.len() >= 3 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                // Collect all values after the key
                let mut values = Vec::new();
                for part in &parts[2..] {
                    if let BytesFrame::BlobString { data: val, .. } = part {
                        values.push(String::from_utf8_lossy(val).to_string());
                    }
                }
                if !values.is_empty() {
                    let val_string = values.join(","); // Join multiple values with comma
                    Some(("lpush".to_string(), key_string, val_string))
                } else {
                    None
                }
            } else {
                None
            }
        }
        "RPUSH" if parts.len() >= 3 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                // Collect all values after the key
                let mut values = Vec::new();
                for part in &parts[2..] {
                    if let BytesFrame::BlobString { data: val, .. } = part {
                        values.push(String::from_utf8_lossy(val).to_string());
                    }
                }
                if !values.is_empty() {
                    let val_string = values.join(","); // Join multiple values with comma
                    Some(("rpush".to_string(), key_string, val_string))
                } else {
                    None
                }
            } else {
                None
            }
        }
        "LPOP" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("lpop".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "RPOP" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("rpop".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "LLEN" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("llen".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "LRANGE" if parts.len() >= 4 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: start, .. },
                    BytesFrame::BlobString { data: stop, .. }) =
                    (&parts[1], &parts[2], &parts[3]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let start_str = String::from_utf8_lossy(start).to_string();
                let stop_str = String::from_utf8_lossy(stop).to_string();
                let range_str = format!("{},{}", start_str, stop_str);
                Some(("lrange".to_string(), key_string, range_str))
            } else {
                None
            }
        }
        // Hash operations
        "HSET" if parts.len() >= 4 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: field, .. },
                    BytesFrame::BlobString { data: val, .. }) =
                    (&parts[1], &parts[2], &parts[3]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let field_string = String::from_utf8_lossy(field).to_string();
                let val_string = String::from_utf8_lossy(val).to_string();
                let combined = format!("{}:{}", field_string, val_string);
                Some(("hset".to_string(), key_string, combined))
            } else {
                None
            }
        }
        "HGET" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: field, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let field_string = String::from_utf8_lossy(field).to_string();
                Some(("hget".to_string(), key_string, field_string))
            } else {
                None
            }
        }
        "HGETALL" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("hgetall".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "HDEL" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: field, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let field_string = String::from_utf8_lossy(field).to_string();
                Some(("hdel".to_string(), key_string, field_string))
            } else {
                None
            }
        }
        "HEXISTS" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: field, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let field_string = String::from_utf8_lossy(field).to_string();
                Some(("hexists".to_string(), key_string, field_string))
            } else {
                None
            }
        }
        "HMGET" if parts.len() >= 3 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                // Collect all field names after the key
                let mut fields = Vec::new();
                for part in &parts[2..] {
                    if let BytesFrame::BlobString { data: field, .. } = part {
                        fields.push(String::from_utf8_lossy(field).to_string());
                    }
                }
                if !fields.is_empty() {
                    let fields_string = fields.join(",");
                    Some(("hmget".to_string(), key_string, fields_string))
                } else {
                    None
                }
            } else {
                None
            }
        }
        "PING" => {
            Some(("ping".to_string(), String::new(), String::new()))
        }
        "DEL" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("del".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "EXISTS" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("exists".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "EXPIRE" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: ttl, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let ttl_string = String::from_utf8_lossy(ttl).to_string();
                Some(("expire".to_string(), key_string, ttl_string))
            } else {
                None
            }
        }
        "TTL" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("ttl".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "KEYS" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: pattern, .. } = &parts[1] {
                let pattern_string = String::from_utf8_lossy(pattern).to_string();
                Some(("keys".to_string(), pattern_string, String::new()))
            } else {
                None
            }
        }
        // Set operations
        "SADD" if parts.len() >= 3 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                // Collect all values after the key
                let mut values = Vec::new();
                for part in &parts[2..] {
                    if let BytesFrame::BlobString { data: val, .. } = part {
                        values.push(String::from_utf8_lossy(val).to_string());
                    }
                }
                if !values.is_empty() {
                    let val_string = values.join(",");
                    Some(("sadd".to_string(), key_string, val_string))
                } else {
                    None
                }
            } else {
                None
            }
        }
        "SMEMBERS" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("smembers".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "SISMEMBER" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key, .. },
                    BytesFrame::BlobString { data: member, .. }) =
                    (&parts[1], &parts[2]) {
                let key_string = String::from_utf8_lossy(key).to_string();
                let member_string = String::from_utf8_lossy(member).to_string();
                Some(("sismember".to_string(), key_string, member_string))
            } else {
                None
            }
        }
        "SINTER" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key1, .. },
                    BytesFrame::BlobString { data: key2, .. }) =
                    (&parts[1], &parts[2]) {
                let key1_string = String::from_utf8_lossy(key1).to_string();
                let key2_string = String::from_utf8_lossy(key2).to_string();
                Some(("sinter".to_string(), key1_string, key2_string))
            } else {
                None
            }
        }
        "SDIFF" if parts.len() >= 3 => {
            if let (BytesFrame::BlobString { data: key1, .. },
                    BytesFrame::BlobString { data: key2, .. }) =
                    (&parts[1], &parts[2]) {
                let key1_string = String::from_utf8_lossy(key1).to_string();
                let key2_string = String::from_utf8_lossy(key2).to_string();
                Some(("sdiff".to_string(), key1_string, key2_string))
            } else {
                None
            }
        }
        "SCARD" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("scard".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        // Basic transaction commands (sequential execution, not true atomicity)
        "MULTI" => {
            Some(("multi".to_string(), String::new(), String::new()))
        }
        "EXEC" => {
            Some(("exec".to_string(), String::new(), String::new()))
        }
        "DISCARD" => {
            Some(("discard".to_string(), String::new(), String::new()))
        }
        "WATCH" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("watch".to_string(), key_string, String::new()))
            } else {
                None
            }
        }
        "UNWATCH" => {
            Some(("unwatch".to_string(), String::new(), String::new()))
        }
        _ => None,
    }
}

// Old wait_for_response function removed - using oneshot channels