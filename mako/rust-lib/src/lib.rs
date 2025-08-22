use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use redis_protocol::resp3::{types::BytesFrame, types::DecodedFrame};

mod resp3_handler;
use resp3_handler::Resp3Handler;

const MAX_BLOCKING_THREADS: usize = 4;

// Request and Response structures to match C++ expectations
#[derive(Debug, Clone)]
pub struct RustResponse {
    pub result: String,
    pub success: bool,
}

// Global state - hybrid approach for FFI compatibility
static mut RUNTIME_HANDLE: Option<tokio::runtime::Handle> = None;
static DATABASE_MUTEX: Mutex<()> = Mutex::new(());


extern "C" {
    fn cpp_execute_request_sync(operation: *const std::os::raw::c_char, key: *const std::os::raw::c_char, value: *const std::os::raw::c_char, result: *mut *mut std::os::raw::c_char) -> bool;
    fn cpp_free_string(ptr: *mut std::os::raw::c_char);
    fn cpp_execute_batch_request_sync(batch_data: *const std::os::raw::c_char, result: *mut *mut std::os::raw::c_char) -> bool;
}


// Initialize Rust async runtime and channels
#[no_mangle]
pub extern "C" fn rust_init() -> bool {
    unsafe {
        // Create async runtime
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(MAX_BLOCKING_THREADS)
            .thread_name("mako-worker")
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to create tokio runtime: {}", e);
                return false;
            }
        };
        
        // Get runtime handle for spawning tasks
        RUNTIME_HANDLE = Some(rt.handle().clone());

        // Spawn async server in background thread with runtime
        std::thread::spawn(move || {
            rt.block_on(async {
                if let Err(e) = start_async_server().await {
                    eprintln!("Async server error: {}", e);
                }
            });
        });
        
        true
    }
}

async fn start_async_server() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("Async Rust server started on 127.0.0.1:6380");
    
    loop {
        let (stream, _) = listener.accept().await?;
        
        // Spawn async task for each client
        tokio::spawn(async move {
            if let Err(e) = handle_client_async(stream).await {
                eprintln!("Client handling error: {}", e);
            }
        });
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

async fn handle_client_async(mut stream: TcpStream) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(10 * 1024 * 1024); // 10MB internal buffer
    let mut read_buf = [0u8; 4096];
    let mut pipeline_buffer: Vec<(String, String, String)> = Vec::new();
    let mut in_transaction = false;
    
    loop {
        match stream.read(&mut read_buf).await {
            Ok(0) => break, // Connection closed
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }
        
        // Process frames one by one
        while let Ok(opt) = resp3.next_frame() {
            if let Some(frame) = opt {
                if let Some((operation, key, value)) = parse_resp3_command(frame) {
                    // Use spawn_blocking for C++ calls to avoid blocking the async runtime

                    if operation == "multi" {
                        in_transaction = true;
                        pipeline_buffer.clear();
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    } else if operation == "exec" {
                        in_transaction = false;
                        
                        // Execute all buffered operations as a batch
                        if !pipeline_buffer.is_empty() {
                            let batch_operations = pipeline_buffer.clone();
                            pipeline_buffer.clear();
                            
                            let response = tokio::task::spawn_blocking(move || {
                                call_cpp_batch_operation(batch_operations)
                            }).await.map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::Other, format!("Blocking task failed: {}", e))
                            })?;
                            
                            let exec_response = format_exec_response_from_batch(&response.result);
                            stream.write_all(exec_response.as_bytes()).await?;
                        } else {
                            // Empty transaction
                            stream.write_all(b"*0\r\n").await?;
                        }
                        continue;
                    } else if operation == "discard" {
                        in_transaction = false;
                        pipeline_buffer.clear();
                        stream.write_all(b"+OK\r\n").await?;
                        continue;
                    } else if operation == "watch" {
                        // Send watch command as single-item batch to C++ backend
                        let single_op = vec![(operation.clone(), key.clone(), value.clone())];
                        
                        let response = tokio::task::spawn_blocking(move || {
                            call_cpp_batch_operation(single_op)
                        }).await.map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, format!("Blocking task failed: {}", e))
                        })?;
                        
                        if response.success {
                            stream.write_all(b"+OK\r\n").await?;
                        } else {
                            stream.write_all(b"-ERR watch failed\r\n").await?;
                        }
                        continue;
                    } else if operation == "unwatch" {
                        // Send unwatch command as single-item batch to C++ backend
                        let single_op = vec![(operation.clone(), key.clone(), value.clone())];
                        
                        let response = tokio::task::spawn_blocking(move || {
                            call_cpp_batch_operation(single_op)
                        }).await.map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, format!("Blocking task failed: {}", e))
                        })?;
                        
                        if response.success {
                            stream.write_all(b"+OK\r\n").await?;
                        } else {
                            stream.write_all(b"-ERR unwatch failed\r\n").await?;
                        }
                        continue;
                    }
                    
                    // If in transaction, buffer the command and send QUEUED
                    if in_transaction {
                        pipeline_buffer.push((operation, key, value));
                        stream.write_all(b"+QUEUED\r\n").await?;
                    } else {
                        // Execute command immediately as single-item batch
                        let single_op = vec![(operation.clone(), key.clone(), value.clone())];
                        
                        let response = tokio::task::spawn_blocking(move || {
                            call_cpp_batch_operation(single_op)
                        }).await.map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, format!("Blocking task failed: {}", e))
                        })?;
                        
                        let response_str = format_response(&operation, &response);
                        // println!("Single operation response: {}", response_str.replace("\r\n", "\\r\\n"));
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

// Direct C++ operation call using spawn_blocking and unified execute function
fn call_cpp_batch_operation(operations: Vec<(String, String, String)>) -> RustResponse {
    // Use mutex to ensure thread-safe access to C++ database
    // Note: This runs on a blocking thread pool, not the main async runtime
    let _lock = DATABASE_MUTEX.lock().unwrap();
    
    let mut batch_data = String::new();
    for (i, (op, key, val)) in operations.iter().enumerate() {
        if i > 0 {
            batch_data.push_str("\r\n");
        }
        batch_data.push_str(op);
        batch_data.push_str("\r\n");
        batch_data.push_str(key);
        batch_data.push_str("\r\n");
        batch_data.push_str(val);
    }

    // println!("Batch request to C++: {}", batch_data.replace("\r\n", "\\r\\n"));
    unsafe {
        let batch_cstr = std::ffi::CString::new(batch_data).unwrap();
        let mut result_ptr: *mut std::os::raw::c_char = std::ptr::null_mut();
        
        let success = cpp_execute_batch_request_sync(
            batch_cstr.as_ptr(), 
            &mut result_ptr
        );
        
        let result = if success && !result_ptr.is_null() {
            let result_str = std::ffi::CStr::from_ptr(result_ptr).to_string_lossy().to_string();
            cpp_free_string(result_ptr);
            result_str
        } else {
            String::new()
        };
        
        RustResponse { result, success }
    }
}

fn format_exec_response_from_batch(batch_result: &str) -> String {
    // C++ returns results separated by '\r\n': "result1\r\nresult2\r\nresult3\r\n..."
    if batch_result.is_empty() {
        return "*0\r\n".to_string();
    }
    
    let parts: Vec<&str> = batch_result.split("\r\n").collect();
    let mut result = format!("*{}\r\n", parts.len());
    
    for part in parts {
        if part.is_empty() {
            result.push_str("$-1\r\n"); // NULL
        } else {
            result.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
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
                Some(("get".to_string(), key_string, "nil".to_string()))
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
                Some(("incr".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "DECR" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("decr".to_string(), key_string, "nil".to_string()))
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
                Some(("lpop".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "RPOP" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("rpop".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "LLEN" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("llen".to_string(), key_string, "nil".to_string()))
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
                Some(("hgetall".to_string(), key_string, "nil".to_string()))
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
            Some(("ping".to_string(), "nil".to_string(), "nil".to_string()))
        }
        "DEL" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("del".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "EXISTS" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("exists".to_string(), key_string, "nil".to_string()))
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
                Some(("ttl".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "KEYS" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: pattern, .. } = &parts[1] {
                let pattern_string = String::from_utf8_lossy(pattern).to_string();
                Some(("keys".to_string(), pattern_string, "nil".to_string()))
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
                Some(("smembers".to_string(), key_string, "nil".to_string()))
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
                Some(("scard".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        // Basic transaction commands (sequential execution, not true atomicity)
        "MULTI" => {
            Some(("multi".to_string(), "nil".to_string(), "nil".to_string()))
        }
        "EXEC" => {
            Some(("exec".to_string(), "nil".to_string(), "nil".to_string()))
        }
        "DISCARD" => {
            Some(("discard".to_string(), "nil".to_string(), "nil".to_string()))
        }
        "WATCH" if parts.len() >= 2 => {
            if let BytesFrame::BlobString { data: key, .. } = &parts[1] {
                let key_string = String::from_utf8_lossy(key).to_string();
                Some(("watch".to_string(), key_string, "nil".to_string()))
            } else {
                None
            }
        }
        "UNWATCH" => {
            Some(("unwatch".to_string(), "nil".to_string(), "nil".to_string()))
        }
        _ => None,
    }
}
