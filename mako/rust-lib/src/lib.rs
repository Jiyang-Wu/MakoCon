use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::{Read, Write, BufWriter};
use bytes::Bytes;
use redis_protocol::resp3::{types::BytesFrame, types::DecodedFrame};
use socket2::{Socket, Domain, Type, Protocol};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;

mod resp3_handler;
use resp3_handler::Resp3Handler;

extern "C" {
    // GET/SET single-call interface returning an optional malloc'd buffer for GET
    fn cpp_execute_request_sync(
        op: u32,
        key_ptr: *const u8, key_len: usize,
        val_ptr: *const u8, val_len: usize,
        out_ptr: *mut *mut u8, out_len: *mut usize
    ) -> bool;

    // free buffer returned by cpp_execute_request (if any)
    fn cpp_free_buf(ptr: *mut u8, len: usize);

    // Called when each worker thread starts to initialize C++ thread-local state
    fn cpp_worker_thread_init(thread_id: usize);
}

#[derive(Copy, Clone)]
#[repr(u32)]
enum OpCode {
    Invalid = 0,
    Get     = 1,
    Set     = 2,
}

#[derive(Clone)]
struct Command {
    op: OpCode,
    key: Bytes,
    val: Option<Bytes>,
}

// ===== small helpers =====

#[inline]
fn ascii_eq_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    for (x, y) in a.iter().zip(b.iter()) {
        if x.to_ascii_lowercase() != y.to_ascii_lowercase() { return false; }
    }
    true
}

#[inline]
fn parse_opcode(name: &[u8]) -> OpCode {
    if ascii_eq_ci(name, b"GET") { OpCode::Get }
    else if ascii_eq_ci(name, b"SET") { OpCode::Set }
    else { OpCode::Invalid }
}

// Parse only GET/SET from a RESP3 frame, with zero string allocations.
// Everything else will be treated as "unsupported" for now.
fn parse_resp3(frame: DecodedFrame<BytesFrame>) -> Option<Command> {
    use BytesFrame::*;
    let f = frame.into_complete_frame().ok()?;
    let parts = match f {
        Array { data, .. } => data,
        _ => return None,
    };

    // command name
    let name = match parts.get(0) {
        Some(BlobString { data, .. }) | Some(SimpleString { data, .. }) => data.as_ref(),
        _ => return None,
    };

    let op = parse_opcode(name);
    match op {
        OpCode::Get => {
            if parts.len() < 2 { return None; }
            let key = match &parts[1] {
                BlobString { data, .. } | SimpleString { data, .. } => Bytes::copy_from_slice(data),
                _ => return None,
            };
            Some(Command { op, key, val: None })
        }
        OpCode::Set => {
            if parts.len() < 3 { return None; }
            let key = match &parts[1] {
                BlobString { data, .. } | SimpleString { data, .. } => Bytes::copy_from_slice(data),
                _ => return None,
            };
            let val = match &parts[2] {
                BlobString { data, .. } | SimpleString { data, .. } => Bytes::copy_from_slice(data),
                _ => return None,
            };
            Some(Command { op, key, val: Some(val) })
        }
        OpCode::Invalid => None,
    }
}

// ===== RESP writers (synchronous, no async) =====

#[inline]
fn write_simple_ok<W: Write>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"+OK\r\n")
}

#[inline]
fn write_nil_bulk<W: Write>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"$-1\r\n")
}

#[inline]
fn write_bulk<W: Write>(stream: &mut W, data: &[u8]) -> std::io::Result<()> {
    let mut buf = itoa::Buffer::new();
    stream.write_all(b"$")?;
    stream.write_all(buf.format(data.len()).as_bytes())?;
    stream.write_all(b"\r\n")?;
    stream.write_all(data)?;
    stream.write_all(b"\r\n")
}

#[inline]
fn write_err<W: Write>(stream: &mut W, msg: &str) -> std::io::Result<()> {
    stream.write_all(b"-ERR ")?;
    stream.write_all(msg.as_bytes())?;
    stream.write_all(b"\r\n")
}

// ===== FFI bridge for GET/SET =====

fn ffi_getset(cmd: &Command) -> Result<Option<&'static [u8]>, ()> {
    // Returns:
    //   Ok(Some(bytes)) for GET hit
    //   Ok(None)        for GET miss or SET success
    //   Err(())         for backend failure

    let (val_ptr, val_len) = if let Some(v) = &cmd.val {
        (v.as_ptr(), v.len())
    } else {
        (std::ptr::null(), 0)
    };

    let mut out_ptr: *mut u8 = std::ptr::null_mut();
    let mut out_len: usize = 0;

    let ok = unsafe {
        cpp_execute_request_sync(
            cmd.op as u32,
            cmd.key.as_ptr(), cmd.key.len(),
            val_ptr, val_len,
            &mut out_ptr, &mut out_len
        )
    };
    if !ok { return Err(()); }

    if out_len == 0 {
        Ok(None)
    } else {
        // we'll immediately write and free in the caller
        let slice = unsafe { std::slice::from_raw_parts(out_ptr, out_len) };
        let static_slice: &'static [u8] = unsafe { std::mem::transmute(slice) };
        Ok(Some(static_slice))
    }
}

// ===== Runtime + server =====

/// Creates a TcpListener with SO_REUSEPORT enabled (blocking mode).
/// This allows multiple sockets to bind to the same port, each with its own
/// kernel accept queue, enabling true parallel accept scaling.
fn create_reuseport_listener(addr: &str) -> std::io::Result<TcpListener> {
    let addr: SocketAddr = addr.parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;  // Key: enables multiple sockets on same port
    socket.set_nonblocking(false)?; // BLOCKING mode for synchronous I/O
    socket.set_nodelay(true)?;      // Disable Nagle's algorithm
    socket.bind(&addr.into())?;
    socket.listen(1024)?;           // Backlog of 1024 pending connections

    Ok(TcpListener::from(socket))
}

/// Thread-per-core architecture: Each OS thread has its own isolated socket.
/// 100% SYNCHRONOUS - no async, no Tokio runtime, pure blocking I/O.
///
/// Architecture:
/// ```text
///   OS Thread 0                 OS Thread 1                 OS Thread 2
///   ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
///   │ Blocking Socket │         │ Blocking Socket │         │ Blocking Socket │
///   │ (SO_REUSEPORT)  │         │ (SO_REUSEPORT)  │         │ (SO_REUSEPORT)  │
///   │                 │         │                 │         │                 │
///   │ accept() blocks │         │ accept() blocks │         │ accept() blocks │
///   │ read() blocks   │         │ read() blocks   │         │ read() blocks   │
///   │ write() blocks  │         │ write() blocks  │         │ write() blocks  │
///   └─────────────────┘         └─────────────────┘         └─────────────────┘
///           │                           │                           │
///           └───────────────────────────┴───────────────────────────┘
///                                       │
///                              Port 6380 (kernel distributes)
/// ```
#[no_mangle]
pub extern "C" fn rust_init(n_threads: usize) -> bool {
    let addr = "127.0.0.1:6380";

    // Barrier to synchronize all worker threads before accepting
    let barrier = Arc::new(Barrier::new(n_threads));

    // Counter for ready threads
    let ready_count = Arc::new(AtomicUsize::new(0));

    println!("Starting {} thread-per-core workers on {} (SO_REUSEPORT, 100% SYNC)",
             n_threads, addr);

    for thread_id in 0..n_threads {
        let barrier = Arc::clone(&barrier);
        let ready_count = Arc::clone(&ready_count);
        let addr = addr.to_string();

        std::thread::Builder::new()
            .name(format!("mako-worker-{}", thread_id))
            .spawn(move || {
                // Each thread creates its OWN listener with SO_REUSEPORT
                let listener = match create_reuseport_listener(&addr) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("[thread-{}] Failed to create listener: {e}", thread_id);
                        return;
                    }
                };

                // === Per-thread C++ initialization ===
                // Initialize C++ thread-local state (mbta_type::thread_init, etc.)
                unsafe { cpp_worker_thread_init(thread_id); }

                // Mark this thread as ready
                let count = ready_count.fetch_add(1, Ordering::SeqCst) + 1;

                // Wait for all threads to be ready
                barrier.wait();

                if thread_id == 0 {
                    println!("All {} threads ready, accepting connections on {}",
                             count, addr);
                }

                // Accept loop - 100% synchronous, blocking I/O
                loop {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            if let Err(e) = stream.set_nodelay(true) {
                                eprintln!("Failed to set TCP_NODELAY: {e}");
                            }

                            // Handle client SYNCHRONOUSLY - blocks until done
                            if let Err(e) = handle_client_sync(&mut stream) {
                                eprintln!("Client handling error: {e}");
                            }
                        }
                        Err(e) => {
                            eprintln!("[thread-{}] Accept error: {e}", thread_id);
                        }
                    }
                }
            })
            .expect("Failed to spawn worker thread");
    }

    true
}

/// Handle a client connection synchronously (100% blocking).
/// All reads, writes, and C++ calls block the thread until complete.
fn handle_client_sync(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(10 * 1024 * 1024);

    // Buffer size choice: 16KB read buffer (matches Redis PROTO_IOBUF_LEN)
    let mut read_buf = [0u8; 16384];

    // Buffer size choice: 16KB write buffer (matches Redis PROTO_REPLY_CHUNK_BYTES)
    let mut writer = BufWriter::with_capacity(16384, stream.try_clone()?);

    loop {
        // BLOCKING read - waits until data arrives
        match stream.read(&mut read_buf) {
            Ok(0) => break,  // Connection closed
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }

        // Process ALL available frames
        loop {
            match resp3.next_frame() {
                Ok(Some(frame)) => {
                    if let Some(cmd) = parse_resp3(frame) {
                        match cmd.op {
                            OpCode::Get => {
                                match ffi_getset(&cmd) {
                                    Err(_) => write_err(&mut writer, "backend")?,
                                    Ok(None) => write_nil_bulk(&mut writer)?,
                                    Ok(Some(bytes)) => {
                                        write_bulk(&mut writer, bytes)?;
                                        unsafe { cpp_free_buf(bytes.as_ptr() as *mut u8, bytes.len()) };
                                    }
                                }
                            }
                            OpCode::Set => {
                                match ffi_getset(&cmd) {
                                    Err(_) => write_err(&mut writer, "backend")?,
                                    Ok(_)  => write_simple_ok(&mut writer)?,
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        write_err(&mut writer, "unsupported command")?;
                    }
                }
                Ok(None) => break,  // No more complete frames
                Err(_) => {
                    write_err(&mut writer, "protocol error")?;
                    break;
                }
            }
        }

        // BLOCKING flush - waits until all data sent
        writer.flush()?;
    }

    Ok(())
}

