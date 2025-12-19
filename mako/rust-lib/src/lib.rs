use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::BufWriter;
use tokio::io::AsyncWrite;
use bytes::{Bytes, BytesMut};
use redis_protocol::resp3::{types::BytesFrame, types::DecodedFrame};
use redis_protocol::error::RedisProtocolError;

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

// ===== RESP writers (no big String formatting) =====

#[inline]
async fn write_simple_ok<W: AsyncWrite + Unpin>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"+OK\r\n").await
}

#[inline]
async fn write_nil_bulk<W: AsyncWrite + Unpin>(stream: &mut W) -> std::io::Result<()> {
    stream.write_all(b"$-1\r\n").await
}

#[inline]
async fn write_bulk<W: AsyncWrite + Unpin>(stream: &mut W, data: &[u8]) -> std::io::Result<()> {
    let mut buf = itoa::Buffer::new();
    stream.write_all(b"$").await?;
    stream.write_all(buf.format(data.len()).as_bytes()).await?;
    stream.write_all(b"\r\n").await?;
    stream.write_all(data).await?;
    stream.write_all(b"\r\n").await
}

#[inline]
async fn write_err<W: AsyncWrite + Unpin>(stream: &mut W, msg: &str) -> std::io::Result<()> {
    stream.write_all(b"-ERR ").await?;
    stream.write_all(msg.as_bytes()).await?;
    stream.write_all(b"\r\n").await
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

#[no_mangle]
pub extern "C" fn rust_init(n_threads: usize) -> bool {
    let max_blocking = 4;

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(n_threads)                 // single-thread event loop like Redis
        .max_blocking_threads(max_blocking)
        .thread_name("mako-worker")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => { eprintln!("Failed to create tokio runtime: {e}"); return false; }
    };

    std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = start_async_server().await {
                eprintln!("Async server error: {e}");
            }
        });
    });

    true
}

async fn start_async_server() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("Async Rust server started on 127.0.0.1:6380");

    let use_db = true;

    loop {
        let (stream, _) = listener.accept().await?;
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("Failed to set TCP_NODELAY: {e}");
        }
        tokio::spawn({
            let use_db = use_db;
            async move {
                let res = if use_db {
                    handle_client_async(stream).await      // original, with DB
                } else {
                    handle_client_async_nodb(stream).await // new, no DB
                };
                if let Err(e) = res {
                    eprintln!("Client handling error: {e}");
                }
            }
        });
    }
}

async fn handle_client_async(stream: TcpStream) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(10 * 1024 * 1024);
    
    // Buffer size choice: 16KB read buffer
    // Reference: Redis uses PROTO_IOBUF_LEN = 16384 (16KB) in networking.c
    // See: https://github.com/redis/redis/blob/unstable/src/networking.c
    // Rationale: Large enough to read many pipelined commands in one syscall,
    // but small enough to avoid excessive memory overhead per client
    let mut read_buf = [0u8; 16384];

    // Split stream into reader and writer, wrap writer in buffer
    let (mut reader, writer) = stream.into_split();
    
    // Buffer size choice: 16KB write buffer
    // Reference: Redis uses PROTO_REPLY_CHUNK_BYTES = 16384 for reply buffers
    // See: https://github.com/redis/redis/blob/unstable/src/networking.c
    // Rationale: Batches multiple responses together to amortize syscall overhead.
    // With redis-benchmark -P 1000, this allows ~100-200 responses per flush
    // depending on response sizes (simple OK vs bulk strings)
    let mut writer = BufWriter::with_capacity(16384, writer);

    loop {
        // Read once - in pipelined mode this may contain hundreds of commands
        match reader.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }

        // CRITICAL CHANGE: Process ALL available frames without intermediate flushes
        loop {
            match resp3.next_frame() {
                Ok(Some(frame)) => {
                    if let Some(cmd) = parse_resp3(frame) {
                        match cmd.op {
                            OpCode::Get => {
                                match ffi_getset(&cmd) {
                                    Err(_) => write_err(&mut writer, "backend").await?,
                                    Ok(None) => write_nil_bulk(&mut writer).await?,
                                    Ok(Some(bytes)) => {
                                        write_bulk(&mut writer, bytes).await?;
                                        unsafe { cpp_free_buf(bytes.as_ptr() as *mut u8, bytes.len()) };
                                    }
                                }
                            }
                            OpCode::Set => {
                                match ffi_getset(&cmd) {
                                    Err(_) => write_err(&mut writer, "backend").await?,
                                    Ok(_)  => write_simple_ok(&mut writer).await?,
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        write_err(&mut writer, "unsupported command").await?;
                    }
                }
                Ok(None) => break,  // No more complete frames available
                Err(_) => {
                    write_err(&mut writer, "protocol error").await?;
                    break;
                }
            }
        }
        
        // CRITICAL CHANGE: Single flush after processing entire batch
        // Original code flushed every 100 operations AND after each read batch.
        // New behavior: Only flush after exhausting all parseable frames from current buffer.
        // This matches Redis's event loop pattern and is essential for pipeline performance.
        writer.flush().await?;
    }
    
    Ok(())
}

async fn handle_client_async_nodb(stream: TcpStream) -> std::io::Result<()> {
    let mut resp3 = Resp3Handler::new(10 * 1024 * 1024);
    let mut read_buf = [0u8; 16384];

    let (mut reader, writer) = stream.into_split();
    let mut writer = BufWriter::with_capacity(16384, writer);

    // Dummy value to return for GET hits (8 bytes, like your -d 8)
    const DUMMY_VALUE: &[u8] = b"AAAAAAAA";

    loop {
        // Read a batch of pipelined commands
        match reader.read(&mut read_buf).await {
            Ok(0) => break,
            Ok(n) => resp3.read_bytes(&read_buf[..n]),
            Err(e) => return Err(e),
        }

        loop {
            match resp3.next_frame() {
                Ok(Some(frame)) => {
                    if let Some(cmd) = parse_resp3(frame) {
                        match cmd.op {
                            OpCode::Get => {
                                write_bulk(&mut writer, DUMMY_VALUE).await?;
                            }
                            OpCode::Set => {
                                write_simple_ok(&mut writer).await?;
                            }
                            _ => {
                                write_err(&mut writer, "unsupported command").await?;
                            }
                        }
                    } else {
                        write_err(&mut writer, "unsupported command").await?;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    write_err(&mut writer, "protocol error").await?;
                    break;
                }
            }
        }

        writer.flush().await?;
    }

    Ok(())
}
