# Mako KV Store

A high-performance key-value store implementation with a hybrid C++/Rust architecture where C++ handles the main logic and storage while Rust manages socket communication and request queuing.

## Architecture

The Mako KV Store uses a unique hybrid architecture:

- **C++ Main Process**: Handles the main application logic, key-value storage, and request processing
- **Rust Socket Listener**: Manages TCP socket connections and maintains request/response queues
- **Inter-language Communication**: C++ polls Rust for incoming requests and sends responses back

### Architecture Flow

1. **C++ main()** initializes the KV store and starts a polling thread
2. **rust_init()** is called to start the Rust socket listener on port 6380
3. **Rust listener** accepts client connections and parses Redis protocol commands
4. **Request queuing**: Rust adds parsed requests to an internal queue with unique IDs
5. **C++ polling**: C++ continuously polls Rust for new requests via `rust_retrieve_request_from_queue()`
6. **Request processing**: C++ executes the request (GET/PUT operations) on its internal storage
7. **Response handling**: C++ sends the result back to Rust via `rust_put_response_back_queue()`
8. **Client response**: Rust waits for the response and sends it back to the client

## Request Format

The system supports Redis-compatible commands with the format: `{operation}:{key}:{value}`

- **GET requests**: `get:key_name:`
- **SET requests**: `set:key_name:value_data`

## Building

### Prerequisites

- Rust (latest stable version)
- C++ compiler with C++17 support (g++/clang++)
- CMake (version 3.16 or higher)
- netcat (for testing)

### Build Steps

```bash
# 1. Build Rust library manually
cargo build --release

# Navigate to the mako directory
cd mako

# Create build directory and configure
mkdir build
cd build
cmake ..

# Or build with specific number of jobs
make -j$(nproc)
```

### Start the Server

```bash
./build/mako_server
```

The server will:
- Initialize the C++ KV store
- Start the Rust socket listener on `127.0.0.1:6380`
- Begin polling for requests
- Display status messages

### Testing with Redis Clients

You can test the server using any Redis client or simple telnet/netcat:

#### Using Redis CLI
```bash
redis-cli -p 6380
> SET mykey myvalue
OK
> GET mykey
"myvalue"
```

## Cleanup

```bash
# Stop the server (Ctrl+C)

# Clean build artifacts
cd build
make clean

# Clean everything including Rust artifacts
make clean_all
```

## Comparison with Original Implementation

| Aspect | Original (makocon) | New (mako) |
|--------|-------------------|------------|
| Main Process | Rust | C++ |
| Socket Handling | Rust | Rust |
| Storage Logic | C++ (via AutoCXX) | C++ |
| Queue Management | C++ internal | Rust managed |
| Communication | Direct function calls | C interface with polling |
| Request Flow | Rust → C++ → Rust | Rust → C++ (poll) → Rust |

This new architecture provides clearer separation of concerns while maintaining the performance benefits of both languages.