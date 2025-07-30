# rust-lib 

- **RESP3 Protocol**: Parses Redis commands using streaming decoder

## Dependencies

- `redis-protocol` (6.0.0) - RESP3 protocol parsing (https://crates.io/crates/redis-protocol/6.0.0)


## Architecture

1. TCP listener on `127.0.0.1:6380`
2. RESP3 command parsing via `Resp3Handler`
3. Request/response queues for C++ communication
4. Thread-per-client connection handling 