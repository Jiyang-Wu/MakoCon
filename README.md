Connection server for Mako project

# build

## kv_store 
A kv_store implementation as a library. Basic logic: starting a long-run thread, and communicate with rust via request and response queue.
```
cd ./makocon/third-party
make clean
make
make test
./test_kv
```

## redis-client
```
cd ./testing_client/redis_rs_client
cargo clean
cargo build
```

## makocon
```
cd makocon
cargo clean
cargo build
```

## testing
```
# start a rust server, listening on :6380
./target/debug/makocon

# start a redis client
./target/debug/redis_rs_client
```