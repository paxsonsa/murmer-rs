# murmer - A distributed actor framework for Rust

### Build, Test, & Run
```sh
cargo build
cargo run nextest

# In separate terminals, run seed nodes:
RUST_LOG=debug cargo run --bin cluster_test seed 8001
# In another terminal, run a node that joins the cluster:
RUST_LOG=debug cargo run --bin cluster_test join 8002 127.0.0.1:8001
```


