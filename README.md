# kjdb - Keyed JSON Database

A database engine that operates on JSON lines.

When all you need is an async, atomic, in-process, disk-persistent, key-value store and sqlite is too much work. Works with any serde serializable type. No database schema, only `#[derive(Serialize, Deserialize)]`.

```rust
use kjdb::Database;

// Open or create a new database, define the value as `String`
let mut db = Database::<String>::open_writer("db.kjdb").await?;

// Add entries to the database
db.put("hello".to_string(), &"world".to_string()).await?;
db.put("foo".to_string(), &"bar".to_string()).await?;

// Update a value
db.put("hello".to_string(), &"again".to_string()).await?;

// Read an entry (if it exists)
if let Some(value) = db.get("hello").await? {
    assert_eq!(value, "again");
}

// Iterate over keys by prefix
for key in db.prefix_keys("hel") {
    println!("Key: {key:?}");
}

// Iterate over key-values by prefix
let iter = db.prefix_items("hel");
tokio::pin!(iter);
while let Some((key, value)) = iter.try_next().await? {
    println!("Key: {key:?}, Value: {value:?}");
}

// Iterate over keys by range
for key in db.range_keys("h".."x") {
    println!("Key: {key:?}");
}

// Iterate over key-values by range
let iter = db.range_items("h".."x");
tokio::pin!(iter);
while let Some((key, value)) = iter.try_next().await? {
    println!("Key: {key:?}, Value: {value:?}");
}

// Delete an entry
db.delete("hello").await?;

// Close the database
drop(db);

// Open two concurrent readers
let db1 = Database::<String>::open_reader("db.kjdb").await?;
let db2 = Database::<String>::open_reader("db.kjdb").await?;

assert_eq!(db1.get("hello").await?, db2.get("hello").await?);
```

## Status

- No further changes to the disk format are planned.
- The initial scan (when opening a database) is not well optimized yet. You should expect it to take ~5ms when opening a database with 1,000 keys, each having a 512 byte value (see benchmarking section).
- Reading and writing after the initial scan are already quite efficient, with limited room for significant further gains.

## Disk format

An on-disk entry looks like this (quoted for clarity):

```rust
" {\"hello\":\"world\"}\n"
```

Each line is prefixed with either `' '` or `'\t'` as a tie-breaker for interrupted writes.

Before a key is updated to a new value, the old line (if any) gets it's first byte changed to `'\t'`:

```rust
"\t{\"hello\":\"world\"}\n"
```

When the process dies during a write, it could recover from this state:

```json
 {"hello":"world"}
    {"hello":"again"}
```

The entry prefixed with `' '` would win over the entry prefixed with `'\t'` and the database would be consistent.

Outdated entries (and otherwise unused space) is tracked by an allocator and reused for future writes.

## Benchmarking

Use the following to setup a database file to benchmark database opening:

```sh
cargo build --release --examples
python3 -c 'for x in range(1_000): print(f"put hello{x} " + ("A"*512))' | target/release/examples/shell bench.db
time target/release/examples/shell bench.db < /dev/null
```

## Fuzzing

Use libfuzzer to generate database operations and assert the database state is consistent with a control state:

```
cargo fuzz run --release fuzz_db_ops -s none
```

## License

`MIT OR Apache-2.0`
