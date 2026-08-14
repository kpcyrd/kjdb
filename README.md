# kjdb - Keyed JSON Database

A database engine that operates on JSON lines.

When all you need is an async, atomic, in-process, disk-persistent, key-value store and sqlite is too much work. Works with any serde serializable type. No database schema, only `#[derive(Serialize, Deserialize)]`.

```rust
use kjdb::Database;

// Open or create a new database, define the value as `String`
let mut db = Database::<String>::open_writer("db.kjdb").await?;

// Add entries to the database
db.write("hello".to_string(), &"world".to_string()).await?;
db.write("foo".to_string(), &"bar".to_string()).await?;

// Update a value
db.write("hello".to_string(), &"again".to_string()).await?;

// Read an entry (if it exists)
if let Some(value) = db.get("hello").await? {
    assert_eq!(value, "again");
}

// Iterate over keys by prefix
for key in db.range_keys("he".."hf") {
    println!("Key: {key:?}");
}

// Iterate over key-values by prefix
let iter = db.range_items("he".."hf");
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

## Fuzzing

Use libfuzzer to generate database operations and assert the database state is consistent with a control state:

```
cargo fuzz run --release fuzz_db_ops -s none
```

## License

`MIT OR Apache-2.0`
