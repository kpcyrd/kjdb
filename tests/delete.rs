use async_tempfile::TempFile;
use kjdb::{
    Database,
    errors::*,
    pool::{self, Pool},
    writer::DatabaseWriter,
};
use std::path::Path;
use tokio::fs;

const DATABASE: &str = r#"
 {"hello":"world"}
 {"foo":"bar"}
 {"baz":"qux"}
"#;

const EXPECTED: &str = r#"

{"hello":"world"}
 {"foo":"bar"}
"#;

async fn setup() -> TempFile {
    let file = TempFile::new().await.unwrap();
    fs::write(file.file_path(), DATABASE).await.unwrap();
    file
}

async fn test<D: Db>(mut db: D, path: &Path) {
    assert_eq!(db.get("hello").await.unwrap().as_deref(), Some("world"));
    db.delete("hello").await.unwrap();

    assert_eq!(db.get("hello").await.unwrap().as_deref(), None);
    assert_eq!(db.get("404").await.unwrap().as_deref(), None);

    // deleting a non-existent key is a no-op
    db.delete("404").await.unwrap();

    assert_eq!(db.get("foo").await.unwrap().as_deref(), Some("bar"));
    assert_eq!(db.get("baz").await.unwrap().as_deref(), Some("qux"));

    // test truncation works
    db.delete("baz").await.unwrap();

    assert_eq!(fs::read_to_string(path).await.unwrap(), EXPECTED);
}

trait Db {
    async fn get(&mut self, key: &str) -> Result<Option<String>>;

    async fn delete(&mut self, key: &str) -> Result<()>;
}

impl Db for DatabaseWriter<String> {
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.get(key).await
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        self.delete(key).await
    }
}

impl Db for pool::Writer<'_, String> {
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.get(key).await
    }

    async fn delete(&mut self, key: &str) -> Result<()> {
        self.delete(key).await
    }
}

#[tokio::test]
async fn test_writer() {
    let file = setup().await;
    let db = Database::<String>::open_writer(file.file_path())
        .await
        .unwrap();
    test(db, file.file_path()).await
}

#[tokio::test]
async fn test_pooled_writer() {
    let file = setup().await;
    let pool = Pool::<String>::open(file.file_path(), 2).await.unwrap();
    let db = pool.writer().await;
    test(db, file.file_path()).await
}
