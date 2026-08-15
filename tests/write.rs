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

const EXPECTED: &str = r#" {"foo":""}
world"}

{"foo":"bar"}
 {"baz":"qux"}
 {"hello":"ohai!"}
"#;

async fn setup() -> TempFile {
    let file = TempFile::new().await.unwrap();
    fs::write(file.file_path(), DATABASE).await.unwrap();
    file
}

async fn test<D: Db<String>>(mut db: D, path: &Path) {
    assert_eq!(db.get("hello").await.unwrap().as_deref(), Some("world"));
    db.write("hello".to_string(), &"ohai!".to_string())
        .await
        .unwrap();

    assert_eq!(db.get("foo").await.unwrap().as_deref(), Some("bar"));
    db.write("foo".to_string(), &"".to_string()).await.unwrap();

    assert_eq!(db.get("404").await.unwrap().as_deref(), None);

    assert_eq!(db.get("hello").await.unwrap().as_deref(), Some("ohai!"));
    assert_eq!(db.get("foo").await.unwrap().as_deref(), Some(""));

    assert_eq!(fs::read_to_string(path).await.unwrap(), EXPECTED);
}

trait Db<T> {
    async fn get(&mut self, key: &str) -> Result<Option<T>>;

    async fn write(&mut self, key: String, value: &T) -> Result<()>;
}

impl Db<String> for DatabaseWriter<String> {
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.get(key).await
    }

    async fn write(&mut self, key: String, value: &String) -> Result<()> {
        self.write(key, value).await
    }
}

impl Db<String> for pool::Writer<'_, String> {
    async fn get(&mut self, key: &str) -> Result<Option<String>> {
        self.get(key).await
    }

    async fn write(&mut self, key: String, value: &String) -> Result<()> {
        self.write(key, value).await
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
