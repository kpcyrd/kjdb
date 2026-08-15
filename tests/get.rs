use async_tempfile::TempFile;
use kjdb::errors::*;
use kjdb::{Database, pool::Pool};
use std::ops::AsyncFnMut;
use tokio::fs;

const DATABASE: &str = r#"
 {"hello": "world"}
 {"foo": "bar"}
 {"baz": "qux"}
"#;

async fn setup() -> TempFile {
    let file = TempFile::new().await.unwrap();
    fs::write(file.file_path(), DATABASE).await.unwrap();
    file
}

async fn test<F: AsyncFnMut(&str) -> Result<Option<String>>>(mut get: F) {
    assert_eq!(get("hello").await.unwrap().as_deref(), Some("world"));
    assert_eq!(get("foo").await.unwrap().as_deref(), Some("bar"));
    assert_eq!(get("404").await.unwrap().as_deref(), None);
}

#[tokio::test]
async fn test_reader() {
    let file = setup().await;
    let mut db = Database::<String>::open_reader(file.file_path())
        .await
        .unwrap();
    test(async |key| db.get(key).await).await
}

#[tokio::test]
async fn test_writer() {
    let file = setup().await;
    let mut db = Database::<String>::open_writer(file.file_path())
        .await
        .unwrap();
    test(async |key| db.get(key).await).await
}

#[tokio::test]
async fn test_pooled_reader() {
    let file = setup().await;
    let pool = Pool::<String>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.reader().await.unwrap();
    test(async |key| db.get(key).await).await
}

#[tokio::test]
async fn test_pooled_writer() {
    let file = setup().await;
    let pool = Pool::<String>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.writer().await;
    test(async |key| db.get(key).await).await
}
