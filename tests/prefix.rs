use async_tempfile::TempFile;
use kjdb::errors::*;
use kjdb::futures::{Stream, TryStreamExt};
use kjdb::{Database, pool::Pool};
use std::ops::AsyncFnMut;
use tokio::fs;

const DATABASE: &str = r#"
 {"apple":1}
 {"application":2}
 {"apply":3}
 {"banana":4}
 {"app":5}
 {"appetite":6}
"#;

async fn setup() -> TempFile {
    let file = TempFile::new().await.unwrap();
    fs::write(file.file_path(), DATABASE).await.unwrap();
    file
}

fn keys<'a>(iter: impl Iterator<Item = &'a str>) -> Vec<String> {
    iter.map(String::from).collect::<Vec<_>>()
}

async fn items(iter: impl Stream<Item = Result<(&str, u32)>>) -> Result<Vec<(String, u32)>> {
    iter.map_ok(|(k, v)| (k.to_owned(), v))
        .try_collect::<Vec<_>>()
        .await
}

fn test_keys<F: FnMut(&str) -> Vec<String>>(mut prefix: F) {
    assert_eq!(prefix("hello"), Vec::<String>::new());
    assert_eq!(
        prefix("a"),
        ["app", "appetite", "apple", "application", "apply"]
    );
    assert_eq!(
        prefix("app"),
        ["app", "appetite", "apple", "application", "apply"]
    );
    assert_eq!(prefix("appl"), ["apple", "application", "apply"]);
    assert_eq!(
        prefix(""),
        ["app", "appetite", "apple", "application", "apply", "banana"]
    );
}

async fn test_items<F: AsyncFnMut(&str) -> Result<Vec<(String, u32)>>>(mut prefix: F) {
    assert_eq!(prefix("hello").await.unwrap(), Vec::<(String, u32)>::new());
    assert_eq!(
        prefix("a").await.unwrap(),
        [
            ("app".to_string(), 5),
            ("appetite".to_string(), 6),
            ("apple".to_string(), 1),
            ("application".to_string(), 2),
            ("apply".to_string(), 3),
        ]
    );
    assert_eq!(
        prefix("app").await.unwrap(),
        [
            ("app".to_string(), 5),
            ("appetite".to_string(), 6),
            ("apple".to_string(), 1),
            ("application".to_string(), 2),
            ("apply".to_string(), 3),
        ]
    );
    assert_eq!(
        prefix("appl").await.unwrap(),
        [
            ("apple".to_string(), 1),
            ("application".to_string(), 2),
            ("apply".to_string(), 3),
        ]
    );
    assert_eq!(
        prefix("").await.unwrap(),
        [
            ("app".to_string(), 5),
            ("appetite".to_string(), 6),
            ("apple".to_string(), 1),
            ("application".to_string(), 2),
            ("apply".to_string(), 3),
            ("banana".to_string(), 4),
        ]
    );
}

#[tokio::test]
async fn test_reader() {
    let file = setup().await;
    let mut db = Database::<u32>::open_reader(file.file_path())
        .await
        .unwrap();
    test_keys(|key| keys(db.prefix_keys(key)));
    test_items(async |key| items(db.prefix_items(key)).await).await;
}

#[tokio::test]
async fn test_writer() {
    let file = setup().await;
    let mut db = Database::<u32>::open_writer(file.file_path())
        .await
        .unwrap();
    test_keys(|prefix| keys(db.prefix_keys(prefix)));
    test_items(async |prefix| items(db.prefix_items(prefix)).await).await;
}

#[tokio::test]
async fn test_pooled_reader() {
    let file = setup().await;
    let pool = Pool::<u32>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.reader().await.unwrap();
    test_keys(|prefix| keys(db.prefix_keys(prefix)));
    test_items(async |prefix| items(db.prefix_items(prefix)).await).await;
}

#[tokio::test]
async fn test_pooled_writer() {
    let file = setup().await;
    let pool = Pool::<u32>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.writer().await;
    test_keys(|prefix| keys(db.prefix_keys(prefix)));
    test_items(async |prefix| items(db.prefix_items(prefix)).await).await;
}
