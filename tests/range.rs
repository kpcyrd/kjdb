use async_tempfile::TempFile;
use kjdb::errors::*;
use kjdb::futures::{Stream, TryStreamExt};
use kjdb::{Database, pool::Pool};
use std::ops::Bound;
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

macro_rules! test_range_keys {
    ($db:ident) => {
        // full scan
        assert_eq!(
            keys($db.range_keys(..)),
            ["app", "appetite", "apple", "application", "apply", "banana"]
        );

        // "app"..="apply"
        assert_eq!(
            keys($db.range_keys((Bound::Included("app"), Bound::Included("apply")))),
            ["app", "appetite", "apple", "application", "apply"]
        );

        // "app".."apply"
        assert_eq!(
            keys($db.range_keys((Bound::Included("app"), Bound::Excluded("apply")))),
            ["app", "appetite", "apple", "application"]
        );

        // ..="app"
        assert_eq!(
            keys($db.range_keys((Bound::Unbounded, Bound::Included("app")))),
            ["app"]
        );

        // "apply"..
        assert_eq!(
            keys($db.range_keys((Bound::Included("apply"), Bound::Unbounded))),
            ["apply", "banana"]
        );

        // "apple"..="banana"
        assert_eq!(
            keys($db.range_keys((Bound::Included("apple"), Bound::Included("banana")))),
            ["apple", "application", "apply", "banana"]
        );

        // "appl".."apply"
        assert_eq!(
            keys($db.range_keys((Bound::Included("appl"), Bound::Excluded("apply")))),
            ["apple", "application"]
        );

        // "x".."z"
        assert_eq!(
            keys($db.range_keys((Bound::Included("x"), Bound::Excluded("z")))),
            Vec::<String>::new()
        );
    };
}

macro_rules! test_range_items {
    ($db:ident) => {
        assert_eq!(
            items($db.range_items(..)).await.unwrap(),
            [
                ("app".to_string(), 5),
                ("appetite".to_string(), 6),
                ("apple".to_string(), 1),
                ("application".to_string(), 2),
                ("apply".to_string(), 3),
                ("banana".to_string(), 4),
            ]
        );

        // "app"..="apply"
        assert_eq!(
            items($db.range_items((Bound::Included("app"), Bound::Included("apply"))))
                .await
                .unwrap(),
            [
                ("app".to_string(), 5),
                ("appetite".to_string(), 6),
                ("apple".to_string(), 1),
                ("application".to_string(), 2),
                ("apply".to_string(), 3),
            ]
        );

        // "app".."apply"
        assert_eq!(
            items($db.range_items((Bound::Included("app"), Bound::Excluded("apply"))))
                .await
                .unwrap(),
            [
                ("app".to_string(), 5),
                ("appetite".to_string(), 6),
                ("apple".to_string(), 1),
                ("application".to_string(), 2),
            ]
        );

        // ..="app"
        assert_eq!(
            items($db.range_items((Bound::Unbounded, Bound::Included("app"))))
                .await
                .unwrap(),
            [("app".to_string(), 5)]
        );

        // "apply"..
        assert_eq!(
            items($db.range_items((Bound::Included("apply"), Bound::Unbounded)))
                .await
                .unwrap(),
            [("apply".to_string(), 3), ("banana".to_string(), 4)]
        );

        // "apple"..="banana"
        assert_eq!(
            items($db.range_items((Bound::Included("apple"), Bound::Included("banana"))))
                .await
                .unwrap(),
            [
                ("apple".to_string(), 1),
                ("application".to_string(), 2),
                ("apply".to_string(), 3),
                ("banana".to_string(), 4),
            ]
        );

        // "appl".."apply"
        assert_eq!(
            items($db.range_items((Bound::Included("appl"), Bound::Excluded("apply"))))
                .await
                .unwrap(),
            [("apple".to_string(), 1), ("application".to_string(), 2),]
        );

        // "x".."z"
        assert_eq!(
            items($db.range_items((Bound::Included("x"), Bound::Excluded("z"))))
                .await
                .unwrap(),
            Vec::<(String, u32)>::new()
        );
    };
}

#[tokio::test]
async fn test_reader() {
    let file = setup().await;
    let mut db = Database::<u32>::open_reader(file.file_path())
        .await
        .unwrap();
    test_range_keys!(db);
    test_range_items!(db);
}

#[tokio::test]
async fn test_writer() {
    let file = setup().await;
    let mut db = Database::<u32>::open_writer(file.file_path())
        .await
        .unwrap();
    test_range_keys!(db);
    test_range_items!(db);
}

#[tokio::test]
async fn test_pooled_reader() {
    let file = setup().await;
    let pool = Pool::<u32>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.reader().await.unwrap();
    test_range_keys!(db);
    test_range_items!(db);
}

#[tokio::test]
async fn test_pooled_writer() {
    let file = setup().await;
    let pool = Pool::<u32>::open(file.file_path(), 2).await.unwrap();
    let mut db = pool.writer().await;
    test_range_keys!(db);
    test_range_items!(db);
}
