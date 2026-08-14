use crate::Database;
use crate::errors::*;
use crate::writer::DatabaseWriter;
use futures_util::TryStreamExt;
use futures_util::stream::{self, FuturesUnordered};
use futures_util::{Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::ops::RangeBounds;
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::{
    fs::File,
    sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, mpsc},
};

pub struct Pool<T: Serialize + DeserializeOwned> {
    db: RwLock<DatabaseWriter<T>>,
    pool: Mutex<mpsc::UnboundedReceiver<File>>,
    tx: mpsc::UnboundedSender<File>,
}

impl<T: Serialize + DeserializeOwned> Pool<T> {
    pub fn new<I: IntoIterator<Item = File>>(db: DatabaseWriter<T>, readers: I) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for file in readers {
            let _ = tx.send(file);
        }
        Self {
            db: RwLock::new(db),
            pool: Mutex::new(rx),
            tx,
        }
    }

    pub async fn open<P: AsRef<Path>>(path: P, num_readers: usize) -> Result<Self> {
        let db = DatabaseWriter::<T>::open(&path).await?;

        let mut readers = FuturesUnordered::new();
        for _ in 0..num_readers {
            readers.push(async { OpenOptions::new().read(true).write(false).open(&path).await });
        }

        let db = Self::new(db, []);
        while let Some(file) = readers.try_next().await? {
            let _ = db.tx.send(file);
        }

        Ok(db)
    }

    pub async fn reader(&self) -> Option<Reader<'_, T>> {
        let file = self.pool.lock().await.recv().await?;
        Some(Reader {
            db: self.db.read().await,
            file: PooledFile {
                file: Some(file),
                tx: self.tx.clone(),
            },
        })
    }

    pub async fn writer(&self) -> Writer<'_, T> {
        Writer {
            db: self.db.write().await,
        }
    }

    pub fn try_reader(&self) -> Option<Reader<'_, T>> {
        let file = self.pool.try_lock().ok()?.try_recv().ok()?;
        Some(Reader {
            db: self.db.try_read().ok()?,
            file: PooledFile {
                file: Some(file),
                tx: self.tx.clone(),
            },
        })
    }

    pub fn try_writer(&self) -> Option<Writer<'_, T>> {
        Some(Writer {
            db: self.db.try_write().ok()?,
        })
    }

    pub fn into_inner(self) -> DatabaseWriter<T> {
        self.db.into_inner()
    }
}

struct PooledFile {
    file: Option<File>,
    tx: mpsc::UnboundedSender<File>,
}

impl Drop for PooledFile {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = self.tx.send(file);
        }
    }
}

pub struct Reader<'a, T: Serialize + DeserializeOwned> {
    db: RwLockReadGuard<'a, DatabaseWriter<T>>,
    file: PooledFile,
}

impl<'a, T: Serialize + DeserializeOwned> Reader<'a, T> {
    pub async fn get(&mut self, key: &str) -> Result<Option<T>> {
        let file = self.file.file.as_mut().ok_or(Error::ClosedPoolHandle)?;
        Database::read_from_alloc(file, &self.db.alloc, key).await
    }

    /// Iterate over the keys currently in the database, in sorted order
    pub fn range_keys<R: RangeBounds<str>>(&self, range: R) -> impl Iterator<Item = &str> {
        self.db.range_keys(range)
    }

    /// Iterate over database keys and read their respective value from the database
    pub fn range_items<R: RangeBounds<str>>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = Result<(&str, T)>> {
        if let Some(file) = self.file.file.as_mut() {
            Database::range_items_from_alloc(file, &self.db.alloc, range).left_stream()
        } else {
            stream::once(async { Err(Error::ClosedPoolHandle) }).right_stream()
        }
    }
}

pub struct Writer<'a, T: Serialize + DeserializeOwned> {
    db: RwLockWriteGuard<'a, DatabaseWriter<T>>,
}

impl<'a, T: Serialize + DeserializeOwned> Writer<'a, T> {
    pub async fn get(&mut self, key: &str) -> Result<Option<T>> {
        self.db.get(key).await
    }

    pub async fn write(&mut self, key: String, value: &T) -> Result<()> {
        self.db.write(key, value).await
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        self.db.delete(key).await
    }

    /// Iterate over the keys currently in the database, in sorted order
    pub fn range_keys<R: RangeBounds<str>>(&self, range: R) -> impl Iterator<Item = &str> {
        self.db.range_keys(range)
    }

    /// Iterate over database keys and read their respective value from the database
    pub fn range_items<R: RangeBounds<str>>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = Result<(&str, T)>> {
        self.db.range_items(range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_pool(num: usize) -> Pool<String> {
        let file = async_tempfile::TempFile::new().await.unwrap();
        Pool::open(file.file_path(), num).await.unwrap()
    }

    #[tokio::test]
    async fn test_write_and_reader_get() {
        let pool = make_pool(1).await;

        {
            let mut w = pool.writer().await;
            w.write("key1".to_string(), &"hello".to_string())
                .await
                .unwrap();
        }

        let mut r = pool.reader().await.unwrap();
        assert_eq!(r.get("key1").await.unwrap(), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_delete() {
        let pool = make_pool(1).await;

        {
            let mut w = pool.writer().await;
            w.write("key1".to_string(), &"hello".to_string())
                .await
                .unwrap();
        }

        {
            let mut w = pool.writer().await;
            w.delete("key1").await.unwrap();
        }

        let mut r = pool.reader().await.unwrap();
        assert_eq!(r.get("key1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_reader_returned_to_pool_on_drop() {
        let pool = make_pool(2).await;

        {
            // Acquire two readers, after the first writers should be
            // unavailable, after the second readers should be unavailable too

            let reader = pool.reader().await.unwrap();
            assert!(pool.try_writer().is_none());

            let reader2 = pool.reader().await.unwrap();
            assert!(pool.try_reader().is_none());
            assert!(pool.try_writer().is_none());

            drop(reader);
            drop(reader2);
        }

        // Test both readers and writers are available again
        assert!(pool.try_reader().is_some());
        assert!(pool.try_writer().is_some());

        assert!(pool.reader().await.is_some());
        pool.writer().await;
    }
}
