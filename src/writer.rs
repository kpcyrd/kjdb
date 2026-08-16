use crate::Database;
use crate::alloc::Alloc;
use crate::errors::*;
use crate::record::{Entry, Record};
use futures_util::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::num::NonZeroU64;
use std::ops::RangeBounds;
use std::path::Path;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

pub struct DatabaseWriter<T: Serialize + DeserializeOwned> {
    pub(crate) db: Database<T>,
    pub(crate) alloc: Alloc,
}

impl<T: Serialize + DeserializeOwned> DatabaseWriter<T> {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        Database::new(file).scan_read_write().await
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<T>> {
        Database::read_from_alloc(&mut self.db.file, &self.alloc, key).await
    }

    pub async fn put(&mut self, key: String, value: &T) -> Result<()> {
        let prev = if let Some(record) = self.alloc.get(&key) {
            self.write_at(record.offset, b"\t").await?;
            Some(record)
        } else {
            None
        };

        let entry = Entry::new(key.to_string(), value);
        let json = serde_json::to_string(&entry)?;
        let key = entry.key();
        let buf = format!(" {json}\n");
        let size = NonZeroU64::new(buf.len() as u64).unwrap(); // XXX: Due to our framing, this can never be zero

        // Write to file
        let offset = self.alloc.gaps.take_space(size);
        self.write_at(offset, buf.as_bytes()).await?;

        // Add to map
        self.alloc.map.insert(key, Record::new(offset, size));

        // Mark previous space as available
        if let Some(prev) = prev {
            self.delete_at(prev.offset, prev.size).await?;
        }

        Ok(())
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        let Some(record) = self.alloc.get(key) else {
            return Ok(());
        };
        self.delete_at(record.offset, record.size).await?;
        self.alloc.map.remove(key);
        Ok(())
    }

    async fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        self.db.file.seek(SeekFrom::Start(offset)).await?;
        self.db.file.write_all(buf).await?;
        Ok(())
    }

    async fn delete_at(&mut self, offset: u64, size: NonZeroU64) -> Result<()> {
        self.write_at(offset, b"\n").await?;
        self.add_gap(offset, size).await?;
        Ok(())
    }

    /// Iterate over the keys currently in the database, in sorted order
    pub fn range_keys<R: RangeBounds<str>>(&self, range: R) -> impl Iterator<Item = &str> {
        self.alloc.range(range).map(|(k, _v)| k)
    }

    /// Iterate over database keys and read their respective value
    pub fn range_items<R: RangeBounds<str>>(
        &mut self,
        range: R,
    ) -> impl Stream<Item = Result<(&str, T)>> {
        Database::range_items_from_alloc(&mut self.db.file, &self.alloc, range)
    }

    /// Iterate over database keys with a specific prefix, in sorted order
    pub fn prefix_keys(&self, prefix: &str) -> impl Iterator<Item = &str> {
        self.alloc.prefix(prefix).map(|(k, _v)| k)
    }

    /// Iterate over database keys with a specific prefix and read their respective value
    pub fn prefix_items(&mut self, prefix: &str) -> impl Stream<Item = Result<(&str, T)>> {
        Database::prefix_items_from_alloc(&mut self.db.file, &self.alloc, prefix)
    }

    async fn add_gap(&mut self, offset: u64, size: NonZeroU64) -> Result<()> {
        // Register the gap
        self.alloc.gaps.add_gap(offset, size);
        // Truncate the file if possible
        self.db.file.set_len(self.alloc.gaps.truncate()).await?;
        Ok(())
    }

    /// Unlocks the database file and returns the inner `Database` instance
    ///
    /// This should return immediately, but is marked as async so a hanging
    /// network filesystem won't hang the async runtime
    pub async fn into_inner(self) -> Result<Database<T>> {
        let (db, _alloc) = self.into_parts().await?;
        Ok(db)
    }

    /// Unlocks the database file and return both the inner `Database` instance and the allocator state
    ///
    /// See `into_inner` for more details
    pub async fn into_parts(self) -> Result<(Database<T>, Alloc)> {
        let db = self.db.unlock().await?;
        Ok((db, self.alloc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::DatabaseReader;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use std::path::Path;
    use tokio::fs;

    async fn verify<T: Serialize + DeserializeOwned>(file_path: &Path, db: DatabaseWriter<T>) {
        let (_db, alloc) = db.into_parts().await.unwrap();
        let reader = DatabaseReader::<T>::open(file_path).await.unwrap();
        assert_eq!(reader.alloc, alloc);
    }

    #[tokio::test]
    async fn test_write_once() {
        let file = async_tempfile::TempFile::new().await.unwrap();
        let mut db = DatabaseWriter::<String>::open(file.file_path())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), None);
        db.put("foo".to_string(), &"hello world!".to_string())
            .await
            .unwrap();
        assert_eq!(
            db.get("foo").await.unwrap(),
            Some("hello world!".to_string())
        );

        let data = fs::read_to_string(file.file_path()).await.unwrap();
        assert_eq!(data, " {\"foo\":\"hello world!\"}\n");

        verify(file.file_path(), db).await;
    }

    #[tokio::test]
    async fn test_write_twice() {
        let file = async_tempfile::TempFile::new().await.unwrap();
        let mut db = DatabaseWriter::<String>::open(file.file_path())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), None);

        db.put("foo".to_string(), &"hello one!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello one!".to_string()));

        db.put("foo".to_string(), &"hello two!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello two!".to_string()));

        let data = fs::read_to_string(file.file_path()).await.unwrap();
        assert_eq!(
            data,
            "\n{\"foo\":\"hello one!\"}\n {\"foo\":\"hello two!\"}\n"
        );

        verify(file.file_path(), db).await;
    }

    #[tokio::test]
    async fn test_write_three_times() {
        let file = async_tempfile::TempFile::new().await.unwrap();
        let mut db = DatabaseWriter::<String>::open(file.file_path())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), None);

        // 1
        db.put("foo".to_string(), &"hello one!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello one!".to_string()));

        // 2
        db.put("foo".to_string(), &"hello two!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello two!".to_string()));

        // 3
        db.put("foo".to_string(), &"hello 3!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello 3!".to_string()));

        // Compare
        let data = fs::read_to_string(file.file_path()).await.unwrap();
        assert_eq!(data, " {\"foo\":\"hello 3!\"}\n");

        verify(file.file_path(), db).await;
    }

    #[tokio::test]
    async fn test_write_three_times_avoid_truncate() {
        let file = async_tempfile::TempFile::new().await.unwrap();
        let mut db = DatabaseWriter::<String>::open(file.file_path())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), None);

        // 1
        db.put("foo".to_string(), &"hello one!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello one!".to_string()));

        // Add another entry to avoid truncating everything away
        db.put("bar".to_string(), &"something".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("bar").await.unwrap(), Some("something".to_string()));

        // 2
        db.put("foo".to_string(), &"hello two!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello two!".to_string()));

        // 3
        db.put("foo".to_string(), &"hello 3!".to_string())
            .await
            .unwrap();
        assert_eq!(db.get("foo").await.unwrap(), Some("hello 3!".to_string()));

        // Compare
        let data = fs::read_to_string(file.file_path()).await.unwrap();
        assert_eq!(
            data,
            " {\"foo\":\"hello 3!\"}\n}\n {\"bar\":\"something\"}\n"
        );

        verify(file.file_path(), db).await;
    }
}
