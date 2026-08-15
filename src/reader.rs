use crate::Database;
use crate::alloc::Alloc;
use crate::errors::*;
use futures_util::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ops::RangeBounds;
use std::path::Path;

pub struct DatabaseReader<T: Serialize + DeserializeOwned> {
    pub(crate) db: Database<T>,
    pub(crate) alloc: Alloc,
}

impl<T: Serialize + DeserializeOwned> DatabaseReader<T> {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = tokio::fs::OpenOptions::new().read(true).open(path).await?;
        Database::new(file).scan_read_only().await
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<T>> {
        Database::read_from_alloc(&mut self.db.file, &self.alloc, key).await
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
