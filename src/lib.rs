pub mod alloc;
pub mod errors;
pub mod gaps;
pub mod reader;
pub mod record;
pub mod writer;

use crate::alloc::Alloc;
use crate::errors::*;
use crate::reader::DatabaseReader;
use crate::record::Entry;
use crate::writer::DatabaseWriter;
use fs4::AsyncFileExt;
use futures_util::Stream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ops::RangeBounds;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

pub use serde_json::Value as JsonValue;

#[derive(Debug)]
pub struct Database<T: Serialize + DeserializeOwned> {
    file: File,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Database<T> {
    pub fn new(file: File) -> Self {
        Database {
            file,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn open_reader<P: AsRef<Path>>(path: P) -> Result<DatabaseReader<T>> {
        DatabaseReader::<T>::open(path).await
    }

    pub async fn open_writer<P: AsRef<Path>>(path: P) -> Result<DatabaseWriter<T>> {
        DatabaseWriter::<T>::open(path).await
    }

    pub async fn scan_read_only(self) -> Result<DatabaseReader<T>> {
        let file = self.file;

        let file = tokio::task::spawn_blocking(move || file.lock_shared().map(|_| file)).await??;
        let mut db = Database::new(file);

        let alloc = Alloc::scan::<_, T>(&mut db.file).await?;
        Ok(DatabaseReader { db, alloc })
    }

    pub async fn scan_read_write(self) -> Result<DatabaseWriter<T>> {
        let file = self.file;

        let file = tokio::task::spawn_blocking(move || file.lock().map(|_| file)).await??;
        let mut db = Database::new(file);

        let alloc = Alloc::scan::<_, T>(&mut db.file).await?;
        db.file.set_len(alloc.gaps.file_end).await?;
        Ok(DatabaseWriter { db, alloc })
    }

    async fn read_at(&mut self, offset: u64, length: u64) -> Result<T> {
        self.file.seek(SeekFrom::Start(offset)).await?;

        let mut buf = vec![0; length as usize];
        self.file.read_exact(&mut buf).await?;

        let entry = serde_json::from_slice::<Entry<T>>(&buf)?;
        Ok(entry.value())
    }

    async fn read_from_alloc(&mut self, alloc: &Alloc, key: &str) -> Result<Option<T>> {
        let Some(record) = alloc.get(key) else {
            return Ok(None);
        };
        let value = self.read_at(record.offset, record.size.get()).await?;
        Ok(Some(value))
    }

    fn range_items_from_alloc<'a, R: RangeBounds<str>>(
        &mut self,
        alloc: &'a Alloc,
        range: R,
    ) -> impl Stream<Item = Result<(&'a str, T)>> {
        async_stream::try_stream! {
            for (key, record) in alloc.range(range) {
                let value = self.read_at(record.offset, record.size.get()).await?;
                yield (key, value);
            }
        }
    }

    async fn unlock(self) -> Result<Self> {
        let file = self.file;
        let file = tokio::task::spawn_blocking(move || file.unlock().map(|_| file)).await??;
        Ok(Self::new(file))
    }

    /// Return the inner `File` instance
    pub fn into_inner(self) -> File {
        self.file
    }
}
