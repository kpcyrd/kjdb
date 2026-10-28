#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

pub mod alloc;
pub mod errors;
pub mod futures;
pub mod pool;
pub mod reader;
pub mod writer;

use crate::alloc::Alloc;
use crate::alloc::record::{Entry, Record};
use crate::errors::*;
use crate::reader::DatabaseReader;
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

/// A database handle that can be used to read and write records
#[derive(Debug)]
pub struct Database<T: Serialize + DeserializeOwned> {
    file: File,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Database<T> {
    /// Create a new database handle from an open file
    ///
    /// This doesn't do any IO, you need to call `scan_read_only` or
    /// `scan_read_write` to acquire a read or write lock on the file and scan
    /// for existing records before you can perform any reads or writes.
    pub fn new(file: File) -> Self {
        Database {
            file,
            _marker: std::marker::PhantomData,
        }
    }

    /// Open an existing database for reading
    ///
    /// The file must already exist, afterwards a read-lock is acquired and the
    /// database is scanned for existing records, preparing it for read operations.
    pub async fn open_reader<P: AsRef<Path>>(path: P) -> Result<DatabaseReader<T>> {
        DatabaseReader::<T>::open(path).await
    }

    /// Open an existing database for reading and writing
    ///
    /// The file is created if it doesn't exist yet, afterwards a write-lock is
    /// acquired and the database is scanned for existing records, preparing it for
    /// read and write operations.
    pub async fn open_writer<P: AsRef<Path>>(path: P) -> Result<DatabaseWriter<T>> {
        DatabaseWriter::<T>::open(path).await
    }

    /// Acquire a read lock on a database file handle and scan for existing records
    pub async fn scan_read_only(self) -> Result<DatabaseReader<T>> {
        let file = self.file;

        let file = tokio::task::spawn_blocking(move || file.lock_shared().map(|_| file)).await??;
        let mut db = Database::new(file);

        let alloc = Alloc::scan::<_, T>(&mut db.file).await?;
        Ok(DatabaseReader { db, alloc })
    }

    /// Acquire a write lock on a database file handle and scan for existing records
    pub async fn scan_read_write(self) -> Result<DatabaseWriter<T>> {
        let file = self.file;

        let file = tokio::task::spawn_blocking(move || file.lock().map(|_| file)).await??;
        let mut db = Database::new(file);

        let alloc = Alloc::scan::<_, T>(&mut db.file).await?;
        db.file.set_len(alloc.gaps.file_end).await?;
        Ok(DatabaseWriter { db, alloc })
    }

    async fn read_at(file: &mut File, offset: u64, length: u64) -> Result<T> {
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buf = vec![0; length as usize];
        file.read_exact(&mut buf).await?;

        let entry = serde_json::from_slice::<Entry<T>>(&buf)?;
        Ok(entry.value())
    }

    async fn read_from_alloc(file: &mut File, alloc: &Alloc, key: &str) -> Result<Option<T>> {
        let Some(record) = alloc.get(key) else {
            return Ok(None);
        };
        let value = Self::read_at(file, record.offset, record.size.get()).await?;
        Ok(Some(value))
    }

    fn items_from_iter<'a>(
        file: &mut File,
        iter: impl Iterator<Item = (&'a str, &'a Record)>,
    ) -> impl Stream<Item = Result<(&'a str, T)>> {
        async_stream::try_stream! {
            for (key, record) in iter {
                let value = Self::read_at(file, record.offset, record.size.get()).await?;
                yield (key, value);
            }
        }
    }

    fn range_items_from_alloc<'a, R: RangeBounds<str>>(
        file: &mut File,
        alloc: &'a Alloc,
        range: R,
    ) -> impl Stream<Item = Result<(&'a str, T)>> {
        Self::items_from_iter(file, alloc.range(range))
    }

    fn prefix_items_from_alloc<'a>(
        file: &mut File,
        alloc: &'a Alloc,
        prefix: &str,
    ) -> impl Stream<Item = Result<(&'a str, T)>> {
        Self::items_from_iter(file, alloc.prefix(prefix))
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
