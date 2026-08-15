#![no_main]

use kjdb::errors::*;
use kjdb::futures::TryStreamExt;
use kjdb::writer::DatabaseWriter;
use libfuzzer_sys::fuzz_target;
use log::{debug, info};
use rand::distr::{SampleString, Uniform};
use rand::prelude::*;
use rand::rngs::ChaCha8Rng;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use tokio::fs;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};

pub fn rng_str(seed: u8, len: u8) -> String {
    let mut rng = ChaCha8Rng::seed_from_u64(seed as u64);
    let charset = Uniform::new_inclusive(char::from(32), char::from(126)).unwrap();
    charset.sample_string(&mut rng, len as usize)
}

pub fn yield_rng_str<I: Iterator<Item = u8>>(mut iter: I) -> Option<String> {
    let seed = iter.next()?;
    let len = iter.next()?;
    Some(rng_str(seed, len))
}

async fn reopen<T: Serialize + DeserializeOwned>(
    db: DatabaseWriter<T>,
) -> Result<DatabaseWriter<T>> {
    let db = db.into_inner().await?;
    db.scan_read_write().await
}

fuzz_target!(|data: &[u8]| {
    env_logger::try_init().ok();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let file = async_tempfile::TempFile::new().await.unwrap();
        let mut db = DatabaseWriter::<String>::open(file.file_path())
            .await
            .unwrap();
        let mut ctrl = BTreeMap::new();

        let mut instructions = data.iter().copied();

        // Discourage repeated instructions that are expensive and don't alter state
        let mut prev = None;

        while let Some(instr) = instructions.next() {
            match (prev, instr) {
                (prev, 0) if prev != Some(0) => {
                    // Assert what we expect matches what we have in the database
                    let iter = db.range_items(..);
                    tokio::pin!(iter);

                    let mut aggr = BTreeMap::new();
                    while let Some((key, value)) = iter.try_next().await.unwrap() {
                        aggr.insert(key.to_string(), value);
                    }

                    debug!("Asserting database state with control state");
                    if aggr != ctrl {
                        let db = fs::read_to_string(file.file_path()).await.unwrap();
                        println!("Database contents:\t{db:?}");
                        println!("kjdb state:\t\t{aggr:?}");
                        println!("ctrl state:\t\t{ctrl:?}");

                        panic!("Inconsistency detected, aborting");
                    } else {
                        debug!("-> Success");
                    }
                }
                (prev, 1) if prev != Some(1) => {
                    // Re-open database
                    debug!("Reopening database");
                    db = reopen(db).await.unwrap();
                }
                (_, 2) => {
                    let Some(key) = yield_rng_str(&mut instructions) else {
                        break;
                    };
                    let Some(value) = yield_rng_str(&mut instructions) else {
                        break;
                    };

                    info!("Inserting key: {key:?} => {value:?}");
                    db.write(key.clone(), &value).await.unwrap();
                    ctrl.insert(key, value);
                }
                (_, 3) => {
                    let Some(key) = yield_rng_str(&mut instructions) else {
                        break;
                    };

                    info!("Getting key: {key:?}");
                    db.get(&key).await.unwrap();
                }
                (_, 4) => {
                    let Some(key) = yield_rng_str(&mut instructions) else {
                        break;
                    };

                    info!("Removing key: {key:?}");
                    db.delete(&key).await.unwrap();
                    ctrl.remove(&key);
                }
                (_, 5) => {
                    let Some(key) = yield_rng_str(&mut instructions) else {
                        break;
                    };

                    let (inner_db, alloc) = db.into_parts().await.unwrap();

                    let Some(record) = alloc.map.get(&key) else {
                        break;
                    };

                    info!("Manually unstaging key (incomplete write): {key:?}");

                    // Unstage the record (but don't fully complete the write)
                    let mut f = fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(file.file_path())
                        .await
                        .unwrap();
                    f.seek(SeekFrom::Start(record.offset)).await.unwrap();
                    f.write_all(b"\t").await.unwrap();

                    // Reopen the database
                    db = inner_db.scan_read_write().await.unwrap();
                }
                _ => break,
            }

            prev = Some(instr);
        }
    });
});
