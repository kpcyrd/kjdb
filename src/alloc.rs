use crate::errors::*;
use crate::gaps::Gaps;
use crate::record::{self, Record, Tie};
use serde::Deserialize;
use std::collections::{BTreeMap, btree_map};
use std::mem;
use std::num::NonZeroU64;
use std::ops::RangeBounds;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncSeek, AsyncSeekExt, BufReader};

#[derive(Debug, Default, PartialEq)]
pub struct Alloc {
    pub map: BTreeMap<String, Record>,
    pub gaps: Gaps,
}

impl Alloc {
    pub async fn scan<F: AsyncRead + AsyncSeek + Unpin, T: for<'a> Deserialize<'a>>(
        file: &mut F,
    ) -> Result<Self> {
        file.rewind().await?;
        let mut reader = BufReader::new(file);

        let mut alloc = Self::default();

        let mut buf = String::new();
        loop {
            let n = reader.read_line(&mut buf).await?;
            let Some(size) = NonZeroU64::new(n as u64) else {
                // End of file
                break;
            };

            let offset = alloc.gaps.file_end;

            if let Some((tie, json)) = Tie::detect(&buf)
                && let Ok(entry) = record::Entry::<T>::deserialize(json)
            {
                let entry = alloc.map.entry(entry.key());

                match entry {
                    btree_map::Entry::Vacant(v) => {
                        v.insert(Record { tie, offset, size });
                    }
                    btree_map::Entry::Occupied(mut o) => {
                        let existing = o.get_mut();

                        if existing.tie == Tie::Unstaged && tie == Tie::Live {
                            // replace existing record, mark previous space as available for allocations
                            let previous = mem::replace(existing, Record { tie, offset, size });
                            alloc.gaps.add_gap(previous.offset, previous.size);
                        } else {
                            // ignore this record and mark as available for allocations
                            alloc.gaps.add_gap(offset, size);
                        }
                    }
                }
            } else {
                alloc.gaps.add_gap(offset, size);
            }

            alloc.gaps.file_end = offset.saturating_add(size.get());
            buf.clear();
        }

        alloc.gaps.truncate();

        Ok(alloc)
    }

    pub fn get(&self, key: &str) -> Option<Record> {
        self.map.get(key).copied()
    }

    pub fn range<R: RangeBounds<str>>(&self, range: R) -> impl Iterator<Item = (&str, &Record)> {
        self.map.range(range).map(|(k, v)| (k.as_str(), v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_scan_empty() {
        let alloc = Alloc::scan::<_, u64>(&mut Cursor::new(b"")).await.unwrap();
        assert_eq!(alloc, Alloc::default());
    }

    #[tokio::test]
    async fn test_scan_one() {
        let alloc = Alloc::scan::<_, u64>(&mut Cursor::new(b" {\"foo\":1}\n"))
            .await
            .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 0,
                        size: NonZeroU64::new(11).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    file_end: 11,
                    ..Default::default()
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_two() {
        let alloc = Alloc::scan::<_, u64>(&mut Cursor::new(b" {\"foo\":1}\n {\"bar\":2}\n"))
            .await
            .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([
                    (
                        "foo".to_string(),
                        Record {
                            tie: Tie::Live,
                            offset: 0,
                            size: NonZeroU64::new(11).unwrap(),
                        },
                    ),
                    (
                        "bar".to_string(),
                        Record {
                            tie: Tie::Live,
                            offset: 11,
                            size: NonZeroU64::new(11).unwrap(),
                        },
                    ),
                ]),
                gaps: Gaps {
                    file_end: 22,
                    ..Default::default()
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_one_live_unstaged() {
        let alloc = Alloc::scan::<_, u64>(&mut Cursor::new(b" {\"foo\":1}\n\t{\"foo\":2}\n"))
            .await
            .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 0,
                        size: NonZeroU64::new(11).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    file_end: 11,
                    ..Default::default()
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_one_unstaged_live() {
        let alloc = Alloc::scan::<_, u64>(&mut Cursor::new(b"\t{\"foo\":1}\n {\"foo\":2}\n"))
            .await
            .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 11,
                        size: NonZeroU64::new(11).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    map: BTreeMap::from([(0, NonZeroU64::new(11).unwrap(),)]),
                    file_end: 22,
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_live_partial_unstaged() {
        let alloc = Alloc::scan::<_, String>(&mut Cursor::new(
            b" {\"foo\":\"hello 3!\"}\n}\n\t{\"foo\":\"hello two!\"}\n",
        ))
        .await
        .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 0,
                        size: NonZeroU64::new(20).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    file_end: 20,
                    ..Default::default()
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_unstaged_partial_live() {
        let alloc = Alloc::scan::<_, String>(&mut Cursor::new(
            b"\t{\"foo\":\"hello 3!\"}\n}\n {\"foo\":\"hello two!\"}\n",
        ))
        .await
        .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 22,
                        size: NonZeroU64::new(22).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    map: BTreeMap::from([(0, NonZeroU64::new(22).unwrap()),]),
                    file_end: 44,
                },
            }
        );
    }

    #[tokio::test]
    async fn test_scan_two_partial() {
        let alloc = Alloc::scan::<_, String>(&mut Cursor::new(
            b"\t{\"foo\":\"hello 3!\"}\n \"foo\":\"\nh}\n {\"foo\":\"hello two!\"}\n",
        ))
        .await
        .unwrap();
        assert_eq!(
            alloc,
            Alloc {
                map: BTreeMap::from([(
                    "foo".to_string(),
                    Record {
                        tie: Tie::Live,
                        offset: 32,
                        size: NonZeroU64::new(22).unwrap(),
                    },
                )]),
                gaps: Gaps {
                    map: BTreeMap::from([(0, NonZeroU64::new(32).unwrap()),]),
                    file_end: 54,
                },
            }
        );
    }
}
