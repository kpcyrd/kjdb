use std::collections::BTreeMap;
use std::num::NonZeroU64;

#[derive(Debug, Default, PartialEq)]
pub struct Gaps {
    // NOTE: possibly allow reverse lookups `size->offsets` too
    pub map: BTreeMap<u64, NonZeroU64>,
    pub file_end: u64,
}

impl Gaps {
    fn find_gap(&self, requested: NonZeroU64) -> Option<(u64, NonZeroU64)> {
        self.map
            .iter()
            .find(|(_offset, size)| **size >= requested)
            .map(|(offset, size)| (*offset, *size))
    }

    pub fn take_space(&mut self, requested: NonZeroU64) -> u64 {
        if let Some((offset, size)) = self.find_gap(requested) {
            self.map.remove(&offset);
            let remaining = size.get().saturating_sub(requested.get());

            if let Some(new_size) = NonZeroU64::new(remaining) {
                self.map.insert(offset + requested.get(), new_size);
            }

            offset
        } else {
            // Take from the end of the file
            let offset = self.file_end;
            self.file_end = offset.saturating_add(requested.get());
            offset
        }
    }

    pub fn add_gap(&mut self, mut offset: u64, mut size: NonZeroU64) {
        // Check if we are prepending to an existing gap
        if let Some((next_offset, next_size)) = self.map.range(offset.saturating_add(1)..).next()
            && offset.saturating_add(size.get()) >= *next_offset
        {
            // Since we merge with the next gap anyway, cap the size to the distance
            let next_offset = *next_offset;
            let distance = next_offset.saturating_sub(offset);
            let fill = NonZeroU64::new(distance)
                .map(|distance| size.min(distance))
                .unwrap_or(size);

            // Merge with the next gap, make sure the merge can only increase the size, never shrink
            size = size.max(next_size.saturating_add(fill.get()));
            self.map.remove(&next_offset);
        }

        // Check if we are appending to an existing gap
        if let Some((prev_offset, prev_size)) = self.map.range(..offset).next()
            && prev_offset.saturating_add(prev_size.get()) >= offset
        {
            size = size.saturating_add(offset.saturating_sub(*prev_offset));
            offset = *prev_offset;
        }

        self.map.insert(offset, size);
        self.file_end = self.file_end.max(offset.saturating_add(size.get()));
    }

    pub fn truncate(&mut self) -> u64 {
        while let Some(last) = self.map.last_entry() {
            let offset = *last.key();
            let size = *last.get();

            if offset.saturating_add(size.get()) >= self.file_end {
                self.map.remove(&offset);
                self.file_end = offset;
            } else {
                break;
            }
        }
        self.file_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With an empty file, request space from the end
    #[test]
    fn test_take_space_empty() {
        let mut gaps = Gaps::default();
        let offset = gaps.take_space(NonZeroU64::new(100).unwrap());
        assert_eq!(offset, 0);
        assert_eq!(
            gaps,
            Gaps {
                file_end: 100,
                ..Default::default()
            }
        );
    }

    /// With an empty file, request space from the end twice
    #[test]
    fn test_take_space_empty_twice() {
        let mut gaps = Gaps::default();

        let offset = gaps.take_space(NonZeroU64::new(100).unwrap());
        assert_eq!(offset, 0);

        let offset = gaps.take_space(NonZeroU64::new(50).unwrap());
        assert_eq!(offset, 100);

        assert_eq!(
            gaps,
            Gaps {
                file_end: 150,
                ..Default::default()
            }
        );
    }

    /// With an empty file, register an immediate gap
    #[test]
    fn test_empty_add_gap() {
        let mut gaps = Gaps::default();
        gaps.add_gap(0, NonZeroU64::new(100).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(0, NonZeroU64::new(100).unwrap())]),
                file_end: 100,
            }
        );
    }

    /// With a non-empty file, register a gap in the middle
    #[test]
    fn test_nonempty_add_gap() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(50, NonZeroU64::new(20).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(20).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// On an empty file, register a gap, then truncate gaps from the end of the file
    #[test]
    fn test_empty_add_gap_truncate() {
        let mut gaps = Gaps::default();
        gaps.add_gap(0, NonZeroU64::new(100).unwrap());

        let end = gaps.truncate();
        assert_eq!(end, 0);
        assert_eq!(gaps, Gaps::default());
    }

    /// On a non-empty file, register one gap in the middle, two at the end, then truncate gaps from the end of the file
    #[test]
    fn test_nonempty_add_gap_truncate() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(50, NonZeroU64::new(20).unwrap());
        gaps.add_gap(145, NonZeroU64::new(15).unwrap());
        gaps.add_gap(160, NonZeroU64::new(100).unwrap());
        let end = gaps.truncate();
        assert_eq!(end, 145);
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(20).unwrap())]),
                file_end: 145,
            }
        );
    }

    /// Adding a gap after a gap should merge them
    #[test]
    fn test_gap_append_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(50, NonZeroU64::new(10).unwrap());
        gaps.add_gap(60, NonZeroU64::new(30).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(40).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Adding a gap before a gap should merge them
    #[test]
    fn test_gap_prepend_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(60, NonZeroU64::new(30).unwrap());
        gaps.add_gap(50, NonZeroU64::new(10).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(40).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Adding a gap between two gaps should merge them
    #[test]
    fn test_gap_between_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(70, NonZeroU64::new(30).unwrap());
        gaps.add_gap(50, NonZeroU64::new(10).unwrap());
        gaps.add_gap(60, NonZeroU64::new(10).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(50).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Adding a gap after a gap (with overlap) should merge them
    #[test]
    fn test_gap_append_overlap_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(50, NonZeroU64::new(11).unwrap());
        gaps.add_gap(60, NonZeroU64::new(30).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(40).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Adding a gap before a gap (with overlap) should merge them
    #[test]
    fn test_gap_prepend_overlap_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(60, NonZeroU64::new(30).unwrap());
        gaps.add_gap(50, NonZeroU64::new(11).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(40).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Adding a gap between two gaps (with overlap) should merge them
    #[test]
    fn test_gap_between_overlap_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(70, NonZeroU64::new(30).unwrap());
        gaps.add_gap(50, NonZeroU64::new(11).unwrap());
        gaps.add_gap(60, NonZeroU64::new(11).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(50).unwrap())]),
                file_end: 150,
            }
        );
    }

    /// Prepend overlapping to a small gap that we fully exhaust
    /// This is unlikely to happen in practice
    #[test]
    fn test_gap_prepend_to_small_overlap_merge() {
        let mut gaps = Gaps {
            file_end: 150,
            ..Default::default()
        };
        gaps.add_gap(60, NonZeroU64::new(2).unwrap());
        gaps.add_gap(50, NonZeroU64::new(30).unwrap());
        assert_eq!(
            gaps,
            Gaps {
                map: BTreeMap::from([(50, NonZeroU64::new(30).unwrap())]),
                file_end: 150,
            }
        );
    }
}
