use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub tie: Tie,
    pub offset: u64,
    pub size: NonZeroU64,
}

impl Record {
    pub fn new(offset: u64, size: NonZeroU64) -> Self {
        Record {
            tie: Tie::Live,
            offset,
            size,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tie {
    Live,
    Unstaged,
}

impl Tie {
    pub fn detect(record: &str) -> Option<(Self, &str)> {
        let mut chars = record.chars();
        let tie = chars.next()?;
        let tie = match tie {
            ' ' => Tie::Live,
            '\t' => Tie::Unstaged,
            _ => return None,
        };
        Some((tie, chars.as_str()))
    }
}

// This is a very naive implementation that needs improvement later
#[derive(Serialize, Deserialize)]
pub struct Entry<T> {
    #[serde(flatten)]
    pub data: BTreeMap<String, T>,
}

impl<T> Entry<T> {
    pub fn new(key: String, value: T) -> Self {
        let mut data = BTreeMap::new();
        data.insert(key, value);
        Entry { data }
    }

    pub fn serialize(&self) -> Result<String, serde_json::Error>
    where
        T: Serialize,
    {
        serde_json::to_string(self)
    }

    pub fn deserialize(s: &str) -> Result<Self, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_str(s)
    }

    pub fn key(self) -> String {
        // TODO: due to the way we currently use BTreeMap for serde, we don't enforce the map has exactly one entry, so this may panic
        self.data.into_keys().next().unwrap()
    }

    pub fn value(self) -> T {
        // TODO: due to the way we currently use BTreeMap for serde, we don't enforce the map has exactly one entry, so this may panic
        self.data.into_values().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tie_detect() {
        assert_eq!(Tie::detect(" {}"), Some((Tie::Live, "{}")));
        assert_eq!(Tie::detect("\t{}"), Some((Tie::Unstaged, "{}")));
        assert_eq!(Tie::detect("x"), None);
    }

    #[test]
    fn test_serialize() {
        let entry = Entry {
            data: BTreeMap::from([("key".to_string(), "value".to_string())]),
        };
        let serialized = serde_json::to_string(&entry).unwrap();
        assert_eq!(serialized, r#"{"key":"value"}"#);
    }

    #[test]
    fn test_deserialize() {
        let serialized = r#"{"key":"value"}"#;
        let entry: Entry<String> = serde_json::from_str(serialized).unwrap();
        assert_eq!(entry.data.get("key"), Some(&"value".to_string()));
    }
}
