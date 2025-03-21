// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use lru::LruCache;
use polars::prelude::*;
use regex::Regex;

const MAX_ENTRIES: usize = 40;

/// A completions LRU cache.
pub struct Completions {
    lru: LruCache<String, ()>,
    name_re: regex::Regex,
}

impl Default for Completions {
    fn default() -> Self {
        Self {
            lru: LruCache::unbounded(),
            name_re: Regex::new(r"^[[:alpha:]](_|\d|[[:alpha:]])+$").unwrap(),
        }
    }
}

impl Completions {
    /// Add entries to completions history.
    pub fn add(&mut self, entries: &[PlSmallStr]) {
        // Make sure these entries are in the cache irrespective of their size
        // to handle the case where we have a dataframe with many columns.
        if entries.len() > MAX_ENTRIES {
            self.lru.clear();
        } else if entries.len() + self.lru.len() > MAX_ENTRIES {
            let to_remove = entries.len() + self.lru.len() - MAX_ENTRIES;
            for _ in 0..to_remove {
                self.lru.pop_lru();
            }
        }

        for entry in entries {
            self.add_entry(entry);
        }
    }

    /// Returns an iterator to the completions.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.lru.iter().map(|(k, _)| k.as_str())
    }

    fn add_entry(&mut self, entry: &str) {
        if self.lru.get(entry).is_none() {
            // Add backticks to completions if entry is not a valid name.
            let entry = if self.name_re.is_match(entry) {
                entry.to_string()
            } else {
                format!("`{entry}`")
            };

            self.lru.put(entry, ());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completions() {
        let mut completions = Completions::default();

        // This insertion should have all columns, simulates a dataframe that has
        // more columns than MAX_ENTRIES.
        let entries = (0..MAX_ENTRIES + 10)
            .map(|idx| PlSmallStr::from_string(format!("entry{idx}")))
            .collect::<Vec<_>>();

        completions.add(&entries);
        assert_eq!(completions.iter().count(), entries.len());

        // Inserts a subsets of columns smaller than MAX_ENTRIES.
        let entries = (1000..1020)
            .map(|idx| PlSmallStr::from_string(format!("entry{idx}")))
            .collect::<Vec<_>>();
        completions.add(&entries);

        assert_eq!(completions.iter().count(), MAX_ENTRIES);

        // Most recently used are first.
        for (entry, (cached, _)) in entries.iter().rev().zip(completions.lru.iter()) {
            assert_eq!(entry, cached);
        }

        let entries = (2000..2200)
            .map(|idx| PlSmallStr::from_string(format!("entry{idx}")))
            .collect::<Vec<_>>();
        completions.add(&entries);
        assert_eq!(completions.iter().count(), entries.len());

        let entries = (3000..3100)
            .map(|idx| PlSmallStr::from_string(format!("entry{idx}")))
            .collect::<Vec<_>>();
        completions.add(&entries);
        assert_eq!(completions.iter().count(), entries.len());

        for (entry, (cached, _)) in entries.iter().rev().zip(completions.lru.iter()) {
            assert_eq!(entry, cached);
        }
    }
}
