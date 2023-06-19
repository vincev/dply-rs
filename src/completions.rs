// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use lru::LruCache;
use std::num::NonZeroUsize;

const MAX_ENTRIES: usize = 32;

/// A completions LRU cache.
pub struct Completions {
    completions: LruCache<String, ()>,
}

impl Default for Completions {
    fn default() -> Self {
        Self {
            completions: LruCache::new(NonZeroUsize::new(MAX_ENTRIES).unwrap()),
        }
    }
}

impl Completions {
    /// Add a new completion entry
    pub fn add(&mut self, entry: &str) {
        if self.completions.get(entry).is_none() {
            self.completions.put(entry.to_string(), ());
        }
    }

    /// Returns an iterator to the completions.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.completions.iter().map(|(k, _)| k.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completions() {
        let mut completions = Completions::default();

        for idx in 0..MAX_ENTRIES + 10 {
            completions.add(&format!("entry{idx}"));
        }

        // Reinsert same late entries
        for idx in 20..MAX_ENTRIES + 10 {
            completions.add(&format!("entry{idx}"));
        }

        let items = completions.iter().collect::<Vec<_>>();
        assert!(items.len() == MAX_ENTRIES);

        // First 10 should be evicted
        for idx in 0..10 {
            assert!(!items.contains(&format!("entry{idx}").as_str()));
        }
    }
}
