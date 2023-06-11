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

/// Simple fuzzy matcher.
///
/// Inspired by: https://github.com/forrestthewoods/lib_fts
pub struct Matcher {
    pattern: String,
}

impl Matcher {
    pub fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_lowercase(),
        }
    }

    pub fn is_match(&self, text: &str) -> bool {
        let mut pit = self.pattern.chars().peekable();

        for c in text.chars() {
            if let Some(p) = pit.peek() {
                if p.eq_ignore_ascii_case(&c) {
                    pit.next();
                }
            } else {
                break;
            }
        }

        pit.peek().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_match() {
        assert!(Matcher::new("slt").is_match("select"));
        assert!(Matcher::new("SlT").is_match("select"));
        assert!(Matcher::new("ee").is_match("select"));
        assert!(Matcher::new("_").is_match("payment_type"));
        assert!(Matcher::new("PY").is_match("payment_type"));
        assert!(Matcher::new("").is_match("select"));
    }

    #[test]
    fn contains_no_match() {
        assert!(!Matcher::new("az").is_match("select"));
        assert!(!Matcher::new("eee").is_match("select"));
        assert!(!Matcher::new("stt").is_match("select"));
    }
}
