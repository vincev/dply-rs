// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

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
