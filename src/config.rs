// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

/// Configuration for table formatting.
#[derive(Debug, Clone, Copy)]
pub struct FormatConfig {
    /// Maximum number or table columns.
    pub max_columns: usize,
    /// Maximum column width
    pub max_column_width: usize,
    /// Maximum table width, use default if None
    pub max_table_width: Option<usize>,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            max_columns: 8,
            max_column_width: 80,
            max_table_width: None,
        }
    }
}
