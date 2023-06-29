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

/// Configuration for table formatting.
#[derive(Debug, Clone, Copy)]
pub struct FormatConfig {
    /// Maximum number or table columns.
    pub max_columns: usize,
    /// Maximum column width
    pub max_column_width: usize,
    /// Maximum table width, use default if None
    pub max_table_width: Option<u16>,
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
