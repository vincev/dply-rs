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
pub struct FormatConfig {
    /// Maximum number or table columns.
    max_columns: usize,
    /// Maximum column width
    max_colwidth: usize,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            max_columns: 8,
            max_colwidth: 80,
        }
    }
}

impl FormatConfig {
    /// Return the maximum number of table columns.
    ///
    /// If a table has more columns than this value the columns to the right are
    /// truncated.
    pub fn max_columns(&self) -> usize {
        self.max_columns
    }

    /// Return the maximum column width.
    ///
    /// A column value with a length greater than this maximum is truncated.
    pub fn max_colwidth(&self) -> usize {
        self.max_colwidth
    }
}
