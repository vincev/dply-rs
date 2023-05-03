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
use anyhow::{bail, Result};
use polars::prelude::*;

use crate::parser::{Expr, Operator};

use super::*;

/// Where to relocate the selected columns.
enum RelocateTo {
    /// Relocate at the beginning.
    Default,
    /// Relocate before the given column.
    Before(String),
    /// Relocate after the given column.
    After(String),
}

/// Evaluates a select call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let mut relocate_cols = Vec::new();
        let mut relocate_to = RelocateTo::Default;

        for arg in args {
            match arg {
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    // before or after
                    let dest = args::identifier(lhs);
                    let pos = args::identifier(rhs);

                    if !ctx.columns().contains(&pos) {
                        bail!("relocate error: Unknown {dest} column {pos}");
                    }

                    relocate_to = if dest == "before" {
                        RelocateTo::Before(pos)
                    } else {
                        RelocateTo::After(pos)
                    };
                }
                Expr::Identifier(column) => {
                    if !ctx.columns().contains(column) {
                        bail!("relocate error: Unknown column {column}");
                    }

                    if !relocate_cols.contains(column) {
                        relocate_cols.push(column.to_owned());
                    }
                }
                _ => {}
            }
        }

        let mut schema_cols = ctx.columns().to_vec();
        match relocate_to {
            RelocateTo::Default => {
                // Relocate columns to the left.
                schema_cols.retain(|c| !relocate_cols.contains(c));
                schema_cols.splice(0..0, relocate_cols);
            }
            RelocateTo::Before(before_col) => {
                relocate_cols.retain(|c| c != &before_col);
                schema_cols.retain(|c| !relocate_cols.contains(c));
                let pos = schema_cols.iter().position(|c| c == &before_col).unwrap();
                schema_cols.splice(pos..pos, relocate_cols);
            }
            RelocateTo::After(after_col) => {
                relocate_cols.retain(|c| c != &after_col);
                schema_cols.retain(|c| !relocate_cols.contains(c));
                let pos = schema_cols.iter().position(|c| c == &after_col).unwrap() + 1;
                schema_cols.splice(pos..pos, relocate_cols);
            }
        };

        let columns = schema_cols.into_iter().map(|c| col(&c)).collect::<Vec<_>>();
        ctx.set_df(df.select(&columns))?;
    } else if ctx.is_grouping() {
        bail!("relocate error: must call summarize after a group_by");
    } else {
        bail!("relocate error: missing input dataframe");
    }

    Ok(())
}
