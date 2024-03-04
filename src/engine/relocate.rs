// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
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
        let schema_cols = ctx.columns();
        let mut relocate_cols = Vec::<&str>::new();
        let mut relocate_to = RelocateTo::Default;

        for arg in args {
            match arg {
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    // before or after
                    let dest = args::identifier(lhs);
                    let pos = args::identifier(rhs);

                    if !schema_cols.contains(&pos) {
                        bail!("relocate error: Unknown {dest} column {pos}");
                    }

                    relocate_to = if dest == "before" {
                        RelocateTo::Before(pos)
                    } else {
                        RelocateTo::After(pos)
                    };
                }
                Expr::Identifier(column) => {
                    if !schema_cols.contains(column) {
                        bail!("relocate error: Unknown column {column}");
                    }

                    if !relocate_cols.contains(&column.as_str()) {
                        relocate_cols.push(column);
                    }
                }
                _ => {}
            }
        }

        let mut schema_cols = schema_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>();
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

        let columns = schema_cols.into_iter().map(col).collect::<Vec<_>>();
        ctx.set_df(df.select(&columns))?;
    } else if ctx.is_grouping() {
        bail!("relocate error: must call summarize after a group_by");
    } else {
        bail!("relocate error: missing input dataframe");
    }

    Ok(())
}
