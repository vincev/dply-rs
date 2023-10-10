// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use datafusion::logical_expr::LogicalPlanBuilder;

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
    if let Some(plan) = ctx.take_plan() {
        let mut schema_cols = ctx.columns().clone();
        let mut relocate_cols = Vec::new();
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

                    if !relocate_cols.contains(column) {
                        relocate_cols.push(column.to_owned());
                    }
                }
                _ => {}
            }
        }

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

        let columns = schema_cols
            .into_iter()
            .map(args::str_to_col)
            .collect::<Vec<_>>();

        let plan = LogicalPlanBuilder::from(plan).project(columns)?.build()?;

        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("relocate error: must call summarize after a group_by");
    } else {
        bail!("relocate error: missing input dataframe");
    }

    Ok(())
}
