// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use datafusion::logical_expr::LogicalPlanBuilder;

use crate::parser::Expr;

use super::*;

/// Evaluates a distinct call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut select_columns = Vec::new();

        for arg in args {
            let column = args::identifier(arg);
            if !schema_cols.contains(&column) {
                bail!("distinct error: Unknown column {column}");
            }

            if !select_columns.contains(&column) {
                select_columns.push(column);
            }
        }

        let plan = if !select_columns.is_empty() {
            let columns = select_columns
                .iter()
                .map(args::str_to_col)
                .collect::<Vec<_>>();

            LogicalPlanBuilder::from(plan)
                .project(columns)?
                .distinct()?
                .build()?
        } else {
            LogicalPlanBuilder::from(plan).distinct()?.build()?
        };

        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("distinct error: must call summarize after a group_by");
    } else {
        bail!("distinct error: missing input dataframe");
    }

    Ok(())
}
