// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use datafusion::logical_expr::LogicalPlanBuilder;

use crate::parser::Expr;

use super::*;

/// Evaluates an arrange call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::with_capacity(args.len());

        for arg in args {
            match arg {
                Expr::Function(name, args) if name == "desc" => {
                    // arrange(desc(column))
                    let column = args::identifier(&args[0]);
                    if !schema_cols.contains(&column) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(args::str_to_col(&column).sort(false, false));
                }
                Expr::Identifier(column) => {
                    // arrange(column)
                    if !schema_cols.contains(column) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(args::str_to_col(column).sort(true, false));
                }
                _ => {}
            }
        }

        let plan = LogicalPlanBuilder::from(plan).sort(columns)?.build()?;
        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("arrange error: must call summarize after a group_by");
    } else {
        bail!("arrange error: missing input dataframe");
    }

    Ok(())
}
