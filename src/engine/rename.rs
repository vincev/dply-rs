// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use datafusion::logical_expr::LogicalPlanBuilder;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a rename call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let mut schema_cols = ctx
            .columns()
            .iter()
            .map(args::str_to_col)
            .collect::<Vec<_>>();

        for arg in args {
            if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
                let alias = args::identifier(lhs);
                let column = args::str_to_col(args::identifier(rhs));

                if let Some(idx) = schema_cols.iter().position(|c| c == &column) {
                    schema_cols[idx] = schema_cols[idx].clone().alias(&alias);
                } else {
                    bail!("rename error: Unknown column {column}");
                }
            }
        }

        let plan = LogicalPlanBuilder::from(plan)
            .project(schema_cols)?
            .build()?;
        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("rename error: must call summarize after a group_by");
    } else {
        bail!("rename error: missing input dataframe");
    }

    Ok(())
}
