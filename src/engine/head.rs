// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use datafusion::logical_expr::LogicalPlanBuilder;

use crate::parser::Expr;

use super::*;

/// Evaluates a head call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let limit = if !args.is_empty() {
            args::number(&args[0]) as usize
        } else {
            10
        };

        let plan = LogicalPlanBuilder::from(plan)
            .limit(0, Some(limit))?
            .build()?;

        ctx.show(plan)?;
    } else if ctx.is_grouping() {
        bail!("head error: must call summarize after a group_by");
    } else {
        bail!("head error: missing input dataframe");
    }

    Ok(())
}
