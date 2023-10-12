// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};

use crate::parser::Expr;

use super::*;

/// Evaluates a glimpse call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(_args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        ctx.glimpse(plan)?;
    } else if ctx.is_grouping() {
        bail!("glimpse error: must call summarize after a group_by");
    } else {
        bail!("glimpse error: missing input dataframe");
    }

    Ok(())
}
