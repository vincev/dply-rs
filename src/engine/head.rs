// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};

use crate::parser::Expr;

use super::*;

/// Evaluates a head call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let limit = if !args.is_empty() {
            args::number(&args[0]) as u32
        } else {
            10
        };

        let df = df.limit(limit).collect()?;
        ctx.print(df)?;
    } else if ctx.is_grouping() {
        bail!("head error: must call summarize after a group_by");
    } else {
        bail!("head error: missing input dataframe");
    }

    Ok(())
}
