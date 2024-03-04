// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};

use crate::parser::Expr;

use super::*;

/// Evaluates a show call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(_args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let df = df.collect()?;
        ctx.print(df)?;
    } else if ctx.is_grouping() {
        bail!("show error: must call summarize after a group_by");
    } else {
        bail!("show error: missing input dataframe");
    }

    Ok(())
}
