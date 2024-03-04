// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a group_by call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !schema_cols.contains(column) {
                    bail!("group_by error: Unknown column {column}");
                }

                let expr = col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        ctx.set_group(df.group_by_stable(&columns))?;
    } else {
        bail!("group_by error: missing input dataframe");
    }

    Ok(())
}
