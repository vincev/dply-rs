// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};

use crate::parser::Expr;

use super::*;

/// Evaluates a group_by call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !schema_cols.contains(column) {
                    bail!("group_by error: Unknown column {column}");
                }

                let expr = args::str_to_col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        ctx.set_group(plan, columns);
    } else {
        bail!("group_by error: missing input dataframe");
    }

    Ok(())
}
