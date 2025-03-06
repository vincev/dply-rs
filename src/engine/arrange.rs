// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates an arrange call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::with_capacity(args.len());
        let mut descending = Vec::with_capacity(args.len());

        for arg in args {
            match arg {
                Expr::Function(name, args) if name == "desc" => {
                    // arrange(desc(column))
                    let column = args::identifier(&args[0]);
                    if !schema_cols.contains(&column) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(col(column));
                    descending.push(true);
                }
                Expr::Identifier(column) => {
                    // arrange(column)
                    if !schema_cols.contains(&PlSmallStr::from_str(column)) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(col(column));
                    descending.push(false);
                }
                _ => {}
            }
        }

        let sort_opts = SortMultipleOptions {
            descending,
            nulls_last: vec![true],
            ..Default::default()
        };

        ctx.set_df(df.sort_by_exprs(columns, sort_opts))?;
    } else if ctx.is_grouping() {
        bail!("arrange error: must call summarize after a group_by");
    } else {
        bail!("arrange error: missing input dataframe");
    }

    Ok(())
}
