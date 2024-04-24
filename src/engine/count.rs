// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a count call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !schema_cols.contains(column) {
                    bail!("count error: Unknown column {column}");
                }

                let expr = col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        let agg_col = find_agg_column(schema_cols.as_slice());

        let df = if !columns.is_empty() {
            let ncol = columns.last().unwrap().clone();
            let df = df
                .group_by(&columns)
                .agg([ncol.is_not_null().count().alias(&agg_col)]);

            let mut descending = vec![false; columns.len()];

            if args::named_bool(args, "sort")? {
                columns.insert(0, col(&agg_col));
                descending.insert(0, true);
            }

            let sort_opts = SortMultipleOptions {
                descending,
                ..Default::default()
            };

            df.sort_by_exprs(columns, sort_opts)
        } else {
            df.select(&[col(&schema_cols[0]).count().alias(&agg_col)])
        };

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("count error: must call summarize after a group_by");
    } else {
        bail!("count error: missing input dataframe");
    }

    Ok(())
}

/// If there is a column named `n` use `nn`, or `nnn`, etc.
fn find_agg_column(cols: &[String]) -> String {
    let mut col = "n".to_string();

    while cols.contains(&col) {
        col.push('n');
    }

    col
}
