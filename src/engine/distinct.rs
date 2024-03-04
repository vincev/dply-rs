// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a distinct call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let schema_cols = ctx.columns();
        let mut select_columns = Vec::new();

        for arg in args {
            let column = args::identifier(arg);
            if !schema_cols.contains(&column) {
                bail!("distinct error: Unknown column {column}");
            }

            if !select_columns.contains(&column) {
                select_columns.push(column);
            }
        }

        let df = if !select_columns.is_empty() {
            let columns = select_columns.iter().map(|c| col(c)).collect::<Vec<_>>();
            df.select(&columns)
                .unique_stable(Some(select_columns), UniqueKeepStrategy::First)
        } else {
            df.unique_stable(None, UniqueKeepStrategy::First)
        };

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("distinct error: must call summarize after a group_by");
    } else {
        bail!("distinct error: missing input dataframe");
    }

    Ok(())
}
