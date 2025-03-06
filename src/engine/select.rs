// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{bail, Result};
use polars::lazy::dsl::Expr as PolarsExpr;
use polars::prelude::*;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a select call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let schema_cols = ctx.columns();
        let mut select_columns = Vec::new();

        for arg in args {
            match arg {
                Expr::Function(_, _) => {
                    let mut filter_cols = filter_columns(arg, schema_cols, false);
                    filter_cols.retain(|e| !select_columns.contains(e));
                    select_columns.extend(filter_cols);
                }
                Expr::UnaryOp(Operator::Not, expr) => {
                    let mut filter_cols = filter_columns(expr, schema_cols, true);
                    filter_cols.retain(|e| !select_columns.contains(e));
                    select_columns.extend(filter_cols);
                }
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    // select(alias = column)
                    let alias = args::identifier(lhs);
                    let column = args::identifier(rhs);
                    let expr = col(column).alias(alias);

                    if !select_columns.contains(&expr) {
                        select_columns.push(expr);
                    }
                }
                Expr::Identifier(column) => {
                    // select(column)
                    let column = PlSmallStr::from_str(column);
                    if !schema_cols.contains(&column) {
                        bail!("select error: Unknown column {column}");
                    }

                    let expr = col(column);
                    if !select_columns.contains(&expr) {
                        select_columns.push(expr);
                    }
                }
                _ => {}
            }
        }

        ctx.set_df(df.select(&select_columns))?;
    } else if ctx.is_grouping() {
        bail!("select error: must call summarize after a group_by");
    } else {
        bail!("select error: missing input dataframe");
    }

    Ok(())
}

fn filter_columns(expr: &Expr, schema_cols: &[PlSmallStr], negate: bool) -> Vec<PolarsExpr> {
    match expr {
        Expr::Function(name, args) if name == "starts_with" => {
            // select(starts_with("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.starts_with(&pattern) ^ negate)
                .map(|c| col(c.to_owned()))
                .collect()
        }
        Expr::Function(name, args) if name == "ends_with" => {
            // select(ends_with("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.ends_with(&pattern) ^ negate)
                .map(|c| col(c.to_owned()))
                .collect()
        }
        Expr::Function(name, args) if name == "contains" => {
            // select(contains("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.contains(&pattern) ^ negate)
                .map(|c| col(c.to_owned()))
                .collect()
        }
        _ => Vec::new(),
    }
}
