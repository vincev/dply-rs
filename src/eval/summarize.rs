// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use anyhow::{bail, Result};
use polars::lazy::dsl::Expr as PolarsExpr;
use polars::prelude::*;
use std::collections::HashSet;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a summarize call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(group) = ctx.take_group() {
        let columns = eval_args(args, ctx)?;
        ctx.set_df(group.agg(&columns))?;
    } else if let Some(df) = ctx.take_df() {
        let columns = eval_args(args, ctx)?;
        ctx.set_df(df.select(&columns))?;
    } else {
        bail!("summarize error: missing input group or dataframe");
    }

    Ok(())
}

fn eval_args(args: &[Expr], ctx: &mut Context) -> Result<Vec<PolarsExpr>> {
    let schema_cols = ctx.columns();
    let mut aliases = HashSet::new();
    let mut columns = Vec::new();

    for arg in args {
        match arg {
            Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                let alias = args::identifier(lhs);
                if aliases.contains(&alias) {
                    bail!("summarize error: duplicate alias {alias}");
                }

                aliases.insert(alias.clone());

                let expr = eval_expr(rhs, schema_cols)?;
                columns.push(expr.alias(&alias));
            }
            _ => panic!("Unexpected summarize expression: {arg}"),
        }
    }

    Ok(columns)
}

fn eval_expr(expr: &Expr, cols: &[String]) -> Result<PolarsExpr> {
    match expr {
        Expr::Function(name, _) if name == "n" => Ok(col(&cols[0]).count()),
        Expr::Function(name, args) if name == "max" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).max())
        }
        Expr::Function(name, args) if name == "mean" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).mean())
        }
        Expr::Function(name, args) if name == "median" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).median())
        }
        Expr::Function(name, args) if name == "min" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).min())
        }
        Expr::Function(name, args) if name == "quantile" => {
            let column = args::identifier(&args[0]);
            let quantile = args::number(&args[1]);
            Ok(col(&column).quantile(lit(quantile), QuantileInterpolOptions::Linear))
        }
        Expr::Function(name, args) if name == "sd" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).std(1))
        }
        Expr::Function(name, args) if name == "sum" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).sum())
        }
        Expr::Function(name, args) if name == "var" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).var(1))
        }
        _ => panic!("Unexpected summarize expression {expr}"),
    }
}
