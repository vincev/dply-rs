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
use polars::lazy::dsl::{Expr as PolarsExpr, StrpTimeOptions};
use polars::prelude::*;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a mutate call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut df) = ctx.take_df() {
        for arg in args {
            match arg {
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    let alias = args::identifier(lhs);
                    let expr = eval_expr(rhs)?;
                    df = df.with_column(expr.alias(&alias));
                }
                _ => panic!("Unexpected mutate expression: {arg}"),
            }
        }

        ctx.set_df(df);
    } else if ctx.is_grouping() {
        bail!("mutate error: must call summarize after a group_by");
    } else {
        bail!("mutate error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr) -> Result<PolarsExpr> {
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs)?;
            let rhs = eval_expr(rhs)?;

            let result = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => lhs / rhs,
                _ => panic!("Unexpected mutate operator {op}"),
            };

            Ok(result)
        }
        Expr::Identifier(s) => Ok(col(s)),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "dt" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).str().strptime(StrpTimeOptions {
                date_dtype: DataType::Datetime(TimeUnit::Nanoseconds, None),
                ..Default::default()
            }))
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
        Expr::Function(name, args) if name == "max" => {
            let column = args::identifier(&args[0]);
            Ok(col(&column).max())
        }
        _ => panic!("Unexpected mutate expression {expr}"),
    }
}
