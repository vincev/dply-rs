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
use polars::export::regex;
use polars::lazy::dsl::Expr as PolarsExpr;
use polars::prelude::*;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a filter call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut df) = ctx.take_df() {
        let schema = df.schema().map_err(|e| anyhow!("filter error: {e}"))?;

        for arg in args {
            df = df.filter(eval_expr(arg, &schema)?);
        }

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("filter error: must call summarize after a group_by");
    } else {
        bail!("filter error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr, schema: &Schema) -> Result<PolarsExpr> {
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs, schema)?;
            let rhs = eval_expr(rhs, schema)?;

            let result = match op {
                Operator::Eq => lhs.eq(rhs),
                Operator::NotEq => lhs.neq(rhs),
                Operator::Lt => lhs.lt(rhs),
                Operator::LtEq => lhs.lt_eq(rhs),
                Operator::Gt => lhs.gt(rhs),
                Operator::GtEq => lhs.gt_eq(rhs),
                Operator::And => lhs.and(rhs),
                Operator::Or => lhs.or(rhs),
                _ => panic!("Unexpected filter operator {op}"),
            };

            Ok(result)
        }
        Expr::Identifier(s) => Ok(col(s)),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "dt" => {
            let ts = args::timestamp(&args[0])?;
            Ok(lit(ts))
        }
        Expr::Function(name, args) if name == "list_contains" => {
            let column = args::identifier(&args[0]);
            let list_type = schema
                .get(&column)
                .ok_or_else(|| anyhow!("filter error: Unknown column {column}"))?;

            if let DataType::List(etype) = list_type {
                list_contains(&column, &args[1], etype)
            } else {
                Err(anyhow!(
                    "filter error: list_contains {column} is not a list type"
                ))
            }
        }
        _ => panic!("Unexpected filter expression {expr}"),
    }
}

fn list_contains(column: &str, pattern: &Expr, ctype: &DataType) -> Result<PolarsExpr> {
    use DataType::*;

    match (ctype, pattern) {
        (Int8, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as i8))),
        (Int16, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as i16))),
        (Int32, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as i32))),
        (Int64, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as i64))),
        (UInt8, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as u8))),
        (UInt16, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as u16))),
        (UInt32, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as u32))),
        (UInt64, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as u64))),
        (Float32, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n as f32))),
        (Float64, Expr::Number(n)) => Ok(col(column).arr().contains(lit(*n))),
        (Utf8, Expr::String(s)) => {
            let re = regex::Regex::new(&format!("(?i:{s})"))?;

            let function = move |s: Series| {
                let ca = s.list()?;
                let mut bools = Vec::with_capacity(ca.len());

                ca.into_iter().for_each(|arr| {
                    let found = if let Some(s) = arr {
                        s.utf8()
                            .map(|ca| {
                                ca.into_iter()
                                    .any(|s| s.map(|s| re.is_match(s)).unwrap_or(false))
                            })
                            .unwrap_or_default()
                    } else {
                        false
                    };

                    bools.push(found);
                });

                Ok(Some(BooleanChunked::new(ca.name(), bools).into_series()))
            };

            Ok(col(column).map(function, GetOutput::from_type(DataType::Boolean)))
        }
        _ => bail!("filter error: list_contains type mismatch for column {column} {ctype}"),
    }
}
