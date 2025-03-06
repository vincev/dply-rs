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
use polars::lazy::dsl::{duration, DurationArgs, Expr as PolarsExpr, StrptimeOptions};
use polars::prelude::*;
use std::collections::HashSet;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a mutate call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut df) = ctx.take_df() {
        let mut used_aliases = HashSet::new();

        for arg in args {
            match arg {
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    let alias = args::identifier(lhs);
                    if used_aliases.contains(&alias) {
                        bail!("mutate error: duplicate alias '{alias}'");
                    } else {
                        used_aliases.insert(alias.clone());
                    }

                    let expr = df
                        .collect_schema()
                        .map_err(anyhow::Error::from)
                        .and_then(|schema| eval_expr(rhs, &schema))
                        .map_err(|e| anyhow!("mutate error: {e}"))?;
                    df = df.with_column(expr.alias(alias));
                }
                _ => panic!("Unexpected mutate expression: {arg}"),
            }
        }

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("mutate error: must call summarize after a group_by");
    } else {
        bail!("mutate error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr, schema: &Schema) -> Result<PolarsExpr> {
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs, schema)?;
            let rhs = eval_expr(rhs, schema)?;

            let result = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => lhs / rhs,
                Operator::Mod => lhs % rhs.cast(DataType::UInt64),
                _ => panic!("Unexpected mutate operator {op}"),
            };

            Ok(result)
        }
        Expr::Identifier(_) => args::column(expr, schema),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "ymd_hms" => {
            args::column(&args[0], schema).map(|c| {
                c.str().to_datetime(
                    Some(TimeUnit::Nanoseconds),
                    None,
                    StrptimeOptions::default(),
                    lit("raise"),
                )
            })
        }
        Expr::Function(name, args) if name == "dnanos" => args::column(&args[0], schema).map(|c| {
            duration(DurationArgs {
                nanoseconds: c,
                ..Default::default()
            })
        }),
        Expr::Function(name, args) if name == "dmicros" => {
            args::column(&args[0], schema).map(|c| {
                duration(DurationArgs {
                    microseconds: c,
                    ..Default::default()
                })
            })
        }
        Expr::Function(name, args) if name == "dmillis" => {
            args::column(&args[0], schema).map(|c| {
                duration(DurationArgs {
                    milliseconds: c,
                    ..Default::default()
                })
            })
        }
        Expr::Function(name, args) if name == "dsecs" => args::column(&args[0], schema).map(|c| {
            duration(DurationArgs {
                seconds: c,
                ..Default::default()
            })
        }),
        Expr::Function(name, args) if name == "nanos" => {
            args::column(&args[0], schema).map(|c| c.dt().total_nanoseconds())
        }
        Expr::Function(name, args) if name == "micros" => {
            args::column(&args[0], schema).map(|c| c.dt().total_microseconds())
        }
        Expr::Function(name, args) if name == "millis" => {
            args::column(&args[0], schema).map(|c| c.dt().total_milliseconds())
        }
        Expr::Function(name, args) if name == "secs" => {
            args::column(&args[0], schema).map(|c| c.dt().total_seconds())
        }
        Expr::Function(name, args) if name == "field" => {
            let field_name = args::identifier(&args[1]);
            args::column(&args[0], schema).map(|c| c.struct_().field_by_name(&field_name))
        }
        Expr::Function(name, args) if name == "mean" => {
            args::column(&args[0], schema).map(|c| c.mean())
        }
        Expr::Function(name, args) if name == "median" => {
            args::column(&args[0], schema).map(|c| c.median())
        }
        Expr::Function(name, args) if name == "min" => {
            args::column(&args[0], schema).map(|c| c.min())
        }
        Expr::Function(name, args) if name == "max" => {
            args::column(&args[0], schema).map(|c| c.max())
        }
        Expr::Function(name, args) if name == "len" => {
            let column = args::identifier(&args[0]);
            match schema.get(&column) {
                Some(DataType::List(_)) => Ok(col(column).list().len().fill_null(0)),
                Some(DataType::String) => Ok(col(column).str().len_chars()),
                Some(_) => Err(anyhow!("`len` column '{column}' must be list or String")),
                None => Err(anyhow!("Unknown column '{column}'")),
            }
        }
        Expr::Function(name, _args) if name == "row" => {
            let (col_name, _) = schema
                .get_at_index(0)
                .ok_or_else(|| anyhow!("No columns found"))?;
            Ok(col(col_name.to_owned()).map(
                |c| Ok(Some(Series::from_iter(1..=(c.len() as u64)).into())),
                GetOutput::from_type(DataType::UInt64),
            ))
        }
        _ => panic!("Unexpected mutate expression {expr}"),
    }
}
