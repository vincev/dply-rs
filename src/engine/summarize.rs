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
        let columns = group
            .logical_plan
            .compute_schema()
            .map_err(anyhow::Error::from)
            .and_then(|schema| eval_args(args, ctx, &schema, true))
            .map_err(|e| anyhow!("summarize error: {e}"))?;
        ctx.set_df(group.agg(&columns))?;
    } else if let Some(mut df) = ctx.take_df() {
        let columns = df
            .schema()
            .map_err(anyhow::Error::from)
            .and_then(|schema| eval_args(args, ctx, &schema, false))
            .map_err(|e| anyhow!("summarize error: {e}"))?;
        ctx.set_df(df.select(&columns))?;
    } else {
        bail!("summarize error: missing input group or dataframe");
    }

    Ok(())
}

fn eval_args(
    args: &[Expr],
    ctx: &mut Context,
    schema: &Schema,
    grouping: bool,
) -> Result<Vec<PolarsExpr>> {
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

                let column = match rhs.as_ref() {
                    Expr::Function(name, _) if name == "n" => Ok(col(&schema_cols[0]).count()),
                    Expr::Function(name, args) if name == "list" => args::column(&args[0], schema)
                        .map(|c| if grouping { c } else { c.implode() }),
                    Expr::Function(name, args) if name == "max" => {
                        args::column(&args[0], schema).map(|c| c.max())
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
                    Expr::Function(name, args) if name == "quantile" => {
                        let quantile = args::number(&args[1]);
                        args::column(&args[0], schema)
                            .map(|c| c.quantile(lit(quantile), QuantileInterpolOptions::Linear))
                    }
                    Expr::Function(name, args) if name == "sd" => {
                        args::column(&args[0], schema).map(|c| c.std(1))
                    }
                    Expr::Function(name, args) if name == "sum" => {
                        args::column(&args[0], schema).map(|c| c.sum())
                    }
                    Expr::Function(name, args) if name == "var" => {
                        args::column(&args[0], schema).map(|c| c.var(1))
                    }
                    _ => panic!("Unexpected summarize expression {rhs}"),
                }?;

                columns.push(column.alias(&alias));
            }
            _ => panic!("Unexpected summarize expression: {arg}"),
        }
    }

    Ok(columns)
}
