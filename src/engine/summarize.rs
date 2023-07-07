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
use anyhow::{anyhow, bail, Result};
use datafusion::{
    common::DFSchema,
    logical_expr::{
        aggregate_function::AggregateFunction, expr, expr_fn, lit, Expr as DFExpr,
        LogicalPlanBuilder,
    },
};
use std::collections::HashSet;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a summarize call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema = plan.schema();

        let exprs = eval_args(args, schema).map_err(|e| anyhow!("summarize error: {e}"))?;

        let group = ctx.take_group().unwrap_or_default();

        let plan = LogicalPlanBuilder::from(plan)
            .aggregate(group, exprs)?
            .build()?;

        ctx.set_plan(plan);
    } else {
        bail!("summarize error: missing input group or dataframe");
    }

    Ok(())
}

fn eval_args(args: &[Expr], schema: &DFSchema) -> Result<Vec<DFExpr>> {
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
                    Expr::Function(name, _) if name == "n" => Ok(expr_fn::count(lit(1))),
                    Expr::Function(name, args) if name == "list" => {
                        args::expr_to_col(&args[0], schema).map(list)
                    }
                    Expr::Function(name, args) if name == "max" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::max)
                    }
                    Expr::Function(name, args) if name == "mean" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::avg)
                    }
                    Expr::Function(name, args) if name == "median" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::median)
                    }
                    Expr::Function(name, args) if name == "min" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::min)
                    }
                    Expr::Function(name, args) if name == "quantile" => {
                        let quantile = args::number(&args[1]);
                        args::expr_to_col(&args[0], schema)
                            .map(|c| expr_fn::approx_percentile_cont(c, lit(quantile)))
                    }
                    Expr::Function(name, args) if name == "sd" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::stddev)
                    }
                    Expr::Function(name, args) if name == "sum" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::sum)
                    }
                    Expr::Function(name, args) if name == "var" => {
                        args::expr_to_col(&args[0], schema).map(var)
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

fn var(expr: DFExpr) -> DFExpr {
    DFExpr::AggregateFunction(expr::AggregateFunction::new(
        AggregateFunction::Variance,
        vec![expr],
        false,
        None,
        None,
    ))
}

fn list(expr: DFExpr) -> DFExpr {
    DFExpr::AggregateFunction(expr::AggregateFunction::new(
        AggregateFunction::ArrayAgg,
        vec![expr],
        false,
        None,
        None,
    ))
}
