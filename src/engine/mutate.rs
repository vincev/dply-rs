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
    arrow::{
        array::{ArrayRef, UInt32Array},
        datatypes::*,
    },
    common::{
        cast::as_list_array,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{
        aggregate_function::AggregateFunction, expr, expr_fn, utils, window_frame::WindowFrame,
        Expr as DFExpr, LogicalPlanBuilder, Volatility, WindowFunction,
    },
    physical_plan::functions::make_scalar_function,
};

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a mutate call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut plan) = ctx.take_plan() {
        for arg in args {
            match arg {
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    // Save current plan columns for projection
                    let schema_cols = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_owned())
                        .collect::<Vec<_>>();

                    let alias = args::identifier(lhs);
                    let expr = eval_expr(rhs, &plan)
                        .map_err(|e| anyhow!("mutate error: {e}"))?
                        .alias(&alias);

                    // Extract window functions for evaluation before project.
                    let window_exprs = utils::find_window_exprs(&[expr.clone()]);
                    plan = LogicalPlanBuilder::window_plan(plan, window_exprs)?;

                    // Transform window functions expression to column expressions
                    // so that we can use them in the final projection plan.
                    let expr = expr.transform(&|expr| {
                        if matches!(expr, DFExpr::WindowFunction { .. }) {
                            let expr = utils::expr_as_column_expr(&expr, &plan)?;
                            Ok(Transformed::Yes(expr))
                        } else {
                            Ok(Transformed::No(expr))
                        }
                    })?;

                    // Replace or append evaluated expression for projection.
                    let mut columns = schema_cols.iter().map(args::str_to_col).collect::<Vec<_>>();
                    if let Some(idx) = schema_cols.iter().position(|c| c == &alias) {
                        columns[idx] = expr;
                    } else {
                        columns.push(expr);
                    };

                    plan = LogicalPlanBuilder::from(plan).project(columns)?.build()?;
                }
                _ => panic!("Unexpected mutate expression: {arg}"),
            }
        }

        ctx.set_plan(plan)?;
    } else if ctx.is_grouping() {
        bail!("mutate error: must call summarize after a group_by");
    } else {
        bail!("mutate error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr, plan: &LogicalPlan) -> Result<DFExpr> {
    let schema = plan.schema();
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs, plan)?;
            let rhs = eval_expr(rhs, plan)?;

            let result = match op {
                Operator::Plus => lhs + rhs,
                Operator::Minus => lhs - rhs,
                Operator::Multiply => lhs * rhs,
                Operator::Divide => lhs / rhs,
                _ => panic!("Unexpected mutate operator {op}"),
            };

            Ok(result)
        }
        Expr::Identifier(_) => args::expr_to_col(expr, plan.schema()),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "dt" => {
            args::expr_to_col(&args[0], schema).map(expr_fn::to_timestamp_millis)
        }
        Expr::Function(name, args) if name == "mean" => {
            args::expr_to_qualified_col(&args[0], schema)
                .map(|e| window_fn(e, AggregateFunction::Avg))
        }
        Expr::Function(name, args) if name == "median" => {
            args::expr_to_qualified_col(&args[0], schema)
                .map(|e| window_fn(e, AggregateFunction::Median))
        }
        Expr::Function(name, args) if name == "min" => {
            args::expr_to_qualified_col(&args[0], schema)
                .map(|e| window_fn(e, AggregateFunction::Min))
        }
        Expr::Function(name, args) if name == "max" => {
            args::expr_to_qualified_col(&args[0], schema)
                .map(|e| window_fn(e, AggregateFunction::Max))
        }
        Expr::Function(name, args) if name == "to_ns" => {
            let arg = if let Expr::Identifier(id) = &args[0] {
                let data_type = plan
                    .schema()
                    .field_with_unqualified_name(id)
                    .map_err(|_| anyhow!("to_ns: Unknown columns {id}"))?;
                let arg = args::str_to_col(id);
                if let DataType::Interval(_) = data_type.data_type() {
                    cast(arg, DataType::Duration(TimeUnit::Nanosecond))
                } else {
                    arg
                }
            } else {
                // For complex expressions treat it as an interval.
                let arg = eval_expr(&args[0], plan)?;
                cast(arg, DataType::Duration(TimeUnit::Nanosecond))
            };

            Ok(cast(arg, DataType::Int64))
        }
        Expr::Function(name, args) if name == "len" => {
            let column = args::identifier(&args[0]);
            match schema
                .field_with_unqualified_name(&column)
                .map(|f| f.data_type())
            {
                Ok(dt @ DataType::List(_)) => list_len(&column, dt),
                Ok(_) => Err(anyhow!("`len` column '{column}' must be list")),
                Err(_) => Err(anyhow!("Unknown column '{column}'")),
            }
        }
        _ => panic!("Unexpected mutate expression {expr}"),
    }
}

fn window_fn(expr: DFExpr, agg: AggregateFunction) -> DFExpr {
    DFExpr::WindowFunction(expr::WindowFunction::new(
        WindowFunction::AggregateFunction(agg),
        vec![expr],
        vec![],
        vec![],
        WindowFrame::new(false),
    ))
}

fn list_len(column: &str, list_type: &DataType) -> Result<DFExpr> {
    let len_udf = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1);

        let result = as_list_array(&args[0])?
            .iter()
            .map(|list| list.map(|a| a.len() as u32).unwrap_or(0))
            .collect::<UInt32Array>();
        Ok(Arc::new(result) as ArrayRef)
    };

    let len_udf = make_scalar_function(len_udf);

    let len_udf = create_udf(
        "len",
        vec![list_type.clone()],
        Arc::new(DataType::UInt32),
        Volatility::Immutable,
        len_udf,
    );

    Ok(len_udf.call(vec![args::str_to_col(column)]))
}
