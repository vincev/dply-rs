// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, bail, Result};
use datafusion::{
    arrow::{array::ArrayRef, compute::kernels, datatypes::*},
    common::{
        tree_node::{Transformed, TreeNode},
        DFSchema,
    },
    logical_expr::{
        aggregate_function::AggregateFunction, cast, create_udf, expr, expr_fn, lit, utils,
        window_frame::WindowFrame, BuiltInWindowFunction, Expr as DFExpr, LogicalPlanBuilder,
        Volatility, WindowFunction,
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

        ctx.set_plan(plan);
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
                Operator::Mod => lhs % expr_fn::cast(rhs, DataType::UInt64),
                _ => panic!("Unexpected mutate operator {op}"),
            };

            Ok(result)
        }
        Expr::Identifier(_) => args::expr_to_col(expr, plan.schema()),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "ymd_hms" => {
            args::expr_to_col(&args[0], schema).map(expr_fn::to_timestamp_millis)
        }
        Expr::Function(name, args) if name == "dnanos" => {
            args::expr_to_col(&args[0], schema).map(|e| to_duration(e, TimeUnit::Nanosecond))
        }
        Expr::Function(name, args) if name == "dmicros" => {
            args::expr_to_col(&args[0], schema).map(|e| to_duration(e, TimeUnit::Microsecond))
        }
        Expr::Function(name, args) if name == "dmillis" => {
            args::expr_to_col(&args[0], schema).map(|e| to_duration(e, TimeUnit::Millisecond))
        }
        Expr::Function(name, args) if name == "dsecs" => {
            args::expr_to_col(&args[0], schema).map(|e| to_duration(e, TimeUnit::Second))
        }
        Expr::Function(name, args) if name == "nanos" => {
            duration_to_i64(&args[0], schema, TimeUnit::Nanosecond)
        }
        Expr::Function(name, args) if name == "micros" => {
            duration_to_i64(&args[0], schema, TimeUnit::Microsecond)
        }
        Expr::Function(name, args) if name == "millis" => {
            duration_to_i64(&args[0], schema, TimeUnit::Millisecond)
        }
        Expr::Function(name, args) if name == "secs" => {
            duration_to_i64(&args[0], schema, TimeUnit::Second)
        }
        Expr::Function(name, args) if name == "field" => {
            let field_name = args::identifier(&args[1]);
            args::expr_to_qualified_col(&args[0], schema).map(|e| e.field(field_name))
        }
        Expr::Function(name, args) if name == "len" => {
            let column = args::identifier(&args[0]);
            match schema
                .field_with_unqualified_name(&column)
                .map(|f| f.data_type())
            {
                Ok(dt @ DataType::List(_) | dt @ DataType::Utf8) => list_len(&column, dt),
                Ok(_) => Err(anyhow!("`len` column '{column}' must be a list or string")),
                Err(_) => Err(anyhow!("Unknown column '{column}'")),
            }
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
        Expr::Function(name, _args) if name == "row" => Ok(row_fn()),
        _ => panic!("Unexpected mutate expression {expr}"),
    }
}

fn to_duration(expr: DFExpr, time_unit: TimeUnit) -> DFExpr {
    let i64_expr = cast(expr, DataType::Int64);
    cast(i64_expr, DataType::Duration(time_unit))
}

fn duration_to_i64(expr: &Expr, schema: &DFSchema, to_unit: TimeUnit) -> Result<DFExpr> {
    let col_expr = args::expr_to_col(expr, schema)?;
    let col_name = args::identifier(expr);

    let data_type = schema
        .field_with_unqualified_name(&col_name)
        .map(|f| f.data_type())
        .map_err(|_| anyhow!("Unknown column {col_name}"))?;

    if let DataType::Duration(duration_unit) = data_type {
        let units = cast(col_expr, DataType::Int64);
        let result = match (duration_unit, to_unit) {
            (TimeUnit::Second, TimeUnit::Second)
            | (TimeUnit::Millisecond, TimeUnit::Millisecond)
            | (TimeUnit::Microsecond, TimeUnit::Microsecond)
            | (TimeUnit::Nanosecond, TimeUnit::Nanosecond) => units,

            (TimeUnit::Second, TimeUnit::Millisecond)
            | (TimeUnit::Microsecond, TimeUnit::Nanosecond)
            | (TimeUnit::Millisecond, TimeUnit::Microsecond) => units * lit(1_000),

            (TimeUnit::Second, TimeUnit::Microsecond)
            | (TimeUnit::Millisecond, TimeUnit::Nanosecond) => units * lit(1_000_000),

            (TimeUnit::Second, TimeUnit::Nanosecond) => units * lit(1_000_000_000),

            (TimeUnit::Millisecond, TimeUnit::Second)
            | (TimeUnit::Microsecond, TimeUnit::Millisecond)
            | (TimeUnit::Nanosecond, TimeUnit::Microsecond) => units / lit(1_000.0),

            (TimeUnit::Microsecond, TimeUnit::Second)
            | (TimeUnit::Nanosecond, TimeUnit::Millisecond) => units / lit(1_000_000.0),
            (TimeUnit::Nanosecond, TimeUnit::Second) => units / lit(1_000_000_000.0),
        };

        Ok(cast(result, DataType::Int64))
    } else {
        Err(anyhow!("Column '{col_name}' must be a duration type"))
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

fn row_fn() -> DFExpr {
    DFExpr::WindowFunction(expr::WindowFunction::new(
        WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
        vec![],
        vec![],
        vec![],
        WindowFrame::new(false),
    ))
}

fn list_len(column: &str, list_type: &DataType) -> Result<DFExpr> {
    let len_udf = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1);
        let result = kernels::length::length(&args[0])?;
        Ok(result)
    };

    let len_udf = make_scalar_function(len_udf);

    let len_udf = create_udf(
        "len",
        vec![list_type.clone()],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        len_udf,
    );

    Ok(len_udf.call(vec![args::str_to_col(column)]))
}
