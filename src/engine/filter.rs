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
        array::{ArrayRef, BooleanArray},
        datatypes::*,
    },
    common::cast::{as_list_array, as_primitive_array, as_string_array},
    common::DFSchema,
    logical_expr::{create_udf, lit, Expr as DFExpr, LogicalPlanBuilder, Volatility},
    physical_plan::functions::make_scalar_function,
};
use std::sync::Arc;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a filter call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut plan) = ctx.take_plan() {
        for arg in args {
            let expr = eval_expr(arg, plan.schema())?;
            plan = LogicalPlanBuilder::from(plan).filter(expr)?.build()?;
        }

        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("filter error: must call summarize after a group_by");
    } else {
        bail!("filter error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr, schema: &DFSchema) -> Result<DFExpr> {
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs, schema)?;
            let rhs = eval_expr(rhs, schema)?;

            let result = match op {
                Operator::Eq => lhs.eq(rhs),
                Operator::NotEq => lhs.not_eq(rhs),
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
        Expr::Identifier(_) => args::expr_to_col(expr, schema),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "dt" => Ok(args::timestamp(&args[0])?),
        Expr::UnaryOp(Operator::Not, expr) => {
            eval_predicate(expr, schema).map(|expr| DFExpr::Not(expr.into()))
        }
        Expr::Function(_, _) => eval_predicate(expr, schema),
        _ => panic!("Unexpected filter expression {expr}"),
    }
}

fn eval_predicate(expr: &Expr, schema: &DFSchema) -> Result<DFExpr> {
    match expr {
        Expr::Function(name, args) if name == "contains" => {
            let column = args::identifier(&args[0]);
            let column_type = schema
                .field_with_unqualified_name(&column)
                .map(|f| f.data_type())
                .map_err(|_| anyhow!("Unknown `contains` column '{column}'"))?;

            match column_type {
                lt @ DataType::List(_)
                | lt @ DataType::LargeList(_)
                | lt @ DataType::FixedSizeList(_, _) => list_contains(&column, &args[1], lt),
                DataType::Utf8 | DataType::LargeUtf8 => string_contains(&column, &args[1]),
                _ => Err(anyhow!("Column '{column}' must be a str or a list")),
            }
        }
        Expr::Function(name, args) if name == "is_null" => {
            args::expr_to_col(&args[0], schema).map(|c| c.is_null())
        }
        _ => panic!("Unexpected filter expression {expr}"),
    }
}

fn list_contains(column: &str, key: &Expr, list_type: &DataType) -> Result<DFExpr> {
    let elem_type = match list_type {
        DataType::List(elem) | DataType::LargeList(elem) | DataType::FixedSizeList(elem, _) => {
            elem.data_type()
        }
        _ => bail!("Unsopperted list type"),
    };

    match (elem_type, key) {
        (DataType::Int8, Expr::Number(key)) => {
            list_contains_number::<Int8Type>(column, *key, list_type)
        }
        (DataType::Int16, Expr::Number(key)) => {
            list_contains_number::<Int16Type>(column, *key, list_type)
        }
        (DataType::Int32, Expr::Number(key)) => {
            list_contains_number::<Int32Type>(column, *key, list_type)
        }
        (DataType::Int64, Expr::Number(key)) => {
            list_contains_number::<Int64Type>(column, *key, list_type)
        }
        (DataType::UInt8, Expr::Number(key)) => {
            list_contains_number::<UInt8Type>(column, *key, list_type)
        }
        (DataType::UInt16, Expr::Number(key)) => {
            list_contains_number::<UInt16Type>(column, *key, list_type)
        }
        (DataType::UInt32, Expr::Number(key)) => {
            list_contains_number::<UInt32Type>(column, *key, list_type)
        }
        (DataType::UInt64, Expr::Number(key)) => {
            list_contains_number::<UInt64Type>(column, *key, list_type)
        }
        (DataType::Float16, Expr::Number(key)) => {
            list_contains_number::<Float16Type>(column, *key, list_type)
        }
        (DataType::Float32, Expr::Number(key)) => {
            list_contains_number::<Float32Type>(column, *key, list_type)
        }
        (DataType::Float64, Expr::Number(key)) => {
            list_contains_number::<Float64Type>(column, *key, list_type)
        }
        (DataType::Utf8, Expr::String(pattern)) | (DataType::LargeUtf8, Expr::String(pattern)) => {
            list_contains_utf8(column, pattern, list_type)
        }
        _ => bail!("contains error: invalid type {elem_type} for column '{column}'"),
    }
}

fn list_contains_number<T>(column: &str, key: f64, list_type: &DataType) -> Result<DFExpr>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::NumCast,
{
    let matcher_udf = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1);

        let key = num_traits::NumCast::from(key).unwrap_or_default();
        let result = as_list_array(&args[0])?
            .iter()
            .map(|list| match list {
                Some(array) => {
                    let numbers = as_primitive_array::<T>(&array).ok()?;
                    Some(
                        numbers
                            .iter()
                            .any(|v| v.map(|n| n.is_eq(key)).unwrap_or(false)),
                    )
                }
                None => Some(false),
            })
            .collect::<BooleanArray>();
        Ok(Arc::new(result) as ArrayRef)
    };

    let matcher_udf = make_scalar_function(matcher_udf);

    let matcher_udf = create_udf(
        "matcher",
        // Expects a list of utf8
        vec![list_type.clone()],
        // Returns boolean.
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        matcher_udf,
    );

    Ok(matcher_udf.call(vec![args::str_to_col(column)]))
}

fn list_contains_utf8(column: &str, pattern: &str, list_type: &DataType) -> Result<DFExpr> {
    let re = regex::Regex::new(pattern)
        .map_err(|_| anyhow!("invalid contains regex '{pattern}' for column '{column}'"))?;

    let matcher_udf = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1);

        let result = as_list_array(&args[0])?
            .iter()
            .map(|list| match list {
                Some(array) => {
                    let strings = as_string_array(&array).ok()?;
                    Some(
                        strings
                            .iter()
                            .any(|v| v.map(|s| re.is_match(s)).unwrap_or(false)),
                    )
                }
                None => Some(false),
            })
            .collect::<BooleanArray>();
        Ok(Arc::new(result) as ArrayRef)
    };

    let matcher_udf = make_scalar_function(matcher_udf);

    let matcher_udf = create_udf(
        "matcher",
        // Expects a list of utf8
        vec![list_type.clone()],
        // Returns boolean.
        Arc::new(DataType::Boolean),
        Volatility::Immutable,
        matcher_udf,
    );

    Ok(matcher_udf.call(vec![args::str_to_col(column)]))
}

fn string_contains(column: &str, pattern: &Expr) -> Result<DFExpr> {
    if let Expr::String(re) = pattern {
        let re = regex::Regex::new(re)
            .map_err(|_| anyhow!("invalid contains regex '{re}' for column '{column}'"))?;

        let matcher_udf = move |args: &[ArrayRef]| {
            // Mathes on only one string argument.
            assert_eq!(args.len(), 1);

            let result = as_string_array(&args[0])?
                .iter()
                .map(|v| v.map(|s| re.is_match(s)).or(Some(false)))
                .collect::<BooleanArray>();
            Ok(Arc::new(result) as ArrayRef)
        };

        let matcher_udf = make_scalar_function(matcher_udf);

        let matcher_udf = create_udf(
            "matcher",
            // Expects an array of strings.
            vec![DataType::Utf8],
            // Returns boolean.
            Arc::new(DataType::Boolean),
            Volatility::Immutable,
            matcher_udf,
        );

        Ok(matcher_udf.call(vec![args::str_to_col(column)]))
    } else {
        Err(anyhow!(
            "contains predicate for column '{column}' must be a regex"
        ))
    }
}
