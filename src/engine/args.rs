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
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::{common::DFSchema, logical_expr::Expr as DFExpr, prelude::*};
use std::str::FromStr;

use crate::parser::{Expr, Operator};

/// Returns the string from a string expression.
///
/// Panics if the expression is not a string.
pub fn string(expr: &Expr) -> String {
    match expr {
        Expr::String(s) => s.to_owned(),
        _ => panic!("{expr} is not a string expression"),
    }
}

/// Returns the value from a number expression.
///
/// Panics if the expression is not a number.
pub fn number(expr: &Expr) -> f64 {
    match expr {
        Expr::Number(s) => *s,
        _ => panic!("{expr} is not a number expression"),
    }
}

/// Returns the string from an identifier expression.
///
/// Panics if the expression is not an identifier.
pub fn identifier(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(s) => s.to_owned(),
        _ => panic!("{expr} is not an identifier expression"),
    }
}

/// Returns a datafusion column expression and quotes the name.
///
/// The `col` function in datafusion makes identifiers lower case, this function
/// quotes the name so that it preserves case.
pub fn str_to_col(s: impl Into<String>) -> DFExpr {
    DFExpr::Column(Column::new_unqualified(s))
}

/// Returns a datafusion column if it is in the schema.
pub fn expr_to_col(expr: &Expr, schema: &DFSchema) -> Result<DFExpr> {
    let column = identifier(expr);
    if schema.has_column_with_unqualified_name(&column) {
        Ok(str_to_col(column))
    } else {
        Err(anyhow!("Unknown column '{expr}'"))
    }
}

/// Returns a datafusion qualified column if it is in the schema.
///
/// This is needed for when window function expressions are transformed to a
/// column expression as their name needs the table.
pub fn expr_to_qualified_col(expr: &Expr, schema: &DFSchema) -> Result<DFExpr> {
    let column = identifier(expr);

    if let Ok(field) = schema.field_with_unqualified_name(&column) {
        let qualifier = field.qualifier().cloned();
        Ok(DFExpr::Column(Column::new(qualifier, field.name())))
    } else {
        Err(anyhow!("Unknown column '{expr}'"))
    }
}

/// Returns a date time from a string.
///
/// Returns an error if the string is not a valid date time.
pub fn timestamp(expr: &Expr) -> Result<DFExpr> {
    let ts = string(expr);
    let ts = ts.trim();

    let dt = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(ts, "%Y-%m-%d")
                .map(|d| NaiveDateTime::new(d, Default::default()))
        })
        .map_err(|e| anyhow!("Invalid timestamp string {ts}: {e}"))?;

    Ok(lit_timestamp_nano(dt.timestamp_nanos()))
}

/// Returns the value of a named boolean variable like `overwrite = false`.
pub fn named_bool(args: &[Expr], name: &str) -> bool {
    for arg in args {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Identifier(lhs), Expr::Identifier(rhs)) if lhs == name => {
                    return bool::from_str(rhs).unwrap_or(false);
                }
                _ => {}
            }
        }
    }

    false
}

/// Returns the value of a named integer variable like `schema_rows = 2000`.
pub fn named_int(args: &[Expr], name: &str) -> Option<i64> {
    for arg in args {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Identifier(lhs), Expr::Number(rhs)) if lhs == name => {
                    return Some(*rhs as i64);
                }
                _ => {}
            }
        }
    }

    None
}
