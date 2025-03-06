// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, bail, Result};
use chrono::prelude::*;
use polars::lazy::dsl::Expr as PolarsExpr;
use polars::prelude::*;
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

/// Returns the string from an identifier expression.
///
/// Panics if the expression is not an identifier.
pub fn identifier(expr: &Expr) -> PlSmallStr {
    match expr {
        Expr::Identifier(s) => PlSmallStr::from_str(s),
        _ => panic!("{expr} is not an identifier expression"),
    }
}

/// Returns a Polars column if it is in the schema.
pub fn column(expr: &Expr, schema: &Schema) -> Result<PolarsExpr> {
    let column = identifier(expr);
    schema
        .get(&column)
        .map(|_| col(column))
        .ok_or_else(|| anyhow!("Unknown column '{expr}'"))
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

/// Returns a date time from a string.
///
/// Returns an error if the string is not a valid date time.
pub fn timestamp(expr: &Expr) -> Result<NaiveDateTime> {
    let ts = string(expr);
    let ts = ts.trim();

    let dt = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(ts, "%Y-%m-%d")
                .map(|d| NaiveDateTime::new(d, Default::default()))
        })
        .map_err(|e| anyhow!("Invalid timestamp string {ts}: {e}"))?;

    Ok(dt)
}

pub fn named_bool(args: &[Expr], name: &str) -> Result<bool> {
    for arg in args {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Identifier(lhs), Expr::Identifier(rhs)) if lhs == name => {
                    return Ok(bool::from_str(rhs)?);
                }
                _ => {}
            }
        }
    }

    Ok(false)
}

/// Returns the value of a named integer variable like `schema_rows = 2000`.
pub fn named_usize(args: &[Expr], name: &str) -> Result<Option<usize>> {
    for arg in args {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Identifier(lhs), Expr::Number(value)) if lhs == name => {
                    if *value >= 0.0 {
                        return Ok(Some(*value as usize));
                    } else {
                        bail!("{name} must have positive value");
                    }
                }
                _ => {}
            }
        }
    }

    Ok(None)
}
