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
use anyhow::Result;
use datafusion::logical_expr;
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

/// Returns a datafusion column expression and quotes the name.
///
/// The `col` function in datafusion makes identifiers lower case, this function
/// quotes the name so that it preserves case.
pub fn str_to_col(s: impl Into<String>) -> logical_expr::Expr {
    logical_expr::col(format!(r#""{}""#, s.into()))
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
