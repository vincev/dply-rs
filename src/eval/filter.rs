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

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a filter call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut df) = ctx.take_input() {
        for arg in args {
            df = df.filter(eval_expr(arg));
        }

        ctx.set_input(df);
    } else {
        bail!("Missing input dataframe for filter.");
    }

    Ok(())
}

fn eval_expr(expr: &Expr) -> PolarsExpr {
    match expr {
        Expr::BinaryOp(lhs, Operator::Eq, rhs) => eval_expr(lhs).eq(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::NotEq, rhs) => eval_expr(lhs).neq(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::Lt, rhs) => eval_expr(lhs).lt(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::LtEq, rhs) => eval_expr(lhs).lt_eq(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::Gt, rhs) => eval_expr(lhs).gt(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::GtEq, rhs) => eval_expr(lhs).gt_eq(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::And, rhs) => eval_expr(lhs).and(eval_expr(rhs)),
        Expr::BinaryOp(lhs, Operator::Or, rhs) => eval_expr(lhs).or(eval_expr(rhs)),
        Expr::Identifier(s) => col(s),
        Expr::String(s) => lit(s.clone()),
        Expr::Number(n) => lit(*n),
        _ => panic!("Unexpected filter expression {expr}"),
    }
}
