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
    if let Some(mut df) = ctx.take_df() {
        for arg in args {
            df = df.filter(eval_expr(arg)?);
        }

        ctx.set_df(df);
    } else if ctx.is_grouping() {
        bail!("filter error: must call summarize after a group_by");
    } else {
        bail!("filter error: missing input dataframe");
    }

    Ok(())
}

fn eval_expr(expr: &Expr) -> Result<PolarsExpr> {
    match expr {
        Expr::BinaryOp(lhs, op, rhs) => {
            let lhs = eval_expr(lhs)?;
            let rhs = eval_expr(rhs)?;

            let result = match op {
                Operator::Eq => lhs.eq(rhs),
                Operator::NotEq => lhs.neq(rhs),
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
        Expr::Identifier(s) => Ok(col(s)),
        Expr::String(s) => Ok(lit(s.clone())),
        Expr::Number(n) => Ok(lit(*n)),
        Expr::Function(name, args) if name == "dt" => {
            let ts = args::timestamp(&args[0])?;
            Ok(lit(ts))
        }
        _ => panic!("Unexpected filter expression {expr}"),
    }
}
