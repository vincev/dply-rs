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
use datafusion::logical_expr::{Expr as DFExpr, LogicalPlanBuilder};

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a select call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut select_columns = Vec::new();

        for arg in args {
            match arg {
                Expr::Function(_, _) => {
                    let mut filter_cols = filter_columns(arg, schema_cols, false);
                    filter_cols.retain(|e| !select_columns.contains(e));
                    select_columns.extend(filter_cols);
                }
                Expr::UnaryOp(Operator::Not, expr) => {
                    let mut filter_cols = filter_columns(expr, schema_cols, true);
                    filter_cols.retain(|e| !select_columns.contains(e));
                    select_columns.extend(filter_cols);
                }
                Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                    // select(alias = column)
                    let alias = args::identifier(lhs);
                    let column = args::identifier(rhs);
                    let expr = args::str_to_col(&column).alias(&alias);

                    if !select_columns.contains(&expr) {
                        select_columns.push(expr);
                    }
                }
                Expr::Identifier(column) => {
                    // select(column)
                    if !schema_cols.contains(column) {
                        bail!("select error: Unknown column {column}");
                    }

                    let expr = args::str_to_col(column);
                    if !select_columns.contains(&expr) {
                        select_columns.push(expr);
                    }
                }
                _ => {}
            }
        }

        let plan = LogicalPlanBuilder::from(plan)
            .project(select_columns)?
            .build()?;
        ctx.set_plan(plan);
    } else if ctx.is_grouping() {
        bail!("select error: must call summarize after a group_by");
    } else {
        bail!("select error: missing input dataframe");
    }

    Ok(())
}

fn filter_columns(expr: &Expr, schema_cols: &[String], negate: bool) -> Vec<DFExpr> {
    match expr {
        Expr::Function(name, args) if name == "starts_with" => {
            // select(starts_with("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.starts_with(&pattern) ^ negate)
                .map(args::str_to_col)
                .collect()
        }
        Expr::Function(name, args) if name == "ends_with" => {
            // select(ends_with("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.ends_with(&pattern) ^ negate)
                .map(args::str_to_col)
                .collect()
        }
        Expr::Function(name, args) if name == "contains" => {
            // select(contains("pattern"))
            let pattern = args::string(&args[0]);
            schema_cols
                .iter()
                .filter(|c| c.contains(&pattern) ^ negate)
                .map(args::str_to_col)
                .collect()
        }
        _ => Vec::new(),
    }
}
