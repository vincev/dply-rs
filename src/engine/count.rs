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
use datafusion::logical_expr::{self, Expr as DFExpr, LogicalPlan, LogicalPlanBuilder};

use crate::parser::Expr;

use super::*;

/// Evaluates a count call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !schema_cols.contains(column) {
                    bail!("count error: Unknown column {column}");
                }

                let expr = args::str_to_col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        let agg_col = find_agg_column(schema_cols.as_slice());

        let plan = if !columns.is_empty() {
            let plan = count(plan, columns.clone(), &agg_col)?;

            if args::named_bool(args, "sort")? {
                let mut sort_cols = vec![args::str_to_col(&agg_col).sort(false, false)];
                sort_cols.extend(columns.into_iter().map(|c| c.sort(true, false)));
                LogicalPlanBuilder::from(plan).sort(sort_cols)?.build()?
            } else {
                plan
            }
        } else {
            count(plan, vec![], &agg_col)?
        };

        ctx.set_plan(plan)?;
    } else if ctx.is_grouping() {
        bail!("count error: must call summarize after a group_by");
    } else {
        bail!("count error: missing input dataframe");
    }

    Ok(())
}

pub fn count(plan: LogicalPlan, group: Vec<DFExpr>, name: &str) -> Result<LogicalPlan> {
    let agg_col = logical_expr::count(logical_expr::lit(1u8)).alias(name);
    let plan = LogicalPlanBuilder::from(plan)
        .aggregate(group, vec![agg_col])?
        .build()?;
    Ok(plan)
}

/// If there is a column named `n` use `nn`, or `nnn`, etc.
fn find_agg_column(cols: &[String]) -> String {
    let mut col = "n".to_string();

    while cols.contains(&col) {
        col.push('n');
    }

    col
}
