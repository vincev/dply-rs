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
use datafusion::logical_expr::LogicalPlanBuilder;

use crate::parser::Expr;

use super::*;

/// Evaluates an arrange call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema_cols = ctx.columns();
        let mut columns = Vec::with_capacity(args.len());

        for arg in args {
            match arg {
                Expr::Function(name, args) if name == "desc" => {
                    // arrange(desc(column))
                    let column = args::identifier(&args[0]);
                    if !schema_cols.contains(&column) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(args::str_to_col(&column).sort(false, false));
                }
                Expr::Identifier(column) => {
                    // arrange(column)
                    if !schema_cols.contains(column) {
                        bail!("arrange error: Unknown column {column}");
                    }

                    columns.push(args::str_to_col(column).sort(true, false));
                }
                _ => {}
            }
        }

        let plan = LogicalPlanBuilder::from(plan).sort(columns)?.build()?;
        ctx.set_plan(plan)?;
    } else if ctx.is_grouping() {
        bail!("arrange error: must call summarize after a group_by");
    } else {
        bail!("arrange error: missing input dataframe");
    }

    Ok(())
}
