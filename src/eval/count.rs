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
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a count call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !ctx.columns().contains(column) {
                    bail!("count error: Unknown column {column}");
                }

                let expr = col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        let agg_col = find_agg_column(ctx.columns());

        let df = if !columns.is_empty() {
            let ncol = columns.last().unwrap().clone();
            let df = df.groupby(&columns).agg([ncol.count().alias(&agg_col)]);

            let mut sort_mask = vec![false; columns.len()];

            if args::named_bool(args, "sort")? {
                columns.insert(0, col(&agg_col));
                sort_mask.insert(0, true);
            }

            df.sort_by_exprs(columns, sort_mask, false)
        } else {
            df.select(&[col(&ctx.columns()[0]).count().alias(&agg_col)])
        };

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("count error: must call summarize after a group_by");
    } else {
        bail!("count error: missing input dataframe");
    }

    Ok(())
}

/// If there is a column named `n` use `nn`, or `nnn`, etc.
fn find_agg_column(cols: &[String]) -> String {
    let mut col = "n".to_string();

    while cols.contains(&col) {
        col.push('n');
    }

    col
}
