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
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a count call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_input() {
        let schema_cols = df
            .schema()
            .map_err(|e| anyhow!("Schema error: {e}"))?
            .iter_names()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        let mut columns = Vec::new();

        for arg in args {
            if let Expr::Identifier(column) = arg {
                if !schema_cols.contains(column) {
                    bail!("count error: Unknown column {column}");
                }

                let expr = col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        let agg_col = find_agg_column(&schema_cols);

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
            df.select(&[col(&schema_cols[0]).count().alias(&agg_col)])
        };

        ctx.set_input(df);
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
