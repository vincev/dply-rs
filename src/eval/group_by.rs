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

/// Evaluates a group_by call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
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
                    bail!("group_by error: Unknown column {column}");
                }

                let expr = col(column);
                if !columns.contains(&expr) {
                    columns.push(expr);
                }
            }
        }

        ctx.set_group(df.groupby(&columns));
    } else {
        bail!("group_by error: missing input dataframe");
    }

    Ok(())
}
