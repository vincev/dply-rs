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

/// Evaluates a distinct call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let mut select_columns = Vec::new();

        for arg in args {
            let column = args::identifier(arg);
            if !ctx.columns().contains(&column) {
                bail!("distinct error: Unknown column {column}");
            }

            if !select_columns.contains(&column) {
                select_columns.push(column);
            }
        }

        let df = if !select_columns.is_empty() {
            let columns = select_columns.iter().map(|c| col(c)).collect::<Vec<_>>();
            df.select(&columns)
                .unique_stable(Some(select_columns), UniqueKeepStrategy::First)
        } else {
            df.unique_stable(None, UniqueKeepStrategy::First)
        };

        ctx.set_df(df)?;
    } else if ctx.is_grouping() {
        bail!("distinct error: must call summarize after a group_by");
    } else {
        bail!("distinct error: missing input dataframe");
    }

    Ok(())
}
