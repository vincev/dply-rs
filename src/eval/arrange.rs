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

/// Evaluates an arrange call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_input() {
        // arrange(year, desc(day))
        let mut columns = Vec::with_capacity(args.len());
        let mut descending = Vec::with_capacity(args.len());

        for arg in args {
            match arg {
                Expr::Function(name, args) if name == "desc" => {
                    // arrange(desc(column))
                    let column = args::identifier(&args[0]);
                    columns.push(col(&column));
                    descending.push(true);
                }
                Expr::Identifier(column) => {
                    // arrange(column)
                    columns.push(col(column));
                    descending.push(false);
                }
                _ => {}
            }
        }

        ctx.set_input(df.sort_by_exprs(columns, descending, true));
    } else {
        bail!("Missing input dataframe for arrange.");
    }

    Ok(())
}
