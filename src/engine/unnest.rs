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

/// Evaluates an unnest call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut df) = ctx.take_df() {
        for arg in args {
            let column = args::identifier(arg);
            let schema = df
                .collect_schema()
                .map_err(|e| anyhow!("unnest error: {e}"))?;

            match schema.get(&column) {
                Some(DataType::List(_)) => {
                    df = df.explode(vec![col(column)]);
                }
                Some(DataType::Struct(_)) => {
                    df = df.unnest([column]);
                }
                Some(_) => bail!("unnest error: '{column}' is not a list or struct type"),
                None => bail!("unnest error: unknown column '{column}'"),
            }
        }

        ctx.set_df(df)?;
    } else {
        bail!("unnest error: missing input dataframe");
    }

    Ok(())
}
