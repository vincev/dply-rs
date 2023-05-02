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

use crate::parser::Expr;

use super::*;

/// Evaluates a head call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_df() {
        let limit = if !args.is_empty() {
            args::number(&args[0]) as u32
        } else {
            10
        };

        let df = df.limit(limit).collect()?;
        ctx.print(|w| writeln!(w, "{df}"))?;
    } else if ctx.is_grouping() {
        bail!("head error: must call summarize after a group_by");
    } else {
        bail!("head error: missing input dataframe");
    }

    Ok(())
}
