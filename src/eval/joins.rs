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
use std::collections::HashSet;

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a join call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context, join_type: JoinType) -> Result<()> {
    if let Some(lhs_df) = ctx.take_df() {
        let rhs_df_name = args::identifier(&args[0]);
        if let Some(rhs_df) = ctx.get_df(&rhs_df_name) {
            let lhs_schema = lhs_df.schema().map_err(|e| anyhow!("join error: {e}"))?;
            let rhs_schema = rhs_df.schema().map_err(|e| anyhow!("join error: {e}"))?;

            let lhs_schema_cols = lhs_schema
                .iter_names()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>();

            let rhs_schema_cols = rhs_schema
                .iter_names()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>();

            // If no join columns are specified use common columns
            let (lhs_cols, rhs_cols) = if args.len() == 1 {
                let common_cols = lhs_schema_cols
                    .intersection(&rhs_schema_cols)
                    .map(|s| col(s))
                    .collect::<Vec<_>>();
                if common_cols.is_empty() {
                    bail!("join error: Missing join columns for '{rhs_df_name}'");
                }

                (common_cols.clone(), common_cols)
            } else {
                let mut lhs_cols = Vec::with_capacity(args.len());
                let mut rhs_cols = Vec::with_capacity(args.len());

                for arg in args.iter().skip(1) {
                    if let Expr::BinaryOp(lhs, Operator::Eq, rhs) = arg {
                        let lhs_col = args::identifier(lhs);
                        if !lhs_schema_cols.contains(&lhs_col) {
                            bail!("join error: Unknown column '{lhs_col}'");
                        }
                        lhs_cols.push(col(&lhs_col));

                        let rhs_col = args::identifier(rhs);
                        if !rhs_schema_cols.contains(&rhs_col) {
                            bail!("join error: Unknown column '{rhs_col}'");
                        }
                        rhs_cols.push(col(&rhs_col));

                        let have_same_type = lhs_schema
                            .get(&lhs_col)
                            .zip(rhs_schema.get(&rhs_col))
                            .map(|(ldt, rdt)| ldt == rdt)
                            .unwrap_or(false);
                        if !have_same_type {
                            bail!(
                                "join error: '{lhs_col}' and '{rhs_col}' don't have the same type"
                            );
                        }
                    }
                }

                (lhs_cols, rhs_cols)
            };

            ctx.set_df(lhs_df.join(rhs_df.clone(), lhs_cols, rhs_cols, join_type))?;
        } else {
            bail!("join error: undefined dataframe variable '{rhs_df_name}'");
        }
    } else if ctx.is_grouping() {
        bail!("join error: must call summarize after a group_by");
    } else {
        bail!("join error: missing input dataframe");
    }

    Ok(())
}
