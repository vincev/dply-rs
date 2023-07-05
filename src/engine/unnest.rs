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
use crate::parser::Expr;
use anyhow::{bail, Result};
use datafusion::{arrow::datatypes::*, logical_expr::LogicalPlanBuilder};

use super::*;

/// Evaluates an unnest call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(mut plan) = ctx.take_plan() {
        for arg in args {
            let column = args::identifier(arg);
            let schema = plan.schema();

            match schema
                .field_with_unqualified_name(&column)
                .map(|f| f.data_type())
            {
                Ok(DataType::List(_)) => {
                    plan = LogicalPlanBuilder::from(plan)
                        .unnest_column(column)?
                        .build()?;
                }
                // TODO: This needs changes to DataFusion to work, or a plan extension.
                // Some(DataType::Struct(_)) => {
                //     df = df.unnest([&column]);
                // }
                Ok(_) => bail!("unnest error: '{column}' is not a list or struct type"),
                Err(_) => bail!("unnest error: unknown column '{column}'"),
            }
        }

        ctx.set_plan(plan);
    } else {
        bail!("unnest error: missing input dataframe");
    }

    Ok(())
}
