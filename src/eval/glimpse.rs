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
use comfy_table::presets;
use comfy_table::{ColumnConstraint, ContentArrangement, Width};
use comfy_table::{Row, Table};
use polars::prelude::*;

use crate::parser::Expr;

use super::*;

/// Evaluates a glimpse call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(_args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(df) = ctx.take_input() {
        let count_df = df.clone().select([count()]).collect()?;
        let num_rows = count_df[0].max::<usize>().unwrap_or_default();
        ctx.print(|w| writeln!(w, "Rows: {num_rows}"))?;

        let df = df.fetch(100)?;
        let num_cols = df.get_columns().len();
        ctx.print(|w| writeln!(w, "Columns: {num_cols}"))?;

        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::DynamicFullWidth);
        table.load_preset(presets::ASCII_FULL_CONDENSED);

        if let Ok(slen) = std::env::var("POLARS_FMT_STR_LEN") {
            table.set_width(slen.parse()?);
        }

        for col in df.get_columns() {
            let mut row = Row::new();
            row.add_cell(col.name().into());
            row.add_cell(format!("{}", col.dtype()).into());

            let mut values = Vec::with_capacity(10);
            for value in col.iter() {
                values.push(format!("{}", value));
            }

            row.add_cell(values.join(", ").into());
            row.max_height(1);

            table.add_row(row);
        }

        table.set_constraints(vec![
            ColumnConstraint::LowerBoundary(Width::Fixed(10)),
            ColumnConstraint::LowerBoundary(Width::Fixed(8)),
            ColumnConstraint::UpperBoundary(Width::Percentage(90)),
        ]);

        ctx.print(|w| writeln!(w, "{table}"))?;
    } else {
        bail!("glimpse error: missing input dataframe");
    }

    Ok(())
}
