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
use anyhow::Result;
use comfy_table::presets;
use comfy_table::{ColumnConstraint, ContentArrangement, Width};
use comfy_table::{Row, Table};
use polars::prelude::*;
use std::io::Write;

/// Prints a dataframe in test format, used for test comparisons.
pub fn df_test(out: &mut dyn Write, df: DataFrame) -> Result<()> {
    let height = df.height();

    let (row, cols) = df.shape();
    writeln!(out, "shape: ({}, {})", row, cols)?;

    // Write columns
    let row = df
        .fields()
        .into_iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>()
        .join("|");
    writeln!(out, "{row}")?;

    // Write columns types
    let row = df
        .fields()
        .into_iter()
        .map(|f| f.data_type().to_string())
        .collect::<Vec<_>>()
        .join("|");
    writeln!(out, "{row}")?;

    // Header separator
    writeln!(out, "---")?;

    // Write values
    for i in 0..height {
        let row = df
            .get_columns()
            .iter()
            .map(|s| s.str_value(i).unwrap())
            .collect::<Vec<_>>()
            .join("|");
        writeln!(out, "{row}")?;
    }

    // Data separator
    writeln!(out, "---")?;

    Ok(())
}

/// Prints a dataframe in glimpse format.
pub fn glimpse(w: &mut dyn Write, df: LazyFrame) -> Result<()> {
    let count_df = df.clone().select([count()]).collect()?;
    let num_rows = count_df[0].max::<usize>().unwrap_or_default();
    writeln!(w, "Rows: {num_rows}")?;

    let df = df.fetch(100)?;
    let num_cols = df.get_columns().len();
    writeln!(w, "Columns: {num_cols}")?;

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

    writeln!(w, "{table}")?;
    Ok(())
}
