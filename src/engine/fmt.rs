// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use comfy_table::presets;
use comfy_table::{ColumnConstraint, ContentArrangement, Width};
use comfy_table::{Row, Table};
use polars::prelude::*;
use std::{env, io::Write};

/// Prints a dataframe in test format, used for test comparisons.
pub fn df_test(out: &mut dyn Write, df: DataFrame) -> Result<()> {
    env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "6");

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
    let num_rows = df
        .clone()
        .count()
        .collect()?
        .max_horizontal()?
        .unwrap_or_default()
        .max::<usize>()?
        .unwrap_or_default();

    let df = df.fetch(100)?;
    let num_cols = df.get_columns().len();

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::DynamicFullWidth);
    table.load_preset(presets::UTF8_FULL_CONDENSED);

    let info = format!(
        "Rows: {}\nCols: {}",
        fmt_usize(num_rows),
        fmt_usize(num_cols)
    );
    table.set_header(vec![info, "Type".into(), "Values".into()]);

    if let Ok(slen) = std::env::var("POLARS_FMT_STR_LEN") {
        table.set_width(slen.parse()?);
    }

    for col in df.get_columns() {
        let mut row = Row::new();
        row.add_cell(col.name().into());
        row.add_cell(format!("{}", col.dtype()).into());

        let mut values = Vec::with_capacity(10);
        for idx in 0..col.len() {
            let value = col.str_value(idx).unwrap_or_default();
            values.push(value.into_owned());
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

fn fmt_usize(n: usize) -> String {
    // Colon separated groups of 3.
    let mut s = n.to_string();

    for idx in (1..s.len().max(2) - 2).rev().step_by(3) {
        s.insert(idx, ',');
    }

    s
}
