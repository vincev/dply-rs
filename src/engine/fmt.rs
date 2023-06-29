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
use comfy_table::presets::*;
use comfy_table::*;
use datafusion::{
    arrow::datatypes::{DataType, IntervalUnit, TimeUnit},
    arrow::{
        array::Int64Array,
        record_batch::RecordBatch,
        util::display::{ArrayFormatter, FormatOptions},
    },
    execution::context::TaskContext,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
};
use futures::TryStreamExt;
use std::{io::Write, sync::Arc};

use super::{count, Context};

/// Prints the plan results.
pub async fn show(ctx: &Context, plan: LogicalPlan) -> Result<()> {
    // Get column types before consuming the dataframe so that we can show them
    // even if the dataframe is empty.
    let format_config = ctx.format_config();
    let num_cols = plan.schema().fields().len();
    let truncate_cols = format_config.max_columns < num_cols;

    let mut fields = plan
        .schema()
        .fields()
        .iter()
        .take(format_config.max_columns)
        .map(|f| format!("{}\n---\n{}", f.name(), fmt_data_type(f.data_type())))
        .collect::<Vec<_>>();

    if truncate_cols {
        fields.push("...".to_string());
    }

    let constraints = fields
        .iter()
        .map(|f| {
            let w = f.len().clamp(5, 16);
            ColumnConstraint::LowerBoundary(Width::Fixed(w as u16))
        })
        .collect::<Vec<_>>();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(fields);
    table.set_constraints(constraints);

    let fmt_opts = fmt_opts();
    let mut num_rows = 0;

    for_each_batch(ctx, plan, |batch| {
        num_rows += batch.num_rows();
        let formatters = batch
            .columns()
            .iter()
            .take(format_config.max_columns)
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &fmt_opts))
            .collect::<Result<Vec<_>, _>>()?;

        for row in 0..batch.num_rows() {
            let mut cells = formatters
                .iter()
                .map(|f| {
                    Cell::new(fmt_value(
                        f.value(row).to_string(),
                        format_config.max_column_width,
                    ))
                })
                .collect::<Vec<_>>();

            if truncate_cols {
                cells.push(Cell::new("..."));
            }

            table.add_row(cells);
        }

        Ok(())
    })
    .await?;

    println!("shape: ({}, {})", fmt_usize(num_rows), fmt_usize(num_cols));
    println!("{}", table);

    Ok(())
}

/// Prints a dataframe in test format, used for test comparisons.
pub async fn test(ctx: &Context, plan: LogicalPlan, output: &mut dyn Write) -> Result<()> {
    // Get column types before consuming the dataframe so that we can show them
    // even if the dataframe is empty.
    let col_names = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<Vec<_>>();

    let col_types = plan
        .schema()
        .fields()
        .iter()
        .map(|f| fmt_data_type(f.data_type()))
        .collect::<Vec<_>>();

    let mut batches = Vec::new();
    for_each_batch(ctx, plan, |batch| {
        batches.push(batch);
        Ok(())
    })
    .await?;

    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    writeln!(output, "shape: ({}, {})", num_rows, col_names.len())?;

    // Write columns
    writeln!(output, "{}", col_names.join("|"))?;

    // Write columns types
    writeln!(output, "{}", col_types.join("|"))?;

    // Header separator
    writeln!(output, "---")?;

    // Write values
    let fmt_opts = fmt_opts();

    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &fmt_opts))
            .collect::<Result<Vec<_>, _>>()?;

        for row in 0..batch.num_rows() {
            let values = formatters
                .iter()
                .map(|f| fmt_value(f.value(row).to_string(), 1024))
                .collect::<Vec<_>>();
            writeln!(output, "{}", values.join("|"))?;
        }
    }

    // Data separator
    writeln!(output, "---")?;

    Ok(())
}

/// Prints a dataframe in glimpse format.
pub async fn glimpse(ctx: &Context, plan: LogicalPlan, output: &mut dyn Write) -> Result<()> {
    let mut num_rows = 0;
    let count_plan = count::count(plan.clone(), vec![], "n")?;
    for_each_batch(ctx, count_plan, |batch| {
        num_rows = *batch
            .columns()
            .first()
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(|a| a.values().first())
            .unwrap_or(&0) as usize;
        Ok(())
    })
    .await?;

    let num_cols = plan.schema().fields().len();

    let format_config = ctx.format_config();

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::DynamicFullWidth);
    table.load_preset(UTF8_FULL_CONDENSED);

    if let Some(cols) = format_config.max_table_width {
        table.set_width(cols);
    }

    let info = format!(
        "Rows: {}\nCols: {}",
        fmt_usize(num_rows),
        fmt_usize(num_cols)
    );
    table.set_header(vec![info, "Type".into(), "Values".into()]);

    const NUM_VALUES: usize = 100;

    let plan = LogicalPlanBuilder::from(plan)
        .limit(0, Some(NUM_VALUES))?
        .build()?;

    let fmt_opts = fmt_opts();

    for_each_batch(ctx, plan, |batch| {
        let columns = batch.columns().iter();

        for (fld, col) in batch.schema().fields().into_iter().zip(columns) {
            let mut row = Row::new();
            row.add_cell(fld.name().into());
            row.add_cell(fmt_data_type(fld.data_type()).into());

            let fmt = ArrayFormatter::try_new(col.as_ref(), &fmt_opts)?;
            let mut values = Vec::with_capacity(NUM_VALUES);

            for idx in 0..col.len() {
                values.push(fmt_value(
                    fmt.value(idx).to_string(),
                    format_config.max_column_width,
                ));
            }

            row.add_cell(values.join(", ").into());
            row.max_height(1);

            table.add_row(row);
        }

        Ok(())
    })
    .await?;

    table.set_constraints(vec![
        ColumnConstraint::LowerBoundary(Width::Fixed(10)),
        ColumnConstraint::LowerBoundary(Width::Fixed(8)),
        ColumnConstraint::UpperBoundary(Width::Percentage(90)),
    ]);

    writeln!(output, "{table}")?;
    Ok(())
}

/// Invoke function for each record batch generated by the plan.
///
/// This funcion consume the stream for each partition without bringing all the
/// results in memory.
async fn for_each_batch<F>(ctx: &Context, plan: LogicalPlan, mut f: F) -> Result<()>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    let plan = ctx.create_physical_plan(&plan).await?;

    let task_context = Arc::new(TaskContext::from(ctx.session()));
    for partition in 0..plan.output_partitioning().partition_count() {
        let mut stream = plan.execute(partition, task_context.clone())?;
        while let Some(batch) = stream.try_next().await? {
            f(batch)?;
        }
    }

    Ok(())
}

fn fmt_opts<'a>() -> FormatOptions<'a> {
    FormatOptions::default()
        .with_display_error(true)
        .with_datetime_format(Some("%Y-%m-%d %H:%M:%S"))
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S"))
        .with_null("null")
}

fn fmt_value(v: String, max_len: usize) -> String {
    if v.chars().count() <= max_len {
        v
    } else {
        let last_idx = v
            .char_indices()
            .take(max_len)
            .map(|(idx, _)| idx)
            .last()
            .unwrap_or(0);
        format!("{}...", &v[..last_idx])
    }
}

fn fmt_usize(n: usize) -> String {
    // Colon separated groups of 3.
    let mut s = n.to_string();

    for idx in (1..s.len().max(2) - 2).rev().step_by(3) {
        s.insert(idx, ',');
    }

    s
}

fn fmt_data_type(dt: &DataType) -> String {
    let s = match dt {
        DataType::Null => "null",
        DataType::Boolean => "bool",
        DataType::Int8 => "i8",
        DataType::Int16 => "i16",
        DataType::Int32 => "i32",
        DataType::Int64 => "i64",
        DataType::UInt8 => "u8",
        DataType::UInt16 => "u16",
        DataType::UInt32 => "u32",
        DataType::UInt64 => "u64",
        DataType::Float16 => "f16",
        DataType::Float32 => "f32",
        DataType::Float64 => "f64",
        DataType::Timestamp(tu, tz) => {
            return match tz {
                Some(tz) => format!("datetime[{}, {}]", fmt_time_unit(tu), tz),
                None => format!("datetime[{}]", fmt_time_unit(tu)),
            }
        }
        DataType::Date32 => "date32",
        DataType::Date64 => "date64",
        DataType::Time32(tu) | DataType::Time64(tu) => {
            return format!("time[{}]", fmt_time_unit(tu))
        }
        DataType::Duration(tu) => return format!("duration[{}]", fmt_time_unit(tu)),
        DataType::Interval(iu) => return format!("interval[{}]", fmt_interval_unit(iu)),
        DataType::Binary => "binary",
        DataType::FixedSizeBinary(_) => "binary",
        DataType::LargeBinary => "binary",
        DataType::Utf8 => "str",
        DataType::LargeUtf8 => "str",
        DataType::List(inner) | DataType::FixedSizeList(inner, _) | DataType::LargeList(inner) => {
            return format!("list[{}]", fmt_data_type(inner.data_type()));
        }
        DataType::Struct(fields) => return format!("struct[{}]", fields.len()),
        DataType::Union(fields, _) => return format!("union[{}]", fields.len()),
        DataType::Dictionary(k, v) => {
            return format!("dict[{}, {}]", fmt_data_type(k), fmt_data_type(v))
        }
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            return format!("decimal[.{p},{s}]")
        }
        DataType::Map(v, _) => return format!("map[{}]", fmt_data_type(v.data_type())),
        DataType::RunEndEncoded(rf, vf) => {
            return format!(
                "ree[{}, {}]",
                fmt_data_type(rf.data_type()),
                fmt_data_type(vf.data_type())
            )
        }
    };

    s.to_string()
}

fn fmt_time_unit(tu: &TimeUnit) -> &str {
    match tu {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "μs",
        TimeUnit::Nanosecond => "ns",
    }
}

fn fmt_interval_unit(iu: &IntervalUnit) -> &str {
    match iu {
        IntervalUnit::YearMonth => "ym",
        IntervalUnit::DayTime => "dt",
        IntervalUnit::MonthDayNano => "mdn",
    }
}
