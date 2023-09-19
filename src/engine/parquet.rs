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
use anyhow::{anyhow, bail, Result};
use datafusion::{
    common::DEFAULT_PARQUET_EXTENSION,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        provider_as_source,
    },
    logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE},
    parquet::{
        arrow::arrow_writer::ArrowWriter,
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
};
use std::{num::NonZeroUsize, path::Path, sync::Arc};

use crate::parser::Expr;

use super::*;

/// Evaluates a parquet call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    let path = args::string(&args[0]);
    let overwrite = args::named_bool(args, "overwrite");

    // If there is an input dataframe save it to disk.
    if let Some(plan) = ctx.take_plan() {
        if !overwrite && Path::new(&path).exists() {
            bail!("parquet error: file '{}' already exists.", path);
        }

        ctx.set_plan(plan.clone());

        let (schema, mut rx) = io::execute_plan(plan, ctx)?;

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("parquet error: cannot create file '{}' {e}", path))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

        while let Some(batch) = rx.blocking_recv() {
            writer.write(&batch?)?;
        }

        writer.close()?;
    } else {
        // Read the data frame and set it as input for the next task.
        let table_path = ListingTableUrl::parse(&path)?;

        let num_cpus = std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .get();

        // Use default extension for recursive loading.
        let extension = if Path::new(&path).is_dir() {
            DEFAULT_PARQUET_EXTENSION
        } else {
            ""
        };

        let file_format = ParquetFormat::new();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(extension)
            .with_target_partitions(num_cpus);

        let resolved_schema =
            ctx.block_on(listing_options.infer_schema(&ctx.session().state(), &table_path))?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let table_provider = ListingTable::try_new(config)?;
        let table_source = provider_as_source(Arc::new(table_provider));
        let plan = LogicalPlanBuilder::scan(UNNAMED_TABLE, table_source, None)?.build()?;

        ctx.set_plan(plan);
    }

    Ok(())
}
