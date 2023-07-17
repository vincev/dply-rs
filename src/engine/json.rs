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
    arrow::{json, record_batch::RecordBatch},
    datasource::{
        file_format::json::{JsonFormat, DEFAULT_JSON_EXTENSION},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        provider_as_source,
    },
    execution::context::TaskContext,
    logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE},
};
use futures::StreamExt;
use std::{num::NonZeroUsize, path::Path, sync::Arc};
use tokio::sync::mpsc;

use crate::parser::Expr;

use super::*;

/// Evaluates a json call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    let path = args::string(&args[0]);
    let overwrite = args::named_bool(args, "overwrite");

    // If there is an input dataframe save it to disk.
    if let Some(plan) = ctx.take_plan() {
        if !overwrite && Path::new(&path).exists() {
            bail!("json error: file '{}' already exists.", path);
        }

        ctx.set_plan(plan.clone());

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("json error: cannot create file '{}' {e}", path))?;

        ctx.block_on(async {
            // Persist all partitions to a single json file.
            let plan = ctx.create_physical_plan(&plan).await?;

            let mut writer = json::LineDelimitedWriter::new(file);
            let task_context = Arc::new(TaskContext::from(ctx.session()));
            let num_partitions = plan.output_partitioning().partition_count();
            let (tx, mut rx) = mpsc::channel::<Result<RecordBatch>>(num_partitions * 16);

            for partition in 0..plan.output_partitioning().partition_count() {
                tokio::task::spawn({
                    let plan = plan.clone();
                    let sender = tx.clone();
                    let task_context = task_context.clone();
                    async move {
                        match plan.execute(partition, task_context) {
                            Ok(mut s) => {
                                while let Some(batch) = s.next().await {
                                    sender
                                        .send(batch.map_err(anyhow::Error::from))
                                        .await
                                        .unwrap();
                                }
                            }
                            Err(e) => sender
                                .send(Err(anyhow!("Json write error: {e}")))
                                .await
                                .unwrap(),
                        }
                        Ok::<_, anyhow::Error>(())
                    }
                });
            }

            drop(tx);

            while let Some(batch) = rx.recv().await {
                writer.write(&batch?)?;
            }

            Ok::<_, anyhow::Error>(())
        })?;
    } else {
        // Read the data frame and set it as input for the next task.
        let table_path = ListingTableUrl::parse(path)?;

        let num_cpus = std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .get();

        let schema_infer_rows = args::named_int(args, "schema_rows")
            .filter(|n| *n > 0)
            .map(|n| n as usize);

        let file_format = JsonFormat::default().with_schema_infer_max_rec(schema_infer_rows);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(DEFAULT_JSON_EXTENSION)
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