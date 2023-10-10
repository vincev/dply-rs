// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, Result};
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    execution::context::TaskContext,
    logical_expr::LogicalPlan,
};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::*;

pub fn execute_plan(
    plan: LogicalPlan,
    ctx: &mut Context,
) -> Result<(SchemaRef, mpsc::Receiver<Result<RecordBatch>>)> {
    ctx.block_on(async {
        let plan = ctx.create_physical_plan(&plan).await?;
        let task_context = Arc::new(TaskContext::from(ctx.session()));

        let num_partitions = plan.output_partitioning().partition_count();
        let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(num_partitions * 16);

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
                            .send(Err(anyhow!("Plan exec error: {e}")))
                            .await
                            .unwrap(),
                    }
                }
            });
        }

        Ok::<_, anyhow::Error>((plan.schema(), rx))
    })
    .map_err(anyhow::Error::from)
}
