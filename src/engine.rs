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

//! Evaluate pipeline functions.
use anyhow::{bail, Result};
use datafusion::{
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
    prelude::{Expr as DFExpr, *},
};
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::runtime;

use crate::completions::Completions;
use crate::parser::Expr;

mod args;
mod parquet;

pub struct Context {
    /// Named data frames.
    vars: HashMap<String, LogicalPlan>,
    /// Input dataframe passed from one pipeline step to the next.
    df: Option<LogicalPlan>,
    /// Columns passed to aggregate functions.
    group: Option<Vec<DFExpr>>,
    /// Dataframe columns.
    columns: Vec<String>,
    /// Optional output used for testing.
    output: Option<Vec<u8>>,
    /// Completions lru
    completions: Completions,
    /// Tokio runtime to run async tasks.
    runtime: runtime::Runtime,
    /// Datafusion context
    session: SessionContext,
}

impl Default for Context {
    fn default() -> Self {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        Self {
            vars: Default::default(),
            df: Default::default(),
            group: Default::default(),
            columns: Default::default(),
            output: Default::default(),
            completions: Default::default(),
            runtime,
            session: Default::default(),
        }
    }
}

impl Context {
    /// Returns the recently used column completions.
    pub fn completions(&self) -> impl Iterator<Item = String> + '_ {
        self.completions.iter().map(|s| s.to_string())
    }

    /// Returns the active dataframe variables.
    pub fn vars(&self) -> Vec<String> {
        self.vars.keys().cloned().collect()
    }

    /// Returns datafusion context
    fn session(&self) -> &SessionContext {
        &self.session
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .session
            .state()
            .create_physical_plan(logical_plan)
            .await?;
        Ok(plan)
    }

    /// Returns datafusion context
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Clear the context removing the active group and dataframe.
    fn clear(&mut self) {
        self.df = None;
        self.group = None;
    }

    /// Returns and consume the input dataframe.
    fn take_plan(&mut self) -> Option<LogicalPlan> {
        self.df.take()
    }

    /// Sets the dataframe to be used in pipeline steps.
    fn set_plan(&mut self, df: LogicalPlan) -> Result<()> {
        assert!(self.group.is_none());

        // Get unqualified column names.
        self.columns = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        self.update_completions();

        self.df = Some(df);
        Ok(())
    }

    /// Gets the active group.
    fn is_grouping(&mut self) -> bool {
        self.group.is_some()
    }

    fn update_completions(&mut self) {
        self.completions.add(&self.columns);
    }
}

/// Evaluate pipelines expressions to standard output.
pub fn eval(ctx: &mut Context, exprs: &[Expr]) -> Result<()> {
    // Let the interpreters handle the number of rows in the output.
    eval_pipelines(exprs, ctx)
}

/// Evaluate pipelines expressions to a string output, used for testing.
pub fn eval_to_string(exprs: &[Expr]) -> Result<String> {
    let mut ctx = Context {
        output: Some(Default::default()),
        ..Default::default()
    };

    eval_pipelines(exprs, &mut ctx)?;

    Ok(String::from_utf8(ctx.output.unwrap())?)
}

fn eval_pipelines(exprs: &[Expr], ctx: &mut Context) -> Result<()> {
    for expr in exprs {
        if let Expr::Pipeline(exprs) = expr {
            ctx.clear();

            for expr in exprs {
                eval_pipeline_step(expr, ctx)?;
            }
        }
    }

    Ok(())
}

fn eval_pipeline_step(expr: &Expr, ctx: &mut Context) -> Result<()> {
    match expr {
        Expr::Function(name, args) => match name.as_str() {
            "parquet" => parquet::eval(args, ctx)?,
            _ => panic!("Unknown function {name}"),
        },
        Expr::Identifier(name) => {
            // If there is an input assign it to the variable.
            if let Some(df) = ctx.take_plan() {
                ctx.vars.insert(name.to_owned(), df.clone());
                ctx.set_plan(df)?;
            } else if let Some(df) = ctx.vars.get(name) {
                ctx.set_plan(df.clone())?;
            } else if ctx.is_grouping() {
                bail!("Cannot assign a group to variable '{name}'");
            } else {
                bail!("Undefined variable '{name}'");
            }
        }
        _ => panic!("Unexpected expression {expr}"),
    }

    Ok(())
}