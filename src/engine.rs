// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Evaluate pipeline functions.
use anyhow::{bail, Result};
use datafusion::{
    execution::context::SessionContext, logical_expr::LogicalPlan, physical_plan::ExecutionPlan,
    prelude::Expr as DFExpr,
};
use std::{collections::HashMap, future::Future, sync::Arc};
use tokio::runtime;

use crate::{completions::Completions, config::FormatConfig, parser::Expr};

mod args;
mod arrange;
mod config;
mod count;
mod csv;
mod distinct;
mod filter;
mod fmt;
mod glimpse;
mod group_by;
mod head;
mod io;
mod joins;
mod json;
mod mutate;
mod parquet;
mod relocate;
mod rename;
mod select;
mod show;
mod summarize;
mod unnest;

pub struct Context {
    /// Named logical plans.
    vars: HashMap<String, LogicalPlan>,
    /// Logical plan passed from one pipeline step to the next.
    plan: Option<LogicalPlan>,
    /// Columns passed to aggregate functions.
    group: Option<Vec<DFExpr>>,
    /// Dataframe columns.
    columns: Vec<String>,
    /// Optional output used for testing.
    output: Option<Vec<u8>>,
    /// Output format configuration
    format_config: FormatConfig,
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
            plan: Default::default(),
            group: Default::default(),
            columns: Default::default(),
            output: Default::default(),
            format_config: Default::default(),
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

    /// Returns the plan associated with the given variable.
    fn get_plan(&self, name: &str) -> Option<LogicalPlan> {
        self.vars.get(name).cloned()
    }

    /// Returns the active dataframe or group columns.
    fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    /// Returns datafusion context
    fn session(&self) -> &SessionContext {
        &self.session
    }

    /// Returns the current format configuration
    fn format_config(&self) -> &FormatConfig {
        &self.format_config
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
        self.plan = None;
        self.group = None;
    }

    /// Returns and consume the input dataframe.
    fn take_plan(&mut self) -> Option<LogicalPlan> {
        self.plan.take()
    }

    /// Sets the dataframe to be used in pipeline steps.
    fn set_plan(&mut self, plan: LogicalPlan) {
        assert!(self.group.is_none());

        // Get unqualified column names.
        self.columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

        self.update_completions();

        self.plan = Some(plan);
    }

    /// Sets the grouping columns used for aggregation.
    fn set_group(&mut self, plan: LogicalPlan, group: Vec<DFExpr>) {
        self.set_plan(plan);
        self.group = Some(group);
    }

    fn take_group(&mut self) -> Option<Vec<DFExpr>> {
        self.group.take()
    }

    /// Gets the active group.
    fn is_grouping(&mut self) -> bool {
        self.group.is_some()
    }

    fn update_completions(&mut self) {
        self.completions.add(&self.columns);
    }

    fn show(&mut self, plan: LogicalPlan) -> Result<()> {
        if let Some(mut output) = self.output.take() {
            self.runtime.block_on(fmt::test(self, plan, &mut output))?;
            self.output = Some(output);
            Ok(())
        } else {
            self.runtime.block_on(fmt::show(self, plan))
        }
    }

    fn glimpse(&mut self, plan: LogicalPlan) -> Result<()> {
        if let Some(mut output) = self.output.take() {
            self.runtime
                .block_on(fmt::glimpse(self, plan, &mut output))?;
            self.output = Some(output);
            Ok(())
        } else {
            let output = &mut std::io::stdout();
            self.runtime.block_on(fmt::glimpse(self, plan, output))
        }
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
        format_config: FormatConfig {
            max_table_width: Some(82),
            ..Default::default()
        },
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
            "anti_join" => joins::eval(args, ctx, joins::JoinType::Anti)?,
            "arrange" => arrange::eval(args, ctx)?,
            "config" => config::eval(args, ctx)?,
            "count" => count::eval(args, ctx)?,
            "cross_join" => joins::eval(args, ctx, joins::JoinType::Cross)?,
            "csv" => csv::eval(args, ctx)?,
            "distinct" => distinct::eval(args, ctx)?,
            "filter" => filter::eval(args, ctx)?,
            "glimpse" => glimpse::eval(args, ctx)?,
            "group_by" => group_by::eval(args, ctx)?,
            "head" => head::eval(args, ctx)?,
            "inner_join" => joins::eval(args, ctx, joins::JoinType::Inner)?,
            "json" => json::eval(args, ctx)?,
            "left_join" => joins::eval(args, ctx, joins::JoinType::Left)?,
            "mutate" => mutate::eval(args, ctx)?,
            "outer_join" => joins::eval(args, ctx, joins::JoinType::Outer)?,
            "parquet" => parquet::eval(args, ctx)?,
            "relocate" => relocate::eval(args, ctx)?,
            "rename" => rename::eval(args, ctx)?,
            "select" => select::eval(args, ctx)?,
            "show" => show::eval(args, ctx)?,
            "summarize" => summarize::eval(args, ctx)?,
            "unnest" => unnest::eval(args, ctx)?,
            _ => panic!("Unknown function {name}"),
        },
        Expr::Identifier(name) => {
            // If there is an input assign it to the variable.
            if let Some(plan) = ctx.take_plan() {
                ctx.vars.insert(name.to_owned(), plan.clone());
                ctx.set_plan(plan);
            } else if let Some(plan) = ctx.vars.get(name) {
                ctx.set_plan(plan.clone());
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
