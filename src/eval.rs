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
use anyhow::{anyhow, bail, Result};
use polars::prelude::*;
use std::collections::HashMap;

use crate::parser::Expr;

mod args;
mod arrange;
mod count;
mod csv;
mod distinct;
mod filter;
mod glimpse;
mod group_by;
mod head;
mod joins;
mod mutate;
mod parquet;
mod relocate;
mod rename;
mod select;
mod show;
mod summarize;
mod unnest;

#[derive(Default)]
pub struct Context {
    /// Named data frames.
    vars: HashMap<String, LazyFrame>,
    /// Input dataframe passed from one pipeline step to the next.
    df: Option<LazyFrame>,
    /// Group passed to aggregate functions.
    group: Option<LazyGroupBy>,
    /// Optional output used for testing.
    output: Option<Vec<u8>>,
    /// Dataframe columns.
    columns: Vec<String>,
}

impl Context {
    /// Returns and consume the input dataframe.
    pub fn take_df(&mut self) -> Option<LazyFrame> {
        self.df.take()
    }

    /// Sets the dataframe to be used in pipeline steps.
    pub fn set_df(&mut self, df: LazyFrame) -> Result<()> {
        assert!(self.group.is_none());

        self.columns = df
            .schema()
            .map_err(|e| anyhow!("Schema error: {e}"))?
            .iter_names()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        self.df = Some(df);
        Ok(())
    }

    /// Returns the dataframe associated to the given variable.
    pub fn get_df(&self, name: &str) -> Option<&LazyFrame> {
        self.vars.get(name)
    }

    /// Returns the last dataframe columns.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns and consume the active group.
    pub fn take_group(&mut self) -> Option<LazyGroupBy> {
        self.group.take()
    }

    /// Gets the active group.
    pub fn is_grouping(&mut self) -> bool {
        self.group.is_some()
    }

    /// Sets the active group.
    pub fn set_group(&mut self, group: LazyGroupBy) {
        assert!(self.df.is_none());
        self.group = Some(group);
    }

    /// Print results to the context output.
    pub fn print<F>(&mut self, out_fn: F) -> Result<()>
    where
        F: Fn(&mut dyn std::io::Write) -> std::io::Result<()>,
    {
        if let Some(write) = self.output.as_mut() {
            out_fn(write).map_err(anyhow::Error::from)
        } else {
            out_fn(&mut std::io::stdout()).map_err(anyhow::Error::from)
        }
    }
}

/// Evaluate pipelines expressions to standard output.
pub fn eval(exprs: &[Expr]) -> Result<()> {
    // Let the interpreters handle the number of rows in the output.
    std::env::set_var("POLARS_FMT_MAX_ROWS", i64::MAX.to_string());

    let mut ctx = Context::default();
    eval_pipelines(exprs, &mut ctx)
}

/// Evaluate pipelines expressions to a string output, used for testing.
pub fn eval_to_string(exprs: &[Expr]) -> Result<String> {
    let mut ctx = Context {
        output: Some(Default::default()),
        ..Default::default()
    };

    // Let the interpreters handle the number of rows in the output.
    std::env::set_var("POLARS_FMT_MAX_ROWS", i64::MAX.to_string());
    // Sets the number of columns for testing output.
    std::env::set_var("POLARS_FMT_STR_LEN", "120");
    std::env::set_var("POLARS_TABLE_WIDTH", "120");

    eval_pipelines(exprs, &mut ctx)?;

    Ok(String::from_utf8(ctx.output.unwrap())?)
}

fn eval_pipelines(exprs: &[Expr], ctx: &mut Context) -> Result<()> {
    for expr in exprs {
        if let Expr::Pipeline(exprs) = expr {
            for expr in exprs {
                eval_pipeline_step(expr, ctx)?;
            }

            // Consume df after the end of a pipeline so that the next pipeline
            // starts with a clean state.
            ctx.take_df();
        }
    }

    Ok(())
}

fn eval_pipeline_step(expr: &Expr, ctx: &mut Context) -> Result<()> {
    match expr {
        Expr::Function(name, args) => match name.as_str() {
            "arrange" => arrange::eval(args, ctx)?,
            "count" => count::eval(args, ctx)?,
            "cross_join" => joins::eval(args, ctx, JoinType::Cross)?,
            "csv" => csv::eval(args, ctx)?,
            "distinct" => distinct::eval(args, ctx)?,
            "filter" => filter::eval(args, ctx)?,
            "glimpse" => glimpse::eval(args, ctx)?,
            "group_by" => group_by::eval(args, ctx)?,
            "head" => head::eval(args, ctx)?,
            "inner_join" => joins::eval(args, ctx, JoinType::Inner)?,
            "left_join" => joins::eval(args, ctx, JoinType::Left)?,
            "mutate" => mutate::eval(args, ctx)?,
            "outer_join" => joins::eval(args, ctx, JoinType::Outer)?,
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
            if let Some(df) = ctx.take_df() {
                ctx.vars.insert(name.to_owned(), df.clone());
                ctx.set_df(df)?;
            } else if let Some(df) = ctx.vars.get(name) {
                ctx.set_df(df.clone())?;
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
