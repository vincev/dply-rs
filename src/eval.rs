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
use polars::prelude::*;
use std::collections::HashMap;

use crate::parser::Expr;

mod args;
mod arrange;
mod csv;
mod glimpse;
mod parquet;

#[derive(Default)]
pub struct Context {
    /// Named data frames.
    vars: HashMap<String, LazyFrame>,
    /// Input dataframe passed from one pipeline step to the next.
    input: Option<LazyFrame>,
    /// Optional output used for testing.
    output: Option<Vec<u8>>,
}

impl Context {
    /// Returns and consume the input dataframe.
    pub fn take_input(&mut self) -> Option<LazyFrame> {
        self.input.take()
    }

    /// Get the input dataframe.
    pub fn get_input(&mut self) -> Option<&LazyFrame> {
        self.input.as_ref()
    }

    /// Set the dataframe to be used in pipeline steps.
    pub fn set_input(&mut self, df: LazyFrame) {
        self.input = Some(df);
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
    std::env::set_var("POLARS_FMT_STR_LEN", "82");

    eval_pipelines(exprs, &mut ctx)?;

    Ok(String::from_utf8(ctx.output.unwrap())?)
}

fn eval_pipelines(exprs: &[Expr], ctx: &mut Context) -> Result<()> {
    for expr in exprs {
        if let Expr::Pipeline(exprs) = expr {
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
            "arrange" => arrange::eval(args, ctx)?,
            "count" => {}
            "csv" => csv::eval(args, ctx)?,
            "distinct" => {}
            "filter" => {}
            "glimpse" => glimpse::eval(args, ctx)?,
            "group_by" => {}
            "mutate" => {}
            "parquet" => parquet::eval(args, ctx)?,
            "relocate" => {}
            "rename" => {}
            "select" => {}
            "summarize" => {}
            _ => panic!("Unknown function {name}"),
        },
        Expr::Identifier(name) => {
            // If there is an input assign it to the variable.
            if let Some(df) = ctx.get_input() {
                let df = df.clone();
                ctx.vars.insert(name.to_owned(), df);
            } else if let Some(df) = ctx.vars.get(name) {
                ctx.set_input(df.clone());
            } else {
                bail!("Undefined variable {name}");
            }
        }
        _ => panic!("Unexpected expression {expr}"),
    }

    Ok(())
}
