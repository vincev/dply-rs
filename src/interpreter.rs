// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Interpreter for dply expressions.
use anyhow::Result;

use crate::{engine, parser, typing};

/// Evaluates a dply script.
pub fn eval(input: &str) -> Result<()> {
    let pipelines = parser::parse(input)?;
    typing::validate(&pipelines)?;

    let mut ctx = engine::Context::default();
    engine::eval(&mut ctx, &pipelines)?;

    Ok(())
}

/// Evaluates a dply script with a string output.
pub fn eval_to_string(input: &str) -> Result<String> {
    let pipelines = parser::parse(input)?;
    typing::validate(&pipelines)?;
    engine::eval_to_string(&pipelines)
}
