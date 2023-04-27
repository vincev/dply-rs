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

//! Interpreter for dply expressions.
use anyhow::Result;

use crate::{eval, parser, typing};

/// Evaluates a dply script.
pub fn eval(input: &str) -> Result<()> {
    let pipelines = parser::parse(input)?;

    typing::validate(&pipelines)?;
    eval::eval(&pipelines)?;

    Ok(())
}

/// Evaluates a dply script with a string output.
pub fn eval_to_string(input: &str) -> Result<String> {
    let pipelines = parser::parse(input)?;

    typing::validate(&pipelines)?;
    eval::eval_to_string(&pipelines)
}
