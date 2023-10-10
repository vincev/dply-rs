// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Data manipulation tool inspired by the [dplyr](https://dplyr.tidyverse.org/) grammar.
#![warn(clippy::all, rust_2018_idioms, missing_docs)]

pub mod interpreter;
pub mod repl;

mod completions;
mod config;
mod engine;
mod fuzzy;
mod parser;
mod signatures;
mod typing;
