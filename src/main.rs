// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
#![warn(clippy::all, rust_2018_idioms)]

// Enable jemalloc as it improves performance in MacOS.
#[global_allocator]
#[cfg(all(not(debug_assertions), target_family = "unix"))]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::{anyhow, Result};
use clap::Parser;
use std::fs;
use std::io::{self, IsTerminal, Read};
use std::path::PathBuf;

use dply::{interpreter, repl};

/// Cli interface.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// dply script file path, use standard input if not provided.
    pub path: Option<PathBuf>,

    /// dply command passed as string.
    #[arg(long, short)]
    pub command: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(input) = cli.command {
        interpreter::eval(&input)?;
    } else if let Some(path) = cli.path {
        let input = fs::read_to_string(&path)
            .map_err(|e| anyhow!("Error reading script {}: {e}", path.display()))?;
        interpreter::eval(&input)?;
    } else if io::stdin().is_terminal() {
        repl::run()?;
    } else {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        interpreter::eval(&input)?;
    };

    Ok(())
}
