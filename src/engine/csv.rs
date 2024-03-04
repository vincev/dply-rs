// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, bail, Result};
use polars::prelude::*;
use std::path::PathBuf;

use crate::parser::Expr;

use super::*;

/// Evaluates a csv call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    // csv("nyctaxi.csv")
    let path = PathBuf::from(args::string(&args[0]));
    // csv("nyctaxi.csv", overwrite = true)
    let overwrite = args::named_bool(args, "overwrite")?;

    // If there is an input dataframe save it to disk.
    if let Some(df) = ctx.take_df() {
        if !overwrite && path.exists() {
            bail!("csv error: file '{}' already exists", path.display());
        }

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("csv error: cannot create file '{}' {e}", path.display()))?;

        let mut out_df = df.clone().collect()?;
        ctx.set_df(df)?;

        CsvWriter::new(file).finish(&mut out_df)?;
    } else {
        let reader = LazyCsvReader::new(&path).with_infer_schema_length(Some(1000));
        let df = reader
            .finish()
            .map_err(|e| anyhow!("csv error: cannot read file '{}' {e}", path.display()))?;
        ctx.set_df(df)?;
    }

    Ok(())
}
