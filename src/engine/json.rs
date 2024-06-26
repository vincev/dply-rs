// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, bail, Result};
use polars::prelude::*;
use std::{num::NonZeroUsize, path::PathBuf};

use crate::parser::Expr;

use super::*;

/// Evaluates a json call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    let path = PathBuf::from(args::string(&args[0]));
    let overwrite = args::named_bool(args, "overwrite")?;

    // If there is an input dataframe save it to disk.
    if let Some(df) = ctx.take_df() {
        if !overwrite && path.exists() {
            bail!("json error: file '{}' already exists.", path.display());
        }

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("parquet error: cannot create file '{}' {e}", path.display()))?;

        let mut out_df = df.clone().collect()?;
        ctx.set_df(df)?;

        JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut out_df)?;
    } else {
        // Read the data frame and set it as input for the next task.
        let df = LazyJsonLineReader::new(&path)
            .with_infer_schema_length(NonZeroUsize::new(1000))
            .finish()
            .map_err(|e| anyhow!("json error: cannot read file '{}' {e}", path.display()))?;
        ctx.set_df(df)?;
    }

    Ok(())
}
