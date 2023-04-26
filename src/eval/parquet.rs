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
use anyhow::{anyhow, bail, Result};
use polars::prelude::*;
use std::path::PathBuf;

use crate::parser::Expr;

use super::*;

pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    // parquet("nyctaxi.parquet")
    let path = PathBuf::from(args::strings(args).first().unwrap());
    // parquet("nyctaxi.parquet", overwrite = true)
    let overwrite = args::named_bool(args, "overwrite")?;

    // If there is an input dataframe save it to disk.
    if let Some(df) = ctx.get_input() {
        if !overwrite && path.exists() {
            bail!("Parquet file '{}' already exists.", path.display());
        }

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("Cannot create parquet file '{}' {e}", path.display()))?;

        let mut out_df = df.clone().collect()?;
        ParquetWriter::new(file).finish(&mut out_df)?;
    } else {
        // Read the data frame and set it as input for the next task.
        let df = LazyFrame::scan_parquet(&path, ScanArgsParquet::default())
            .map_err(|e| anyhow!("Cannot read parquet file '{}' {e}", path.display()))?;
        ctx.set_input(df);
    }

    Ok(())
}
