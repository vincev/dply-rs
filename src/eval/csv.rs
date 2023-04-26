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
    let path = PathBuf::from(args::get_string_at(args, 0)?);
    let overwrite = args::get_bool(args, "overwrite")?;

    // If there is an input dataframe save it to disk.
    if let Some(df) = ctx.get_input() {
        if !overwrite && path.exists() {
            bail!("Csv file '{}' already exists.", path.display());
        }

        let file = std::fs::File::create(&path)
            .map_err(|e| anyhow!("Cannot create csv file '{}' {e}", path.display()))?;

        let mut out_df = df.clone().collect()?;
        CsvWriter::new(file).finish(&mut out_df)?;
    } else {
        let reader = LazyCsvReader::new(&path).with_infer_schema_length(Some(1000));
        let df = reader
            .finish()
            .map_err(|e| anyhow!("Cannot read csv file '{}' {e}", path.display()))?;
        ctx.set_input(df);
    }

    Ok(())
}
