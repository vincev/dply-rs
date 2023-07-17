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
use crate::parser::Expr;

use super::*;

/// Evaluates a config call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Ok(Some(value)) = args::named_usize(args, "max_columns") {
        ctx.format_config.max_columns = value;
    }

    if let Ok(Some(value)) = args::named_usize(args, "max_column_width") {
        ctx.format_config.max_column_width = value;
    }

    if let Ok(Some(value)) = args::named_usize(args, "max_table_width") {
        ctx.format_config.max_table_width = if value > 0 { Some(value) } else { None };
    }

    Ok(())
}
