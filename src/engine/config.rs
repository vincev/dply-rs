// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
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
