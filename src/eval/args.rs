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
use anyhow::Result;
use std::str::FromStr;

use crate::parser::{Expr, Operator};

pub fn strings(args: &[Expr]) -> Vec<String> {
    args.iter()
        .filter_map(|e| match e {
            Expr::String(s) => Some(s.to_owned()),
            _ => None,
        })
        .collect()
}

pub fn identifiers(args: &[Expr]) -> Vec<String> {
    args.iter()
        .filter_map(|e| match e {
            Expr::Identifier(s) => Some(s.to_owned()),
            _ => None,
        })
        .collect()
}

pub fn named_bool(args: &[Expr], name: &str) -> Result<bool> {
    for arg in args {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = arg {
            match (lhs.as_ref(), rhs.as_ref()) {
                (Expr::Identifier(lhs), Expr::Identifier(rhs)) if lhs == name => {
                    return Ok(bool::from_str(rhs)?);
                }
                _ => {}
            }
        }
    }

    Ok(false)
}
