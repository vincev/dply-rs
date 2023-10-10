// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Test binary for all dply functions.
mod arrange;
mod count;
mod df_var;
mod distinct;
mod filter;
mod glimpse;
mod group_by;
mod head;
mod join;
mod json;
mod mutate;
mod relocate;
mod rename;
mod select;
mod show;
mod unnest;

macro_rules! assert_interpreter {
    ($input:expr, $expected:expr) => {
        match dply::interpreter::eval_to_string($input) {
            Err(e) => panic!("Parser failed for:\n{}\n{e}", $input),
            Ok(output) => {
                if output != $expected {
                    panic!(
                        "Interpreter error expected output:\n{}\n===\nGenerated output:\n{}\n===",
                        $expected, output
                    );
                }
            }
        }
    };
}

pub(crate) use assert_interpreter;
