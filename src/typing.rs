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

//! Checks pipeline functions and arguments types.
use anyhow::Result;

use crate::parser::Expr;
use matcher::*;

mod matcher;

/// Checks pipeline functions and arguments types.
pub fn validate(exprs: &[Expr]) -> Result<()> {
    for expr in exprs {
        if let Expr::Pipeline(exprs) = expr {
            for expr in exprs {
                match_identifier.or(match_pipeline_fn).matches(expr)?;
            }
        }
    }

    Ok(())
}

/// Checks arguments for pipeline functions.
fn match_pipeline_fn(expr: &Expr) -> MatchResult {
    match_arrange
        .or(match_count)
        .or(match_csv)
        .or(match_distinct)
        .or(match_filter)
        .or(match_glimpse)
        .or(match_group_by)
        .or(match_head)
        .or(match_mutate)
        .or(match_parquet)
        .or(match_relocate)
        .or(match_rename)
        .or(match_select)
        .or(match_show)
        .or(match_summarize)
        .matches(expr)
}

/// Checks arguments for arrange call.
fn match_arrange(expr: &Expr) -> MatchResult {
    let desc_fn = match_function("desc")
        .and(match_min_args(1))
        .and(match_max_args(1))
        .and(match_args(match_identifier));

    match_function("arrange")
        .and_fail(match_min_args(1).and(match_args(match_identifier.or(desc_fn))))
        .matches(expr)
}

/// Checks arguments for count call.
fn match_count(expr: &Expr) -> MatchResult {
    let sort_opt = match_assign(match_named("sort"), match_bool);

    match_function("count")
        .and_fail(match_args(match_identifier.or(sort_opt)))
        .matches(expr)
}

/// Checks arguments for csv call.
fn match_csv(expr: &Expr) -> MatchResult {
    let overwrite_opt = match_assign(match_named("overwrite"), match_bool);

    match_function("csv")
        .and_fail(
            match_max_args(2)
                .and(match_arg(0, match_string))
                .and(match_opt_arg(1, overwrite_opt)),
        )
        .matches(expr)
}

/// Checks arguments for distinct call.
fn match_distinct(expr: &Expr) -> MatchResult {
    match_function("distinct")
        .and_fail(match_args(match_identifier))
        .matches(expr)
}

/// Checks arguments for filter call.
fn match_filter(expr: &Expr) -> MatchResult {
    let compare_op = || {
        let dt_fn = match_function("dt")
            .and(match_min_args(1))
            .and(match_max_args(1))
            .and(match_args(match_string));

        let rhs_cmp = match_identifier
            .or(match_number)
            .or(match_string)
            .or(match_bool)
            .or(dt_fn);

        let contains_fn = || {
            match_function("contains")
                .and(match_min_args(2))
                .and(match_max_args(2))
                .and(match_arg(0, match_identifier))
                .and(match_arg(1, match_string.or(match_number)))
        };

        let is_null_fn = || match_column_fn("is_null");

        let predicates = contains_fn()
            .or(match_negate(contains_fn()))
            .or(is_null_fn())
            .or(match_negate(is_null_fn()));

        match_compare(match_identifier, rhs_cmp).or(predicates)
    };

    let logic_op = match_logical(compare_op());
    let filter_args = compare_op().or(logic_op);

    match_function("filter")
        .and_fail(match_min_args(1).and(match_args(filter_args)))
        .matches(expr)
}

/// Checks arguments for glimpse call.
fn match_glimpse(expr: &Expr) -> MatchResult {
    match_function("glimpse")
        .and_fail(match_max_args(0))
        .matches(expr)
}

/// Checks arguments for group_by call.
fn match_group_by(expr: &Expr) -> MatchResult {
    match_function("group_by")
        .and_fail(match_min_args(1).and(match_args(match_identifier)))
        .matches(expr)
}

/// Checks arguments for a head call.
fn match_head(expr: &Expr) -> MatchResult {
    // head()
    // head(10)
    match_function("head")
        .and_fail(
            match_min_args(0)
                .and(match_max_args(1))
                .and(match_args(match_number)),
        )
        .matches(expr)
}

/// Checks arguments for mutate call.
fn match_mutate(expr: &Expr) -> MatchResult {
    let operand_fn = || {
        match_identifier
            .or(match_number)
            .or(match_string)
            .or(match_column_fn("dt"))
            .or(match_column_fn("min"))
            .or(match_column_fn("max"))
            .or(match_column_fn("mean"))
            .or(match_column_fn("median"))
    };

    let rhs = operand_fn().or(match_arith(operand_fn()));

    match_function("mutate")
        .and_fail(match_min_args(1).and(match_args(match_assign(match_identifier, rhs))))
        .matches(expr)
}

/// Checks arguments for parquet call.
fn match_parquet(expr: &Expr) -> MatchResult {
    let overwrite_opt = match_assign(match_named("overwrite"), match_bool);

    match_function("parquet")
        .and_fail(
            match_max_args(2)
                .and(match_arg(0, match_string))
                .and(match_opt_arg(1, overwrite_opt)),
        )
        .matches(expr)
}

/// Checks arguments for relocate call.
fn match_relocate(expr: &Expr) -> MatchResult {
    let before_opt = match_assign(match_named("before"), match_identifier);

    // relocate(gain, speed, after = day)
    let after_opt = match_assign(match_named("after"), match_identifier);

    let args = match_identifier.or(before_opt).or(after_opt);

    match_function("relocate")
        .and_fail(match_min_args(1).and(match_args(args)))
        .matches(expr)
}

/// Checks arguments for rename call.
fn match_rename(expr: &Expr) -> MatchResult {
    let rename_opt = match_assign(match_identifier, match_identifier);

    match_function("rename")
        .and_fail(match_min_args(1).and(match_args(rename_opt)))
        .matches(expr)
}

/// Checks arguments for select call.
fn match_select(expr: &Expr) -> MatchResult {
    let contains_fn = || {
        match_function("contains")
            .and(match_min_args(1))
            .and(match_max_args(1))
            .and(match_args(match_string))
    };

    // select(starts_with("time"), !starts_with("time"))
    let starts_with_fn = || {
        match_function("starts_with")
            .and(match_min_args(1))
            .and(match_max_args(1))
            .and(match_args(match_string))
    };

    // select(ends_with("time"), !ends_with("time"))
    let ends_with_fn = || {
        match_function("ends_with")
            .and(match_min_args(1))
            .and(match_max_args(1))
            .and(match_args(match_string))
    };

    // select(tail_num = tailnum)
    let rename_opt = match_assign(match_identifier, match_identifier);

    let args = contains_fn()
        .or(match_negate(contains_fn()))
        .or(starts_with_fn())
        .or(match_negate(starts_with_fn()))
        .or(ends_with_fn())
        .or(match_negate(ends_with_fn()))
        .or(rename_opt)
        .or(match_identifier);

    match_function("select")
        .and_fail(match_min_args(1).and(match_args(args)))
        .matches(expr)
}

/// Checks arguments for a show call.
fn match_show(expr: &Expr) -> MatchResult {
    match_function("show")
        .and_fail(match_max_args(0))
        .matches(expr)
}

/// Checks arguments for summarize call.
fn match_summarize(expr: &Expr) -> MatchResult {
    let n_fn = match_function("n").and(match_max_args(0));

    // quantile(n = quantile(passenger_count, 0.75))
    let quantile_fn = match_function("quantile")
        .and(match_min_args(2))
        .and(match_max_args(2))
        .and(match_arg(0, match_identifier))
        .and(match_arg(1, match_number));

    let summarize_op = n_fn
        .or(quantile_fn)
        .or(match_column_fn("max"))
        .or(match_column_fn("mean"))
        .or(match_column_fn("median"))
        .or(match_column_fn("min"))
        .or(match_column_fn("sd"))
        .or(match_column_fn("sum"))
        .or(match_column_fn("var"));

    match_function("summarize")
        .and_fail(match_min_args(0).and(match_args(match_assign(match_identifier, summarize_op))))
        .matches(expr)
}

/// Matches a function that takes a column identifier as parameter.
fn match_column_fn(name: &str) -> impl Matcher {
    match_function(name)
        .and(match_min_args(1))
        .and(match_max_args(1))
        .and(match_args(match_identifier))
}
