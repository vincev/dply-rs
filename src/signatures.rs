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
/// A function signature arguments.
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::fuzzy;

pub type SignaturesMap = HashMap<&'static str, &'static Args>;

pub fn functions() -> &'static SignaturesMap {
    static SIGNATURES: OnceLock<SignaturesMap> = OnceLock::new();

    SIGNATURES.get_or_init(|| {
        let mut signatures = HashMap::new();

        def_arrange(&mut signatures);
        def_config(&mut signatures);
        def_count(&mut signatures);
        def_csv(&mut signatures);
        def_distinct(&mut signatures);
        def_filter(&mut signatures);
        def_glimpse(&mut signatures);
        def_group_by(&mut signatures);
        def_head(&mut signatures);
        def_joins(&mut signatures);
        def_json(&mut signatures);
        def_mutate(&mut signatures);
        def_parquet(&mut signatures);
        def_relocate(&mut signatures);
        def_rename(&mut signatures);
        def_show(&mut signatures);
        def_select(&mut signatures);
        def_summarize(&mut signatures);
        def_unnest(&mut signatures);

        signatures
    })
}

pub fn completions(pattern: &str) -> Vec<String> {
    static NAMES: OnceLock<Vec<String>> = OnceLock::new();

    let names = NAMES.get_or_init(|| {
        let mut names = Vec::with_capacity(1024);

        for (name, args) in functions() {
            let name = if let Args::None = args {
                format!("{name}()")
            } else if has_string_arg(name) {
                format!("{name}(\"")
            } else {
                format!("{name}(")
            };

            names.push(name);
            names.extend(args.names());
        }

        names.push("true".to_string());
        names.push("false".to_string());

        names.sort();
        names.dedup();

        names
    });

    let matcher = fuzzy::Matcher::new(pattern);

    names
        .iter()
        .filter(|s| matcher.is_match(s))
        .map(|s| s.to_string())
        .collect()
}

fn has_string_arg(name: &str) -> bool {
    // We don't include "contains" as the one used in filter doesn't take a
    // string parameter (e.g. filter(contains(name, "john"))).
    matches!(
        name,
        "parquet" | "csv" | "json" | "starts_with" | "ends_with"
    )
}

#[derive(Debug, Clone)]
pub enum Args {
    /// No arguments.
    None,
    /// Zero or one arguments.
    NoneOrOne(ArgType),
    /// Zero or more arguments.
    ZeroOrMore(ArgType),
    /// One or more arguments.
    OneOrMore(ArgType),
    /// One argument of the first type and zero or more arguments of the second.
    OneThenMore(ArgType, ArgType),
    /// A function with a fixed number of arguments.
    Ordered(&'static [ArgType]),
}

impl Args {
    /// Extracts all the function and variable names in this arguments.
    fn names(&self) -> Vec<String> {
        let mut names = Vec::new();

        match self {
            Args::NoneOrOne(arg) => names.extend(arg.names()),
            Args::ZeroOrMore(arg) => names.extend(arg.names()),
            Args::OneOrMore(arg) => names.extend(arg.names()),
            Args::OneThenMore(first, rest) => {
                names.extend(first.names());
                names.extend(rest.names());
            }
            Args::Ordered(args) => {
                for arg in &args[..] {
                    names.extend(arg.names());
                }
            }
            _ => {}
        }

        names.sort();
        names.dedup();
        names
    }
}

/// Function argument type.
#[derive(Debug, Clone)]
pub enum ArgType {
    /// An arithmetich expression.
    Arith(&'static ArgType),
    /// An assign expression
    Assign(&'static ArgType, &'static ArgType),
    /// A bool type.
    Bool,
    /// A compare expression.
    Compare(&'static ArgType, &'static ArgType),
    /// An equality expression.
    Eq(&'static ArgType, &'static ArgType),
    /// A function call expression.
    Function(&'static str, &'static Args),
    /// An identifier expression.
    Identifier,
    /// A logical expression.
    Logical(&'static ArgType),
    /// A named identifier.
    Named(&'static str),
    /// A negation expression.
    Negate(&'static ArgType),
    /// A number.
    Number,
    /// A multi type argument.
    OneOf(&'static [&'static ArgType]),
    /// A string argument.
    String,
}

impl ArgType {
    /// Extracts all named types.
    fn names(&self) -> Vec<String> {
        let mut names = Vec::new();

        match self {
            ArgType::Arith(arg) => names.extend(arg.names()),
            ArgType::Assign(lhs, rhs) => {
                names.extend(lhs.names());
                names.extend(rhs.names());
            }
            ArgType::Compare(lhs, rhs) => {
                names.extend(lhs.names());
                names.extend(rhs.names());
            }
            ArgType::Eq(lhs, rhs) => {
                names.extend(lhs.names());
                names.extend(rhs.names());
            }
            ArgType::Function(name, args) => {
                let name = if let Args::None = args {
                    format!("{name}()")
                } else {
                    format!("{name}(")
                };

                names.push(name);
                names.extend(args.names());
            }
            ArgType::Logical(arg) => names.extend(arg.names()),
            ArgType::Named(name) => names.push(name.to_string()),
            ArgType::Negate(arg) => names.extend(arg.names()),
            ArgType::OneOf(args) => {
                for arg in &args[..] {
                    names.extend(arg.names());
                }
            }
            _ => {}
        }

        names
    }
}

fn def_arrange(signatures: &mut SignaturesMap) {
    signatures.insert(
        "arrange",
        &Args::OneOrMore(ArgType::OneOf(&[
            &ArgType::Identifier,
            &ArgType::Function("desc", &Args::Ordered(&[ArgType::Identifier])),
        ])),
    );
}

fn def_config(signatures: &mut SignaturesMap) {
    signatures.insert(
        "config",
        &Args::ZeroOrMore(ArgType::OneOf(&[
            &ArgType::Assign(&ArgType::Named("max_columns"), &ArgType::Number),
            &ArgType::Assign(&ArgType::Named("max_column_width"), &ArgType::Number),
            &ArgType::Assign(&ArgType::Named("max_table_width"), &ArgType::Number),
        ])),
    );
}

fn def_count(signatures: &mut SignaturesMap) {
    signatures.insert(
        "count",
        &Args::ZeroOrMore(ArgType::OneOf(&[
            &ArgType::Identifier,
            &ArgType::Assign(&ArgType::Named("sort"), &ArgType::Bool),
        ])),
    );
}

fn def_csv(signatures: &mut SignaturesMap) {
    signatures.insert(
        "csv",
        &Args::OneThenMore(
            ArgType::String,
            ArgType::Assign(&ArgType::Named("overwrite"), &ArgType::Bool),
        ),
    );
}

fn def_distinct(signatures: &mut SignaturesMap) {
    signatures.insert("distinct", &Args::OneOrMore(ArgType::Identifier));
}

fn def_filter(signatures: &mut SignaturesMap) {
    const COMPARE_ARGS: &ArgType = &ArgType::Compare(
        &ArgType::Identifier,
        &ArgType::OneOf(&[
            &ArgType::Identifier,
            &ArgType::Number,
            &ArgType::String,
            &ArgType::Bool,
            &ArgType::Function("dt", &Args::Ordered(&[ArgType::String])),
        ]),
    );

    const CONTAINS_FN: &ArgType = &ArgType::Function(
        "contains",
        &Args::Ordered(&[
            ArgType::Identifier,
            ArgType::OneOf(&[&ArgType::String, &ArgType::Number]),
        ]),
    );

    const IS_NULL_FN: &ArgType =
        &ArgType::Function("is_null", &Args::Ordered(&[ArgType::Identifier]));

    const PREDICATES: &ArgType = &ArgType::OneOf(&[
        CONTAINS_FN,
        &ArgType::Negate(CONTAINS_FN),
        IS_NULL_FN,
        &ArgType::Negate(IS_NULL_FN),
    ]);

    const FILTER_ARG: &ArgType = &ArgType::OneOf(&[COMPARE_ARGS, PREDICATES]);

    signatures.insert(
        "filter",
        &Args::OneOrMore(ArgType::OneOf(&[FILTER_ARG, &ArgType::Logical(FILTER_ARG)])),
    );
}

fn def_glimpse(signatures: &mut SignaturesMap) {
    signatures.insert("glimpse", &Args::None);
}

fn def_group_by(signatures: &mut SignaturesMap) {
    signatures.insert("group_by", &Args::OneOrMore(ArgType::Identifier));
}

fn def_head(signatures: &mut SignaturesMap) {
    signatures.insert("head", &Args::NoneOrOne(ArgType::Number));
}

fn def_joins(signatures: &mut SignaturesMap) {
    let args = &Args::OneThenMore(
        ArgType::Identifier,
        ArgType::Eq(&ArgType::Identifier, &ArgType::Identifier),
    );

    signatures.insert("anti_join", args);
    signatures.insert("cross_join", args);
    signatures.insert("inner_join", args);
    signatures.insert("left_join", args);
    signatures.insert("outer_join", args);
}

fn def_json(signatures: &mut SignaturesMap) {
    signatures.insert(
        "json",
        &Args::OneThenMore(
            ArgType::String,
            ArgType::OneOf(&[
                &ArgType::Assign(&ArgType::Named("overwrite"), &ArgType::Bool),
                &ArgType::Assign(&ArgType::Named("schema_rows"), &ArgType::Number),
            ]),
        ),
    );
}

fn def_mutate(signatures: &mut SignaturesMap) {
    const OPERAND: &ArgType = &ArgType::OneOf(&[
        &ArgType::Identifier,
        &ArgType::Number,
        &ArgType::String,
        &ArgType::Function("dt", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function(
            "field",
            &Args::Ordered(&[ArgType::Identifier, ArgType::Identifier]),
        ),
        &ArgType::Function("len", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function("max", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function("mean", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function("median", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function("min", &Args::Ordered(&[ArgType::Identifier])),
        &ArgType::Function("row", &Args::None),
        &ArgType::Function(
            "to_ns",
            &Args::Ordered(&[ArgType::OneOf(&[
                &ArgType::Identifier,
                &ArgType::Arith(&ArgType::Identifier),
            ])]),
        ),
    ]);

    const EXPR: &ArgType = &ArgType::OneOf(&[OPERAND, &ArgType::Arith(OPERAND)]);

    signatures.insert(
        "mutate",
        &Args::OneOrMore(ArgType::Assign(&ArgType::Identifier, EXPR)),
    );
}

fn def_parquet(signatures: &mut SignaturesMap) {
    signatures.insert(
        "parquet",
        &Args::OneThenMore(
            ArgType::String,
            ArgType::Assign(&ArgType::Named("overwrite"), &ArgType::Bool),
        ),
    );
}

fn def_relocate(signatures: &mut SignaturesMap) {
    signatures.insert(
        "relocate",
        &Args::OneOrMore(ArgType::OneOf(&[
            &ArgType::Identifier,
            &ArgType::Assign(&ArgType::Named("after"), &ArgType::Identifier),
            &ArgType::Assign(&ArgType::Named("before"), &ArgType::Identifier),
        ])),
    );
}

fn def_rename(signatures: &mut SignaturesMap) {
    signatures.insert(
        "rename",
        &Args::OneOrMore(ArgType::Assign(&ArgType::Identifier, &ArgType::Identifier)),
    );
}

fn def_select(signatures: &mut SignaturesMap) {
    const CONTAINS_FN: &ArgType =
        &ArgType::Function("contains", &Args::Ordered(&[ArgType::String]));
    const ENDS_WITH_FN: &ArgType =
        &ArgType::Function("ends_with", &Args::Ordered(&[ArgType::String]));
    const START_WITH_FN: &ArgType =
        &ArgType::Function("starts_with", &Args::Ordered(&[ArgType::String]));

    signatures.insert(
        "select",
        &Args::OneOrMore(ArgType::OneOf(&[
            &ArgType::Identifier,
            &ArgType::Assign(&ArgType::Identifier, &ArgType::Identifier),
            CONTAINS_FN,
            &ArgType::Negate(CONTAINS_FN),
            ENDS_WITH_FN,
            &ArgType::Negate(ENDS_WITH_FN),
            START_WITH_FN,
            &ArgType::Negate(START_WITH_FN),
        ])),
    );
}

fn def_summarize(signatures: &mut SignaturesMap) {
    signatures.insert(
        "summarize",
        &Args::OneOrMore(ArgType::Assign(
            &ArgType::Identifier,
            &ArgType::OneOf(&[
                &ArgType::Function("list", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("max", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("mean", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("median", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("min", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("n", &Args::None),
                &ArgType::Function(
                    "quantile",
                    &Args::Ordered(&[ArgType::Identifier, ArgType::Number]),
                ),
                &ArgType::Function("sd", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("sum", &Args::Ordered(&[ArgType::Identifier])),
                &ArgType::Function("var", &Args::Ordered(&[ArgType::Identifier])),
            ]),
        )),
    );
}

fn def_show(signatures: &mut SignaturesMap) {
    signatures.insert("show", &Args::None);
}

fn def_unnest(signatures: &mut SignaturesMap) {
    signatures.insert("unnest", &Args::OneOrMore(ArgType::Identifier));
}
