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

pub type SignaturesMap = HashMap<&'static str, Args>;

pub fn functions() -> &'static SignaturesMap {
    static SIGNATURES: OnceLock<SignaturesMap> = OnceLock::new();

    SIGNATURES.get_or_init(|| {
        let mut signatures = HashMap::new();

        def_arrange(&mut signatures);
        def_count(&mut signatures);
        def_csv(&mut signatures);
        def_distinct(&mut signatures);
        def_filter(&mut signatures);
        def_glimpse(&mut signatures);
        def_group_by(&mut signatures);
        def_head(&mut signatures);
        def_joins(&mut signatures);
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
    matches!(name, "parquet" | "csv" | "starts_with" | "ends_with")
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
    Ordered(Vec<ArgType>),
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
                for arg in args {
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
    Arith(Box<ArgType>),
    /// An assign expression
    Assign(Box<ArgType>, Box<ArgType>),
    /// A bool type.
    Bool,
    /// A compare expression.
    Compare(Box<ArgType>, Box<ArgType>),
    /// An equality expression.
    Eq(Box<ArgType>, Box<ArgType>),
    /// A function call expression.
    Function(&'static str, Box<Args>),
    /// An identifier expression.
    Identifier,
    /// A logical expression.
    Logical(Box<ArgType>),
    /// A named identifier.
    Named(&'static str),
    /// A negation expression.
    Negate(Box<ArgType>),
    /// A number.
    Number,
    /// A multi type argument.
    OneOf(Vec<ArgType>),
    /// A string argument.
    String,
}

impl ArgType {
    /// Creates an assignment type.
    fn assign(lhs: ArgType, rhs: ArgType) -> Self {
        Self::Assign(lhs.into(), rhs.into())
    }

    /// Creates an arithmetic type (+, *, -, /).
    fn arith(arg: ArgType) -> Self {
        Self::Arith(arg.into())
    }

    /// Creates a comparison type (<, >, !=, ==, <=, >=).
    fn compare(lhs: ArgType, rhs: ArgType) -> Self {
        Self::Compare(lhs.into(), rhs.into())
    }

    /// Creates an equality type.
    fn eq(lhs: ArgType, rhs: ArgType) -> Self {
        Self::Eq(lhs.into(), rhs.into())
    }

    /// Creates a logical type (&, |).
    fn logical(arg: ArgType) -> Self {
        Self::Logical(arg.into())
    }

    /// Creates a not type.
    fn negate(arg: ArgType) -> Self {
        Self::Negate(arg.into())
    }

    /// Creates a function ßtype.
    fn function(name: &'static str, args: Args) -> Self {
        ArgType::Function(name, args.into())
    }

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
                let name = if let Args::None = args.as_ref() {
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
                for arg in args {
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
        Args::OneOrMore(ArgType::OneOf(vec![
            ArgType::Identifier,
            ArgType::function("desc", Args::Ordered(vec![ArgType::Identifier])),
        ])),
    );
}

fn def_count(signatures: &mut SignaturesMap) {
    signatures.insert(
        "count",
        Args::ZeroOrMore(ArgType::OneOf(vec![
            ArgType::Identifier,
            ArgType::assign(ArgType::Named("sort"), ArgType::Bool),
        ])),
    );
}

fn def_csv(signatures: &mut SignaturesMap) {
    signatures.insert(
        "csv",
        Args::OneThenMore(
            ArgType::String,
            ArgType::assign(ArgType::Named("overwrite"), ArgType::Bool),
        ),
    );
}

fn def_distinct(signatures: &mut SignaturesMap) {
    signatures.insert("distinct", Args::OneOrMore(ArgType::Identifier));
}

fn def_filter(signatures: &mut SignaturesMap) {
    let compare_args = ArgType::compare(
        ArgType::Identifier,
        ArgType::OneOf(vec![
            ArgType::Identifier,
            ArgType::Number,
            ArgType::String,
            ArgType::Bool,
            ArgType::function("dt", Args::Ordered(vec![ArgType::String])),
        ]),
    );

    let contains_fn = ArgType::function(
        "contains",
        Args::Ordered(vec![
            ArgType::Identifier,
            ArgType::OneOf(vec![ArgType::String, ArgType::Number]),
        ]),
    );

    let is_null_fn = ArgType::function("is_null", Args::Ordered(vec![ArgType::Identifier]));

    let predicates = ArgType::OneOf(vec![
        contains_fn.clone(),
        ArgType::negate(contains_fn),
        is_null_fn.clone(),
        ArgType::negate(is_null_fn),
    ]);

    let filter_arg = ArgType::OneOf(vec![compare_args, predicates]);

    signatures.insert(
        "filter",
        Args::OneOrMore(ArgType::OneOf(vec![
            filter_arg.clone(),
            ArgType::logical(filter_arg),
        ])),
    );
}

fn def_glimpse(signatures: &mut SignaturesMap) {
    signatures.insert("glimpse", Args::None);
}

fn def_group_by(signatures: &mut SignaturesMap) {
    signatures.insert("group_by", Args::OneOrMore(ArgType::Identifier));
}

fn def_head(signatures: &mut SignaturesMap) {
    signatures.insert("head", Args::NoneOrOne(ArgType::Number));
}

fn def_joins(signatures: &mut SignaturesMap) {
    let args = Args::OneThenMore(
        ArgType::Identifier,
        ArgType::eq(ArgType::Identifier, ArgType::Identifier),
    );

    signatures.insert("cross_join", args.clone());
    signatures.insert("inner_join", args.clone());
    signatures.insert("left_join", args.clone());
    signatures.insert("outer_join", args);
}

fn def_mutate(signatures: &mut SignaturesMap) {
    let operand = ArgType::OneOf(vec![
        ArgType::Identifier,
        ArgType::Number,
        ArgType::String,
        ArgType::function("dt", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function("len", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function("max", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function("mean", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function("median", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function("min", Args::Ordered(vec![ArgType::Identifier])),
        ArgType::function(
            "to_ns",
            Args::Ordered(vec![ArgType::OneOf(vec![
                ArgType::Identifier,
                ArgType::arith(ArgType::Identifier),
            ])]),
        ),
    ]);

    let expr = ArgType::OneOf(vec![operand.clone(), ArgType::arith(operand)]);

    signatures.insert(
        "mutate",
        Args::OneOrMore(ArgType::assign(ArgType::Identifier, expr)),
    );
}

fn def_parquet(signatures: &mut SignaturesMap) {
    signatures.insert(
        "parquet",
        Args::OneThenMore(
            ArgType::String,
            ArgType::assign(ArgType::Named("overwrite"), ArgType::Bool),
        ),
    );
}

fn def_relocate(signatures: &mut SignaturesMap) {
    signatures.insert(
        "relocate",
        Args::OneOrMore(ArgType::OneOf(vec![
            ArgType::Identifier,
            ArgType::assign(ArgType::Named("after"), ArgType::Identifier),
            ArgType::assign(ArgType::Named("before"), ArgType::Identifier),
        ])),
    );
}

fn def_rename(signatures: &mut SignaturesMap) {
    signatures.insert(
        "rename",
        Args::OneOrMore(ArgType::assign(ArgType::Identifier, ArgType::Identifier)),
    );
}

fn def_select(signatures: &mut SignaturesMap) {
    let contains_fn = ArgType::function("contains", Args::Ordered(vec![ArgType::String]));
    let ends_with_fn = ArgType::function("ends_with", Args::Ordered(vec![ArgType::String]));
    let start_with_fn = ArgType::function("starts_with", Args::Ordered(vec![ArgType::String]));

    signatures.insert(
        "select",
        Args::OneOrMore(ArgType::OneOf(vec![
            ArgType::Identifier,
            ArgType::assign(ArgType::Identifier, ArgType::Identifier),
            contains_fn.clone(),
            ArgType::negate(contains_fn),
            ends_with_fn.clone(),
            ArgType::negate(ends_with_fn),
            start_with_fn.clone(),
            ArgType::negate(start_with_fn),
        ])),
    );
}

fn def_summarize(signatures: &mut SignaturesMap) {
    signatures.insert(
        "summarize",
        Args::OneOrMore(ArgType::Assign(
            Box::new(ArgType::Identifier),
            Box::new(ArgType::OneOf(vec![
                ArgType::function("list", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("max", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("mean", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("median", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("min", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("n", Args::None),
                ArgType::function(
                    "quantile",
                    Args::Ordered(vec![ArgType::Identifier, ArgType::Number]),
                ),
                ArgType::function("sd", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("sum", Args::Ordered(vec![ArgType::Identifier])),
                ArgType::function("var", Args::Ordered(vec![ArgType::Identifier])),
            ])),
        )),
    );
}

fn def_show(signatures: &mut SignaturesMap) {
    signatures.insert("show", Args::None);
}

fn def_unnest(signatures: &mut SignaturesMap) {
    signatures.insert("unnest", Args::OneOrMore(ArgType::Identifier));
}
