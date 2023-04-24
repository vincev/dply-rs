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
use crate::parser::{Expr, Operator};

/// Error from a matcher function.
#[derive(Debug, thiserror::Error)]
pub enum MatchError {
    /// A recoverable error, the `or` operator will try alternatives.
    #[error("Match error {0}")]
    Error(String),
    /// An unrecoverable error.
    #[error("Match failure {0}")]
    Failure(String),
}

macro_rules! match_error {
    ($($arg:tt)*) => {
        MatchError::Error(format!($($arg)*))
    };
}

/// Matcher result type.
pub type MatchResult = Result<(), MatchError>;

/// An expression type matcher.
pub trait Matcher {
    /// Matches an expression.
    fn matches(&self, expr: &Expr) -> MatchResult;

    /// Combine two matchers returns success if any of them succeed.
    fn or<R>(self, rhs: R) -> Or<Self, R>
    where
        R: Matcher,
        Self: Sized,
    {
        Or { lhs: self, rhs }
    }

    /// Combine two matchers returns success if both of them succeed.
    fn and<R>(self, rhs: R) -> And<Self, R>
    where
        R: Matcher,
        Self: Sized,
    {
        And { lhs: self, rhs }
    }

    /// Combine two matchers returns success if both of them succeed.
    ///
    /// If the lhs matcher fails it returns its error but if the rhs fails the
    /// error becomes a failure that will prevent successive `or` combinators to
    /// try other matchers.
    fn and_fail<R>(self, rhs: R) -> AndFail<Self, R>
    where
        R: Matcher,
        Self: Sized,
    {
        AndFail { lhs: self, rhs }
    }
}

/// An `Or` matcher, succeed if the `lhs` or `rhs` matcher succeed.
pub struct Or<L, R> {
    lhs: L,
    rhs: R,
}

impl<L: Matcher, R: Matcher> Matcher for Or<L, R> {
    fn matches(&self, expr: &Expr) -> MatchResult {
        match self.lhs.matches(expr) {
            Err(MatchError::Error(_)) => self.rhs.matches(expr),
            res => res,
        }
    }
}

/// An `And` matcher, succeed if the `lhs` and `rhs` matcher succeed.
pub struct And<L, R> {
    lhs: L,
    rhs: R,
}

impl<L: Matcher, R: Matcher> Matcher for And<L, R> {
    fn matches(&self, expr: &Expr) -> MatchResult {
        self.lhs.matches(expr)?;
        self.rhs.matches(expr)
    }
}

/// An `AndFail` matcher that returns a failure if `rhs` fails.
pub struct AndFail<L, R> {
    lhs: L,
    rhs: R,
}

impl<L: Matcher, R: Matcher> Matcher for AndFail<L, R> {
    fn matches(&self, expr: &Expr) -> MatchResult {
        self.lhs.matches(expr)?;
        match self.rhs.matches(expr) {
            Err(MatchError::Error(s)) => Err(MatchError::Failure(s)),
            res => res,
        }
    }
}

impl<F> Matcher for F
where
    F: Fn(&Expr) -> MatchResult,
{
    fn matches(&self, expr: &Expr) -> MatchResult {
        self(expr)
    }
}

/// Matches a function expression by name.
pub fn match_function(name: &str) -> impl Matcher {
    let match_name = name.to_string();
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(name, _) if name == &match_name => Ok(()),
            Expr::Function(_, _) => Err(match_error!("Unknown function: {expr}")),
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches a function with at most n arguments.
pub fn match_max_args(n: usize) -> impl Matcher {
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if args.len() <= n => Ok(()),
            Expr::Function(_, _) => Err(match_error!("Too many arguments for: {expr}")),
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches a function with at least n arguments.
pub fn match_min_args(n: usize) -> impl Matcher {
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if args.len() >= n => Ok(()),
            Expr::Function(_, _) => Err(match_error!("Too few arguments for: {expr}")),
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches all arguments for a function.
pub fn match_args<M>(m: M) -> impl Matcher
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) => args.iter().try_for_each(|e| m.matches(e)),
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches an argument at the given index.
pub fn match_arg<M>(idx: usize, m: M) -> impl Matcher
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if idx < args.len() => m.matches(&args[idx]),
            Expr::Function(_, _) => {
                Err(match_error!("No argument at index {idx} on call to {expr}"))
            }
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches an optional argument at the given index.
pub fn match_opt_arg<M>(idx: usize, m: M) -> impl Matcher
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if idx < args.len() => m.matches(&args[idx]),
            Expr::Function(_, _) => Ok(()),
            _ => Err(match_error!("'{expr}' is not a function")),
        }
    }
}

/// Matches an assignment with lhs and rhs matchers.
pub fn match_assign<L, R>(l: L, r: R) -> impl Matcher
where
    L: Matcher,
    R: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = expr {
            l.matches(lhs)?;
            r.matches(rhs)
        } else {
            Err(match_error!("'{expr}' must be an assignment"))
        }
    }
}

/// Matches a comparison with lhs and rhs matchers.
pub fn match_compare<L, R>(l: L, r: R) -> impl Matcher
where
    L: Matcher,
    R: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::BinaryOp(lhs, Operator::Eq, rhs)
            | Expr::BinaryOp(lhs, Operator::NotEq, rhs)
            | Expr::BinaryOp(lhs, Operator::Lt, rhs)
            | Expr::BinaryOp(lhs, Operator::LtEq, rhs)
            | Expr::BinaryOp(lhs, Operator::Gt, rhs)
            | Expr::BinaryOp(lhs, Operator::GtEq, rhs) => {
                l.matches(lhs)?;
                r.matches(rhs)
            }
            _ => Err(match_error!("'{expr}' must be a comparison")),
        }
    }
}

/// Matches a logical operator operands with the given matcher.
pub fn match_logical<M>(m: M) -> impl Matcher
where
    M: Matcher,
{
    fn is_logical(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp(_, Operator::And, _) | Expr::BinaryOp(_, Operator::Or, _)
        )
    }

    fn matcher<M: Matcher>(expr: &Expr, m: &M) -> MatchResult {
        match expr {
            Expr::BinaryOp(lhs, Operator::And, rhs) | Expr::BinaryOp(lhs, Operator::Or, rhs) => {
                if is_logical(lhs) {
                    matcher(lhs, m)?;
                }

                if is_logical(rhs) {
                    matcher(rhs, m)?;
                }

                if !is_logical(lhs) {
                    m.matches(lhs)?;
                }

                if !is_logical(rhs) {
                    m.matches(rhs)?;
                }

                Ok(())
            }
            _ => Err(match_error!("'{expr}' must be a logical expression")),
        }
    }

    move |expr: &Expr| -> MatchResult { matcher(expr, &m) }
}

/// Matches an arithmetic operation
pub fn match_arith<M>(m: M) -> impl Matcher
where
    M: Matcher,
{
    fn is_arith(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp(_, Operator::Plus, _)
                | Expr::BinaryOp(_, Operator::Minus, _)
                | Expr::BinaryOp(_, Operator::Divide, _)
                | Expr::BinaryOp(_, Operator::Multiply, _)
        )
    }

    fn matcher<M: Matcher>(expr: &Expr, m: &M) -> MatchResult {
        match expr {
            Expr::BinaryOp(lhs, Operator::Plus, rhs)
            | Expr::BinaryOp(lhs, Operator::Minus, rhs)
            | Expr::BinaryOp(lhs, Operator::Divide, rhs)
            | Expr::BinaryOp(lhs, Operator::Multiply, rhs) => {
                if is_arith(lhs) {
                    matcher(lhs, m)?;
                }

                if is_arith(rhs) {
                    matcher(rhs, m)?;
                }

                if !is_arith(lhs) {
                    m.matches(lhs)?;
                }

                if !is_arith(rhs) {
                    m.matches(rhs)?;
                }

                Ok(())
            }
            _ => Err(match_error!("'{expr}' must be an arithmetic expression")),
        }
    }

    move |expr: &Expr| -> MatchResult { matcher(expr, &m) }
}

/// Matches an identifier by name.
pub fn match_named(name: &str) -> impl Matcher {
    let name = name.to_string();
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Identifier(s) if s == &name => Ok(()),
            Expr::Identifier(s) => Err(match_error!("Unexpected identifier {s}")),
            _ => Err(match_error!("'{expr}' must be an identifier")),
        }
    }
}

/// Matches an identifier.
pub fn match_identifier(expr: &Expr) -> MatchResult {
    match expr {
        Expr::Identifier(_) => Ok(()),
        _ => Err(match_error!("'{expr}' must be an identifier")),
    }
}

/// Matches a string.
pub fn match_string(expr: &Expr) -> MatchResult {
    match expr {
        Expr::String(_) => Ok(()),
        _ => Err(match_error!("'{expr}' must be a string")),
    }
}

/// Matches a string.
pub fn match_number(expr: &Expr) -> MatchResult {
    match expr {
        Expr::Number(_) => Ok(()),
        _ => Err(match_error!("'{expr}' must be a number")),
    }
}

/// Matches a bool identifier.
pub fn match_bool(expr: &Expr) -> MatchResult {
    match expr {
        Expr::Identifier(s) if s == "true" || s == "false" => Ok(()),
        _ => Err(match_error!("'{expr}' must be a boolean")),
    }
}
