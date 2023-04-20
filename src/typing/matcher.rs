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
use super::*;

/// Error from a matcher function.
#[derive(Debug, thiserror::Error)]
#[error("Match error {msg}")]
pub struct MatchError {
    msg: String,
}

impl MatchError {
    fn new<M: ToString>(msg: M) -> Self {
        Self {
            msg: msg.to_string(),
        }
    }
}

/// Matcher result type.
pub type MatchResult = Result<(), MatchError>;

/// An expression type matcher.
pub trait Matcher {
    /// Matches an expression.
    fn matches(&self, input: &Expr) -> MatchResult;

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
}

/// An `Or` matcher, succeed if the `lhs` or `rhs` matcher succeed.
pub struct Or<L, R> {
    lhs: L,
    rhs: R,
}

impl<L: Matcher, R: Matcher> Matcher for Or<L, R> {
    fn matches(&self, input: &Expr) -> MatchResult {
        match self.lhs.matches(input) {
            Err(_) => self.rhs.matches(input),
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
    fn matches(&self, input: &Expr) -> MatchResult {
        self.lhs.matches(input)?;
        self.rhs.matches(input)
    }
}

impl<F> Matcher for F
where
    F: Fn(&Expr) -> MatchResult,
{
    fn matches(&self, input: &Expr) -> MatchResult {
        self(input)
    }
}

/// Matches a function expression by name.
pub fn match_function(name: &str) -> impl Fn(&Expr) -> MatchResult {
    let name = name.to_string();
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(fname, _) if fname == &name => Ok(()),
            Expr::Function(_, _) => Err(MatchError::new(format!("Unknown function {name}"))),
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches a function with at most n arguments.
pub fn match_max_args(n: usize) -> impl Fn(&Expr) -> MatchResult {
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if args.len() <= n => Ok(()),
            Expr::Function(name, _) => {
                Err(MatchError::new(format!("Too many arguments for {name}")))
            }
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches a function with at least n arguments.
pub fn match_min_args(n: usize) -> impl Fn(&Expr) -> MatchResult {
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if args.len() >= n => Ok(()),
            Expr::Function(name, _) => {
                Err(MatchError::new(format!("Too few arguments for {name}")))
            }
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches all arguments for a function.
pub fn match_args<M>(m: M) -> impl Fn(&Expr) -> MatchResult
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) => args.iter().try_for_each(|e| m.matches(e)),
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches an argument at the given index.
pub fn match_arg<M>(idx: usize, m: M) -> impl Fn(&Expr) -> MatchResult
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if idx < args.len() => m.matches(&args[idx]),
            Expr::Function(name, _) => Err(MatchError::new(format!(
                "No argument at index {idx} on call to {name}"
            ))),
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches an optional argument at the given index.
pub fn match_opt_arg<M>(idx: usize, m: M) -> impl Fn(&Expr) -> MatchResult
where
    M: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Function(_, args) if idx < args.len() => m.matches(&args[idx]),
            Expr::Function(_, _) => Ok(()),
            _ => Err(MatchError::new("Not a function expression")),
        }
    }
}

/// Matches an assignment with lhs and rhs matchers.
pub fn match_assign<L, R>(l: L, r: R) -> impl Fn(&Expr) -> MatchResult
where
    L: Matcher,
    R: Matcher,
{
    move |expr: &Expr| -> MatchResult {
        if let Expr::BinaryOp(lhs, Operator::Assign, rhs) = expr {
            l.matches(lhs)?;
            r.matches(rhs)
        } else {
            Err(MatchError::new("Not an assignment expression"))
        }
    }
}

/// Matches a comparison with lhs and rhs matchers.
pub fn match_compare<L, R>(l: L, r: R) -> impl Fn(&Expr) -> MatchResult
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
            _ => Err(MatchError::new("Not a compare expression")),
        }
    }
}

/// Matches a logical operator operands with the given matcher.
pub fn match_logical<M>(m: M) -> impl Fn(&Expr) -> MatchResult
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

                if !is_logical(lhs) && !is_logical(rhs) {
                    m.matches(lhs)?;
                    m.matches(rhs)?;
                }

                Ok(())
            }
            _ => Err(MatchError::new("Not a logical expression")),
        }
    }

    move |expr: &Expr| -> MatchResult { matcher(expr, &m) }
}

/// Matches an identifier by name.
pub fn match_named(name: &str) -> impl Fn(&Expr) -> MatchResult {
    let name = name.to_string();
    move |expr: &Expr| -> MatchResult {
        match expr {
            Expr::Identifier(s) if s == &name => Ok(()),
            Expr::Identifier(s) => Err(MatchError::new(format!("Unexpected identifier {s}"))),
            _ => Err(MatchError::new("Not an identifier")),
        }
    }
}

/// Matches an identifier.
pub fn match_identifier(expr: &Expr) -> MatchResult {
    if let Expr::Identifier(_) = expr {
        Ok(())
    } else {
        Err(MatchError::new("Not an identifier"))
    }
}

/// Matches a string.
pub fn match_string(expr: &Expr) -> MatchResult {
    if let Expr::String(_) = expr {
        Ok(())
    } else {
        Err(MatchError::new("Not a string"))
    }
}

/// Matches a string.
pub fn match_number(expr: &Expr) -> MatchResult {
    if let Expr::Number(_) = expr {
        Ok(())
    } else {
        Err(MatchError::new("Not a string"))
    }
}

/// Matches a bool identifier.
pub fn match_bool(expr: &Expr) -> MatchResult {
    match expr {
        Expr::Identifier(s) if s == "true" || s == "false" => Ok(()),
        _ => Err(MatchError::new("Not a boolean")),
    }
}
