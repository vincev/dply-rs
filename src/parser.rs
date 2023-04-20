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

//! Parser for dply expressions.
use anyhow::{anyhow, bail, Result};
use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not, tag};
use nom::character::complete::{alpha1, alphanumeric1, char, multispace0, newline};
use nom::combinator::{cut, map, opt, recognize, value, verify};
use nom::error::{context, convert_error, VerboseError};
use nom::multi::{many0, many0_count, many1_count, separated_list0, separated_list1};
use nom::number::complete::double;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::{IResult, Offset};
use std::fmt;
use std::io::Write;

/// A parsed dply expression.
#[derive(Debug)]
pub enum Expr {
    /// A pipeline of data manipulation expressions.
    Pipeline(Vec<Expr>),
    /// A function invocation.
    Function(String, Vec<Expr>),
    /// Binary operation
    BinaryOp(Box<Expr>, Operator, Box<Expr>),
    /// Unary operation
    UnaryOp(Operator, Box<Expr>),
    /// An identifier
    Identifier(String),
    /// A string literal
    String(String),
    /// A number literal
    Number(f64),
}

/// A binary operation.
#[derive(Debug, Copy, Clone)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Logical and
    And,
    /// Logical or
    Or,
    /// Logical not
    Not,
    /// Assignment
    Assign,
}

impl Expr {
    /// Visits the expression tree.
    pub fn visit<V: Visitor>(&self, visitor: &mut V) -> Result<()> {
        match self {
            Expr::Pipeline(exprs) => {
                visitor.pre_pipeline()?;
                for expr in exprs {
                    expr.visit(visitor)?;
                }
                visitor.post_pipeline()?;
            }
            Expr::Function(name, args) => {
                visitor.pre_function(name, args.len())?;
                for expr in args {
                    expr.visit(visitor)?;
                }
                visitor.post_function(name, args.len())?;
            }
            Expr::BinaryOp(lhs, op, rhs) => {
                visitor.pre_binary_op(op)?;
                lhs.visit(visitor)?;
                rhs.visit(visitor)?;
                visitor.post_binary_op(op)?;
            }
            Expr::UnaryOp(op, expr) => {
                visitor.pre_unary_op(op)?;
                expr.visit(visitor)?;
                visitor.post_unary_op(op)?;
            }
            Expr::Identifier(id) => visitor.identifier(id)?,
            Expr::String(s) => visitor.string(s)?,
            Expr::Number(v) => visitor.number(*v)?,
        }

        Ok(())
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v = DisplayVisitor::default();
        self.visit(&mut v).unwrap();
        write!(f, "{}", v.output())
    }
}

/// Visitor for expressions tree.
#[allow(unused_variables)]
pub trait Visitor {
    /// Called before visiting a pipeline expression.
    fn pre_pipeline(&mut self) -> Result<()> {
        Err(anyhow!("Visitor pre_pipeline not implemented!"))
    }

    /// Called after visiting a pipeline expression.
    fn post_pipeline(&mut self) -> Result<()> {
        Err(anyhow!("Visitor post_pipeline not implemented!"))
    }

    /// Called before visiting a function call expression.
    fn pre_function(&mut self, name: &str, args: usize) -> Result<()> {
        Err(anyhow!("Visitor pre_function not implemented!"))
    }

    /// Called after visiting a function call expression.
    fn post_function(&mut self, name: &str, args: usize) -> Result<()> {
        Err(anyhow!("Visitor post_function not implemented!"))
    }

    /// Called before visiting a binary operation expression.
    fn pre_binary_op(&mut self, op: &Operator) -> Result<()> {
        Err(anyhow!("Visitor pre_binary_op not implemented!"))
    }

    /// Called after visiting a binary operation expression.
    fn post_binary_op(&mut self, op: &Operator) -> Result<()> {
        Err(anyhow!("Visitor post_binary_op not implemented!"))
    }

    /// Called before visiting a unary operation expression.
    fn pre_unary_op(&mut self, op: &Operator) -> Result<()> {
        Err(anyhow!("Visitor pre_unary_op not implemented!"))
    }

    /// Called after visiting a unary operation expression.
    fn post_unary_op(&mut self, op: &Operator) -> Result<()> {
        Err(anyhow!("Visitor post_unary_op not implemented!"))
    }

    /// Visit an identifier.
    fn identifier(&mut self, value: &str) -> Result<()> {
        Err(anyhow!("Visitor identifier not implemented!"))
    }

    /// Visit a string.
    fn string(&mut self, value: &str) -> Result<()> {
        Err(anyhow!("Visitor string not implemented!"))
    }

    /// Visit a number.
    fn number(&mut self, value: f64) -> Result<()> {
        Err(anyhow!("Visitor number not implemented!"))
    }
}

fn ws(input: &str) -> IResult<&str, (), VerboseError<&str>> {
    value((), many0_count(is_a(" \t")))(input)
}

fn comment(input: &str) -> IResult<&str, (), VerboseError<&str>> {
    value((), pair(preceded(ws, char('#')), is_not("\n\r")))(input)
}

fn name(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))(input)
}

fn identifier(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(preceded(ws, name), |s| Expr::Identifier(s.to_string()))(input)
}

fn string(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let literal = verify(is_not("\""), |s: &str| !s.is_empty());
    map(
        preceded(char('"'), cut(terminated(literal, char('"')))),
        |s: &str| Expr::String(s.to_string()),
    )(input)
}

/// A group expression `(a == b & c == d) | f != g`.
fn group(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    context(
        "group",
        preceded(
            multispace0,
            delimited(
                char('('),
                preceded(multispace0, argument),
                cut(preceded(multispace0, char(')'))),
            ),
        ),
    )(input)
}

fn expression(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    context(
        "expression",
        preceded(
            ws,
            alt((
                function,
                unary_op,
                identifier,
                string,
                map(double, Expr::Number),
                group,
            )),
        ),
    )(input)
}

fn unary_op(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let operator = alt((
        map(tag("+"), |_| Operator::Plus),
        map(tag("-"), |_| Operator::Minus),
        map(tag("!"), |_| Operator::Not),
    ));

    context(
        "unary_op",
        map(pair(operator, expression), |(op, expr)| {
            Expr::UnaryOp(op, Box::new(expr))
        }),
    )(input)
}

fn compare_op(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let operator = alt((
        map(tag("=="), |_| Operator::Eq),
        map(tag("!="), |_| Operator::NotEq),
        map(tag("<="), |_| Operator::LtEq),
        map(tag("<"), |_| Operator::Lt),
        map(tag(">="), |_| Operator::GtEq),
        map(tag(">"), |_| Operator::Gt),
    ));

    context(
        "binary_op",
        map(
            tuple((
                preceded(multispace0, expression),
                preceded(multispace0, operator),
                preceded(multispace0, alt((compare_op, expression))),
            )),
            |(lhs, op, rhs)| Expr::BinaryOp(Box::new(lhs), op, Box::new(rhs)),
        ),
    )(input)
}

fn logical_op(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let operator = alt((
        map(tag("&"), |_| Operator::And),
        map(tag("|"), |_| Operator::Or),
    ));

    context(
        "logical_op",
        map(
            tuple((
                preceded(multispace0, alt((compare_op, expression))),
                preceded(multispace0, operator),
                preceded(multispace0, alt((logical_op, compare_op, expression))),
            )),
            |(lhs, op, rhs)| Expr::BinaryOp(Box::new(lhs), op, Box::new(rhs)),
        ),
    )(input)
}

fn arith_op(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let operator = alt((
        map(tag("+"), |_| Operator::Plus),
        map(tag("-"), |_| Operator::Minus),
        map(tag("*"), |_| Operator::Multiply),
        map(tag("/"), |_| Operator::Divide),
    ));

    context(
        "logical_op",
        map(
            tuple((
                preceded(multispace0, expression),
                preceded(multispace0, operator),
                preceded(multispace0, alt((arith_op, expression))),
            )),
            |(lhs, op, rhs)| Expr::BinaryOp(Box::new(lhs), op, Box::new(rhs)),
        ),
    )(input)
}

fn assign_op(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    context(
        "logical_op",
        map(
            tuple((
                preceded(multispace0, identifier),
                preceded(multispace0, map(tag("="), |_| Operator::Assign)),
                preceded(multispace0, alt((arith_op, expression))),
            )),
            |(lhs, op, rhs)| Expr::BinaryOp(Box::new(lhs), op, Box::new(rhs)),
        ),
    )(input)
}

fn argument(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    context(
        "argument",
        preceded(
            multispace0,
            alt((assign_op, logical_op, compare_op, unary_op, expression)),
        ),
    )(input)
}

fn function(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let args = delimited(
        char('('),
        separated_list0(preceded(multispace0, char(',')), argument),
        cut(preceded(multispace0, char(')'))),
    );

    context(
        "function",
        preceded(
            many0(is_a(" \t")),
            map(tuple((name, args)), |(s, args)| {
                Expr::Function(s.to_string(), args)
            }),
        ),
    )(input)
}

/// Parses a pipeline.
///
/// A pipeline can be a list of function calls or identifiers separated by a pipe.
fn pipeline(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let separator = tuple((ws, tag("|"), ws, opt(newline)));
    let terminator = tuple((ws, opt(newline)));

    context(
        "pipeline",
        terminated(
            preceded(
                many0(terminated(comment, newline)),
                map(
                    separated_list1(separator, cut(alt((function, identifier)))),
                    Expr::Pipeline,
                ),
            ),
            terminator,
        ),
    )(input)
}

/// Parses one or more pipelines.
fn root(input: &str) -> IResult<&str, Vec<Expr>, VerboseError<&str>> {
    separated_list1(many1_count(newline), cut(pipeline))(input)
}

/// Parses one or more dply pipelines.
pub fn parse(input: &str) -> Result<Vec<Expr>> {
    match root(input.trim()) {
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            bail!("Parse error: {}", convert_error(input, e))
        }
        Err(e) => bail!("Parse error: {e}"),
        Ok(("", exprs)) => Ok(exprs),
        Ok((remain, _)) => {
            let offset = input.offset(remain);
            let line_no = &input[..offset].chars().filter(|c| *c == '\n').count() + 1;
            let (remain_line, _) = remain.split_once('\n').unwrap_or((remain, ""));
            bail!("Parse error at line {}: {}", line_no, remain_line);
        }
    }
}

#[derive(Debug, Default)]
struct DisplayVisitor {
    depth: usize,
    output: Vec<u8>,
}

impl DisplayVisitor {
    fn write(&mut self, msg: String) {
        writeln!(&mut self.output, "{:1$}{msg}", "", self.depth).unwrap();
    }

    fn output(self) -> String {
        let mut s = String::from_utf8(self.output).unwrap();
        s.truncate(s.trim_end().len());
        s
    }
}

impl Visitor for DisplayVisitor {
    fn pre_pipeline(&mut self) -> Result<()> {
        self.write("pre_pipeline".to_string());
        self.depth += 2;
        Ok(())
    }

    fn post_pipeline(&mut self) -> Result<()> {
        self.depth -= 2;
        self.write("post_pipeline".to_string());
        Ok(())
    }

    fn pre_function(&mut self, name: &str, args: usize) -> Result<()> {
        self.write(format!("pre_function: {name}({args})"));
        self.depth += 2;
        Ok(())
    }

    fn post_function(&mut self, name: &str, args: usize) -> Result<()> {
        self.depth -= 2;
        self.write(format!("post_function: {name}({args})"));
        Ok(())
    }

    fn pre_binary_op(&mut self, op: &Operator) -> Result<()> {
        self.write(format!("pre_binary_op: {:?}", op));
        self.depth += 2;
        Ok(())
    }

    fn post_binary_op(&mut self, op: &Operator) -> Result<()> {
        self.depth -= 2;
        self.write(format!("post_binary_op: {:?}", op));
        Ok(())
    }

    fn pre_unary_op(&mut self, op: &Operator) -> Result<()> {
        self.write(format!("pre_unary_op: {:?}", op));
        self.depth += 2;
        Ok(())
    }

    fn post_unary_op(&mut self, op: &Operator) -> Result<()> {
        self.depth -= 2;
        self.write(format!("post_unary_op: {:?}", op));
        Ok(())
    }

    fn identifier(&mut self, name: &str) -> Result<()> {
        self.write(format!("identifier: {name}"));
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<()> {
        self.write(format!("string: {value}"));
        Ok(())
    }

    fn number(&mut self, value: f64) -> Result<()> {
        self.write(format!("number: {value}"));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    macro_rules! assert_parser {
        ($text:expr, $expected:expr) => {
            match parse($text) {
                Err(e) => panic!("Parser failed for:\n{}\n{e}", $text),
                Ok(exprs) => {
                    let mut visitor = DisplayVisitor::default();
                    exprs
                        .into_iter()
                        .for_each(|e| e.visit(&mut visitor).unwrap());
                    let output = visitor.output();
                    if output.trim() != $expected.trim() {
                        panic!(
                            "Parser error:\nexpected:\n{}\nfound:\n{}",
                            $expected, output
                        );
                    }
                }
            }
        };
    }

    #[test]
    fn one_line_pipeline() {
        // Single function call.
        let text = r#"parquet("test.parquet")"#;
        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                post_pipeline"
            )
        );

        // Pipe between two functions.
        let text = r#"parquet("test.parquet") | glimpse()"#;
        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                  pre_function: glimpse(0)
                  post_function: glimpse(0)
                post_pipeline"
            )
        );

        // Single variable
        let text = r#"names_df"#;
        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  identifier: names_df
                post_pipeline"
            )
        );

        // Variable to pipeline
        let text = r#"names_df | glimpse()"#;
        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  identifier: names_df
                  pre_function: glimpse(0)
                  post_function: glimpse(0)
                post_pipeline"
            )
        );

        // Incomplete pipeline.
        let text = r#"parquet("test.parquet") | "#;
        assert!(parse(text).is_err());
    }

    #[test]
    fn multiline_pipeline() {
        let text = indoc! {r#"
            parquet("test.parquet") |
                select(first_name, last_name) |
                filter(year < 2020) |
                show(limit = 25)
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                  pre_function: select(2)
                    identifier: first_name
                    identifier: last_name
                  post_function: select(2)
                  pre_function: filter(1)
                    pre_binary_op: Lt
                      identifier: year
                      number: 2020
                    post_binary_op: Lt
                  post_function: filter(1)
                  pre_function: show(1)
                    pre_binary_op: Assign
                      identifier: limit
                      number: 25
                    post_binary_op: Assign
                  post_function: show(1)
                post_pipeline"
            )
        );
    }

    #[test]
    fn comment() {
        let text = indoc! {r#"
            # This is a pipeline that reads a parquet file
            parquet("test.parquet") |
              glimpse()
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                  pre_function: glimpse(0)
                  post_function: glimpse(0)
                post_pipeline"
            )
        );
    }

    #[test]
    fn numbers() {
        let text = indoc! {r#"
            parquet("test.parquet") |
              mutate(distance = 9.8 / 2 * time * time)
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                  pre_function: mutate(1)
                    pre_binary_op: Assign
                      identifier: distance
                      pre_binary_op: Divide
                        number: 9.8
                        pre_binary_op: Multiply
                          number: 2
                          pre_binary_op: Multiply
                            identifier: time
                            identifier: time
                          post_binary_op: Multiply
                        post_binary_op: Multiply
                      post_binary_op: Divide
                    post_binary_op: Assign
                  post_function: mutate(1)
                post_pipeline"
            )
        );
    }

    #[test]
    fn select_columns_and_rename() {
        let text = indoc! {r#"
            select(last_name, !first_name, start_time = start_time_dt)
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: select(3)
                    identifier: last_name
                    pre_unary_op: Not
                      identifier: first_name
                    post_unary_op: Not
                    pre_binary_op: Assign
                      identifier: start_time
                      identifier: start_time_dt
                    post_binary_op: Assign
                  post_function: select(3)
                post_pipeline"
            )
        );
    }

    #[test]
    fn select_columns_with_or_predicates() {
        let text = indoc! {r#"
            select(contains("year"), !ends_with("date"))
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: select(2)
                    pre_function: contains(1)
                      string: year
                    post_function: contains(1)
                    pre_unary_op: Not
                      pre_function: ends_with(1)
                        string: date
                      post_function: ends_with(1)
                    post_unary_op: Not
                  post_function: select(2)
                post_pipeline"
            )
        );
    }

    #[test]
    fn select_columns_with_and_predicates() {
        let text = indoc! {r#"
            select(!starts_with("time") & contains("year") & !ends_with("day"))
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: select(1)
                    pre_binary_op: And
                      pre_unary_op: Not
                        pre_function: starts_with(1)
                          string: time
                        post_function: starts_with(1)
                      post_unary_op: Not
                      pre_binary_op: And
                        pre_function: contains(1)
                          string: year
                        post_function: contains(1)
                        pre_unary_op: Not
                          pre_function: ends_with(1)
                            string: day
                          post_function: ends_with(1)
                        post_unary_op: Not
                      post_binary_op: And
                    post_binary_op: And
                  post_function: select(1)
                post_pipeline"
            )
        );
    }

    #[test]
    fn and_or_filter() {
        let text = indoc! {r#"
            filter(month == 7 | day >= 23 & year < 2020)
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: filter(1)
                    pre_binary_op: Or
                      pre_binary_op: Eq
                        identifier: month
                        number: 7
                      post_binary_op: Eq
                      pre_binary_op: And
                        pre_binary_op: GtEq
                          identifier: day
                          number: 23
                        post_binary_op: GtEq
                        pre_binary_op: Lt
                          identifier: year
                          number: 2020
                        post_binary_op: Lt
                      post_binary_op: And
                    post_binary_op: Or
                  post_function: filter(1)
                post_pipeline"#
            )
        );
    }

    #[test]
    fn op_grouping() {
        let text = indoc! {r#"
            filter((month == 12 | day == 23) & year == 2023)
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: filter(1)
                    pre_binary_op: And
                      pre_binary_op: Or
                        pre_binary_op: Eq
                          identifier: month
                          number: 12
                        post_binary_op: Eq
                        pre_binary_op: Eq
                          identifier: day
                          number: 23
                        post_binary_op: Eq
                      post_binary_op: Or
                      pre_binary_op: Eq
                        identifier: year
                        number: 2023
                      post_binary_op: Eq
                    post_binary_op: And
                  post_function: filter(1)
                post_pipeline"#
            )
        );

        let text = indoc! {r#"
            filter(month == 12 | (day == 23 & year == 2023))
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: filter(1)
                    pre_binary_op: Or
                      pre_binary_op: Eq
                        identifier: month
                        number: 12
                      post_binary_op: Eq
                      pre_binary_op: And
                        pre_binary_op: Eq
                          identifier: day
                          number: 23
                        post_binary_op: Eq
                        pre_binary_op: Eq
                          identifier: year
                          number: 2023
                        post_binary_op: Eq
                      post_binary_op: And
                    post_binary_op: Or
                  post_function: filter(1)
                post_pipeline"#
            )
        );

        let text = indoc! {r#"
            filter((day == 23) & year == 2023)
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: filter(1)
                    pre_binary_op: And
                      pre_binary_op: Eq
                        identifier: day
                        number: 23
                      post_binary_op: Eq
                      pre_binary_op: Eq
                        identifier: year
                        number: 2023
                      post_binary_op: Eq
                    post_binary_op: And
                  post_function: filter(1)
                post_pipeline"#
            )
        );
    }
}
