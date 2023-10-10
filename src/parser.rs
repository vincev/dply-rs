// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Parser for dply expressions.
use anyhow::{bail, Result};
use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not, tag};
use nom::character::complete::{alpha1, alphanumeric1, char, multispace0, newline};
use nom::combinator::{cut, map, recognize, value, verify};
use nom::error::{context, convert_error, VerboseError};
use nom::multi::{many0, many0_count, many1_count, separated_list0, separated_list1};
use nom::number::complete::double;
use nom::sequence::{delimited, pair, preceded, terminated, tuple};
use nom::IResult;
use std::fmt;

/// A parsed dply expression.
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
    /// Multiplication
    Multiply,
    /// Division
    Divide,
    /// Mod
    Mod,
    /// Logical and
    And,
    /// Logical or
    Or,
    /// Logical not
    Not,
    /// Assignment
    Assign,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op = match self {
            Operator::Eq => "==",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Mod => "%",
            Operator::And => "&",
            Operator::Or => "|",
            Operator::Not => "!",
            Operator::Assign => "=",
        };

        write!(f, "{op}")
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Pipeline(exprs) => {
                for (idx, expr) in exprs.iter().enumerate() {
                    if idx > 0 {
                        write!(f, " | ")?;
                    }
                    expr.fmt(f)?;
                }
                Ok(())
            }
            Expr::Function(name, args) => {
                write!(f, "{name}(")?;
                for (idx, arg) in args.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    arg.fmt(f)?;
                }
                write!(f, ")")
            }
            Expr::BinaryOp(lhs, op, rhs) => {
                lhs.fmt(f)?;
                write!(f, " {op} ")?;
                rhs.fmt(f)
            }
            Expr::UnaryOp(op, expr) => {
                write!(f, "{op}")?;
                expr.fmt(f)
            }
            Expr::Identifier(n) => write!(f, "{n}"),
            Expr::String(s) => write!(f, r#""{s}""#),
            Expr::Number(n) => write!(f, "{n}"),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_debug(self, 0, f)
    }
}

fn fmt_debug(expr: &Expr, indent: usize, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    macro_rules! windent {
        ($dst:expr, $($arg:tt)*) => {
            writeln!($dst, "{:1$}{2:}", "", indent, format_args!($($arg)*))
        }
    }

    match expr {
        Expr::Pipeline(exprs) => {
            windent!(f, "pre_pipeline")?;
            for expr in exprs {
                fmt_debug(expr, indent + 2, f)?;
            }
            windent!(f, "post_pipeline")
        }
        Expr::Function(name, args) => {
            windent!(f, "pre_function: {name}({})", args.len())?;
            for arg in args {
                fmt_debug(arg, indent + 2, f)?;
            }
            windent!(f, "post_function: {name}({})", args.len())
        }
        Expr::BinaryOp(lhs, op, rhs) => {
            windent!(f, "pre_binary_op: {op:?}")?;
            fmt_debug(lhs, indent + 2, f)?;
            fmt_debug(rhs, indent + 2, f)?;
            windent!(f, "post_binary_op: {op:?}")
        }
        Expr::UnaryOp(op, expr) => {
            windent!(f, "pre_unary_op: {op:?}")?;
            fmt_debug(expr, indent + 2, f)?;
            windent!(f, "post_unary_op: {op:?}")
        }
        Expr::Identifier(id) => windent!(f, "identifier: {id}"),
        Expr::String(s) => windent!(f, "string: {s}"),
        Expr::Number(n) => windent!(f, "number: {n}"),
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

fn quoted(input: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    let literal = verify(is_not("`"), |s: &str| !s.is_empty());
    map(
        preceded(char('`'), cut(terminated(literal, char('`')))),
        |s: &str| Expr::Identifier(s.to_string()),
    )(input)
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
                preceded(multispace0, alt((arith_op, argument))),
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
                quoted,
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
        map(tag("%"), |_| Operator::Mod),
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
                preceded(multispace0, alt((quoted, identifier))),
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
            alt((
                assign_op, logical_op, compare_op, unary_op, arith_op, expression,
            )),
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
    let separator = tuple((multispace0, tag("|"), multispace0));

    context(
        "pipeline",
        map(
            separated_list0(separator, cut(alt((function, identifier)))),
            Expr::Pipeline,
        ),
    )(input)
}

/// Parses one or more pipelines.
fn root(input: &str) -> IResult<&str, Vec<Expr>, VerboseError<&str>> {
    let separator = alt((char(';'), newline));
    separated_list1(many1_count(separator), cut(pipeline))(input)
}

/// Parses one or more dply pipelines.
pub fn parse(input: &str) -> Result<Vec<Expr>> {
    let input = input
        .lines()
        .filter(|line| comment(line).is_err())
        .map(|line| format!("{line}\n"))
        .collect::<String>();

    match root(input.trim().trim_end_matches(';')) {
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            bail!("Parse error: {}", convert_error(input.as_str(), e))
        }
        Err(e) => bail!("Parse error: {e}"),
        Ok((_, exprs)) => Ok(exprs),
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
                    let output = exprs
                        .iter()
                        .map(|e| format!("{e:?}"))
                        .collect::<Vec<_>>()
                        .join("\n");
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

        // Semicolon separated
        let text = r#"csv("a.csv") | a_df; csv("b.csv") | left_join(a_df) | glimpse()"#;
        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: csv(1)
                    string: a.csv
                  post_function: csv(1)
                  identifier: a_df
                post_pipeline

                pre_pipeline
                  pre_function: csv(1)
                    string: b.csv
                  post_function: csv(1)
                  pre_function: left_join(1)
                    identifier: a_df
                  post_function: left_join(1)
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
                select(first_name, last_name)
             |  filter(year < 2020) |
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
    fn quoted_identifier() {
        let text = indoc! {r#"
            parquet("test.parquet") |
                select(`first name`, last_name) |
                mutate(`next year` = `this year` + 1) |
                filter(`next year` < 2020)
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
                    identifier: first name
                    identifier: last_name
                  post_function: select(2)
                  pre_function: mutate(1)
                    pre_binary_op: Assign
                      identifier: next year
                      pre_binary_op: Plus
                        identifier: this year
                        number: 1
                      post_binary_op: Plus
                    post_binary_op: Assign
                  post_function: mutate(1)
                  pre_function: filter(1)
                    pre_binary_op: Lt
                      identifier: next year
                      number: 2020
                    post_binary_op: Lt
                  post_function: filter(1)
                post_pipeline"
            )
        );
    }

    #[test]
    fn comment() {
        let text = indoc! {r#"
            # This text is for testing comments

            # This is a pipeline that reads a parquet file
            parquet("test.parquet") |
            # select(year, month) |
            # select(year, month, day) |
              glimpse()

            # todo: Add other pipeline with days.
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
              mutate(
                distance = 9.8 / 2 * time * time,
                group_id = id % 10
              )
        "#};

        assert_parser!(
            text,
            indoc!(
                "
                pre_pipeline
                  pre_function: parquet(1)
                    string: test.parquet
                  post_function: parquet(1)
                  pre_function: mutate(2)
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
                    pre_binary_op: Assign
                      identifier: group_id
                      pre_binary_op: Mod
                        identifier: id
                        number: 10
                      post_binary_op: Mod
                    post_binary_op: Assign
                  post_function: mutate(2)
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
    fn mutate_functions() {
        let text = indoc! {r#"
            mutate(
                travel_time_ns = to_ns(tpep_dropoff_datetime - tpep_pickup_datetime),
                avg_distance_plus_std = mean(trip_distance_km) + sd(trip_distance_km),
                avg_speed_km_h = trip_distance_km / (travel_time_ns / 3.6e12)
            )
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: mutate(3)
                    pre_binary_op: Assign
                      identifier: travel_time_ns
                      pre_function: to_ns(1)
                        pre_binary_op: Minus
                          identifier: tpep_dropoff_datetime
                          identifier: tpep_pickup_datetime
                        post_binary_op: Minus
                      post_function: to_ns(1)
                    post_binary_op: Assign
                    pre_binary_op: Assign
                      identifier: avg_distance_plus_std
                      pre_binary_op: Plus
                        pre_function: mean(1)
                          identifier: trip_distance_km
                        post_function: mean(1)
                        pre_function: sd(1)
                          identifier: trip_distance_km
                        post_function: sd(1)
                      post_binary_op: Plus
                    post_binary_op: Assign
                    pre_binary_op: Assign
                      identifier: avg_speed_km_h
                      pre_binary_op: Divide
                        identifier: trip_distance_km
                        pre_binary_op: Divide
                          identifier: travel_time_ns
                          number: 3600000000000
                        post_binary_op: Divide
                      post_binary_op: Divide
                    post_binary_op: Assign
                  post_function: mutate(3)
                post_pipeline"#
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

        let text = indoc! {r#"
            mutate(a1 = (a2 + a3) / 2, a1 = a2 + a3 / 2)
        "#};

        assert_parser!(
            text,
            indoc!(
                r#"
                pre_pipeline
                  pre_function: mutate(2)
                    pre_binary_op: Assign
                      identifier: a1
                      pre_binary_op: Divide
                        pre_binary_op: Plus
                          identifier: a2
                          identifier: a3
                        post_binary_op: Plus
                        number: 2
                      post_binary_op: Divide
                    post_binary_op: Assign
                    pre_binary_op: Assign
                      identifier: a1
                      pre_binary_op: Plus
                        identifier: a2
                        pre_binary_op: Divide
                          identifier: a3
                          number: 2
                        post_binary_op: Divide
                      post_binary_op: Plus
                    post_binary_op: Assign
                  post_function: mutate(2)
                post_pipeline"#
            )
        );
    }
}
