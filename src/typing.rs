// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0

//! Checks pipeline functions and arguments types.
use anyhow::{anyhow, bail, Result};

use crate::parser::{Expr, Operator};
use crate::signatures::{self, ArgType, Args};

/// Checks pipeline functions and arguments types.
pub fn validate(exprs: &[Expr]) -> Result<()> {
    for expr in exprs {
        if let Expr::Pipeline(exprs) = expr {
            for expr in exprs {
                check_signature(expr)?;
            }
        }
    }

    Ok(())
}

fn check_signature(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Function(name, expr_args) => {
            let sigs = signatures::functions();
            if let Some(sig_args) = sigs.get(name.as_str()) {
                check_args(name, expr_args, sig_args)
            } else {
                Err(anyhow!("Unknown function: {name}"))
            }
        }
        Expr::Identifier(_) => Ok(()),
        _ => Err(anyhow!("Unexpected expression {expr}")),
    }
}

fn check_args(name: &str, exprs: &[Expr], sig_args: &Args) -> Result<()> {
    match sig_args {
        signatures::Args::None => {
            if !exprs.is_empty() {
                bail!("Unexpected argument for function '{name}'");
            }
        }
        signatures::Args::NoneOrOne(arg) => match exprs.len() {
            0 => return Ok(()),
            1 => check_arg(name, &exprs[0], arg)?,
            _ => bail!("Too many arguments for function '{name}'"),
        },
        signatures::Args::ZeroOrMore(arg) => {
            for expr in exprs {
                check_arg(name, expr, arg)?;
            }
        }
        signatures::Args::OneOrMore(arg) => {
            if exprs.is_empty() {
                bail!("Missing arguments for function '{name}'");
            }

            for expr in exprs {
                check_arg(name, expr, arg)?;
            }
        }
        signatures::Args::OneThenMore(first, rest) => {
            if exprs.is_empty() {
                bail!("Missing argument for function '{name}'");
            }

            check_arg(name, &exprs[0], first)?;

            for expr in &exprs[1..] {
                check_arg(name, expr, rest)?;
            }
        }
        signatures::Args::Ordered(args) => {
            if exprs.len() < args.len() {
                bail!("Missing arguments for function '{name}'");
            }

            if exprs.len() > args.len() {
                bail!("Too many arguments for function '{name}'");
            }

            for (expr, arg) in exprs.iter().zip(args.iter()) {
                check_arg(name, expr, arg)?;
            }
        }
    };

    Ok(())
}

fn check_arg(fname: &str, expr: &Expr, arg: &ArgType) -> Result<()> {
    match arg {
        ArgType::Arith(arg) => check_arith(fname, expr, arg),
        ArgType::Assign(lhs, rhs) => check_assign(fname, expr, lhs, rhs),
        ArgType::Bool => check_bool(fname, expr),
        ArgType::Compare(lhs, rhs) => check_compare(fname, expr, lhs, rhs),
        ArgType::Eq(lhs, rhs) => check_equal(fname, expr, lhs, rhs),
        ArgType::Function(name, args) => check_function(name, expr, args),
        ArgType::Identifier => check_identifier(fname, expr),
        ArgType::Logical(arg) => check_logical(fname, expr, arg),
        ArgType::Named(name) => check_named(fname, name, expr),
        ArgType::Negate(arg) => check_negate(fname, expr, arg),
        ArgType::Number => check_number(fname, expr),
        ArgType::OneOf(args) => check_one_of(fname, expr, args),
        ArgType::String => check_string(fname, expr),
    }
}

fn check_arith(fname: &str, expr: &Expr, arg: &ArgType) -> Result<()> {
    fn is_arith(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp(_, Operator::Plus, _)
                | Expr::BinaryOp(_, Operator::Minus, _)
                | Expr::BinaryOp(_, Operator::Divide, _)
                | Expr::BinaryOp(_, Operator::Multiply, _)
                | Expr::BinaryOp(_, Operator::Mod, _)
        )
    }

    match expr {
        Expr::BinaryOp(lhs, Operator::Plus, rhs)
        | Expr::BinaryOp(lhs, Operator::Minus, rhs)
        | Expr::BinaryOp(lhs, Operator::Divide, rhs)
        | Expr::BinaryOp(lhs, Operator::Multiply, rhs)
        | Expr::BinaryOp(lhs, Operator::Mod, rhs) => {
            if is_arith(lhs) {
                check_arith(fname, lhs, arg)?;
            } else {
                check_arg(fname, lhs, arg)?;
            }

            if is_arith(rhs) {
                check_arith(fname, rhs, arg)
            } else {
                check_arg(fname, rhs, arg)
            }
        }
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_assign(fname: &str, expr: &Expr, larg: &ArgType, rarg: &ArgType) -> Result<()> {
    match expr {
        Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
            check_arg(fname, lhs, larg)?;
            check_arg(fname, rhs, rarg)
        }
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_bool(fname: &str, expr: &Expr) -> Result<()> {
    match expr {
        Expr::Identifier(s) if s == "true" => Ok(()),
        Expr::Identifier(s) if s == "false" => Ok(()),
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_compare(fname: &str, expr: &Expr, larg: &ArgType, rarg: &ArgType) -> Result<()> {
    match expr {
        Expr::BinaryOp(lhs, Operator::Eq, rhs)
        | Expr::BinaryOp(lhs, Operator::NotEq, rhs)
        | Expr::BinaryOp(lhs, Operator::Lt, rhs)
        | Expr::BinaryOp(lhs, Operator::LtEq, rhs)
        | Expr::BinaryOp(lhs, Operator::Gt, rhs)
        | Expr::BinaryOp(lhs, Operator::GtEq, rhs) => {
            check_arg(fname, lhs, larg)?;
            check_arg(fname, rhs, rarg)
        }
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_equal(fname: &str, expr: &Expr, larg: &ArgType, rarg: &ArgType) -> Result<()> {
    match expr {
        Expr::BinaryOp(lhs, Operator::Eq, rhs) => {
            check_arg(fname, lhs, larg)?;
            check_arg(fname, rhs, rarg)
        }
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_function(fname: &str, expr: &Expr, sig_args: &Args) -> Result<()> {
    match expr {
        Expr::Function(name, args) if fname == name => check_args(name, args, sig_args),
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_identifier(fname: &str, expr: &Expr) -> Result<()> {
    if !matches!(expr, Expr::Identifier(_)) {
        Err(anyhow!("Invalid argument '{expr}' for function '{fname}'"))
    } else {
        Ok(())
    }
}

fn check_logical(fname: &str, expr: &Expr, arg: &ArgType) -> Result<()> {
    fn is_logical(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::BinaryOp(_, Operator::And, _) | Expr::BinaryOp(_, Operator::Or, _)
        )
    }

    match expr {
        Expr::BinaryOp(lhs, Operator::And, rhs) | Expr::BinaryOp(lhs, Operator::Or, rhs) => {
            if is_logical(lhs) {
                check_logical(fname, lhs, arg)?;
            } else {
                check_arg(fname, lhs, arg)?;
            }

            if is_logical(rhs) {
                check_logical(fname, rhs, arg)
            } else {
                check_arg(fname, rhs, arg)
            }
        }
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_named(fname: &str, name: &str, expr: &Expr) -> Result<()> {
    match expr {
        Expr::Identifier(s) if s == name => Ok(()),
        _ => Err(anyhow!("Invalid argument '{expr}' for function '{fname}'")),
    }
}

fn check_negate(fname: &str, expr: &Expr, arg: &ArgType) -> Result<()> {
    if let Expr::UnaryOp(Operator::Not, expr) = expr {
        check_arg(fname, expr, arg)
    } else {
        Err(anyhow!("Invalid argument '{expr}' for function '{fname}'"))
    }
}

fn check_number(fname: &str, expr: &Expr) -> Result<()> {
    if !matches!(expr, Expr::Number(_)) {
        Err(anyhow!("Invalid argument '{expr}' for function '{fname}'"))
    } else {
        Ok(())
    }
}

fn check_one_of(fname: &str, expr: &Expr, args: &[ArgType]) -> Result<()> {
    for arg in args {
        if check_arg(fname, expr, arg).is_ok() {
            return Ok(());
        }
    }

    Err(anyhow!("Invalid argument '{expr}' for function '{fname}'"))
}

fn check_string(fname: &str, expr: &Expr) -> Result<()> {
    if !matches!(expr, Expr::String(_)) {
        Err(anyhow!("Invalid argument '{expr}' for function '{fname}'"))
    } else {
        Ok(())
    }
}
