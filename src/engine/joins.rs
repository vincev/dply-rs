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
use anyhow::{bail, Result};
use datafusion::{
    common::{Column, JoinType as DFJoinType},
    logical_expr::LogicalPlanBuilder,
};
use std::collections::HashSet;

use crate::parser::{Expr, Operator};

use super::*;

/// Join type
pub enum JoinType {
    /// Anti left join.
    Anti,
    /// Cross join
    Cross,
    /// Inner join
    Inner,
    /// Left join
    Left,
    /// Outer join
    Outer,
}

const LHS_TABLE: &str = "lhs";
const RHS_TABLE: &str = "rhs";

/// Evaluates a join call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context, join_type: JoinType) -> Result<()> {
    if let Some(lhs_plan) = ctx.take_plan() {
        let rhs_df_name = args::identifier(&args[0]);
        if let Some(rhs_plan) = ctx.get_plan(&rhs_df_name) {
            // Assign table names to the left and right sides to avoid
            // collisions when tables have columns with the same name.
            let lhs_plan = LogicalPlanBuilder::from(lhs_plan)
                .alias(LHS_TABLE)?
                .build()?;

            let rhs_plan = LogicalPlanBuilder::from(rhs_plan)
                .alias(RHS_TABLE)?
                .build()?;

            let lhs_schema = lhs_plan.schema();
            let rhs_schema = rhs_plan.schema();

            let lhs_schema_cols = lhs_schema
                .fields()
                .iter()
                .map(|f| f.name().to_owned())
                .collect::<HashSet<_>>();

            let rhs_schema_cols = rhs_schema
                .fields()
                .iter()
                .map(|f| f.name().to_owned())
                .collect::<HashSet<_>>();

            // If no join columns are specified use common columns
            let (lhs_cols, rhs_cols) = if args.len() == 1 {
                let common_cols = lhs_schema_cols
                    .intersection(&rhs_schema_cols)
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                if common_cols.is_empty() {
                    bail!("join error: Missing join columns for '{rhs_df_name}'");
                }
                common_cols
                    .into_iter()
                    .map(|s| {
                        (
                            Column::new(Some(LHS_TABLE), s.clone()),
                            Column::new(Some(RHS_TABLE), s),
                        )
                    })
                    .unzip()
            } else {
                let mut lhs_cols = Vec::with_capacity(args.len());
                let mut rhs_cols = Vec::with_capacity(args.len());

                for arg in args.iter().skip(1) {
                    if let Expr::BinaryOp(lhs, Operator::Eq, rhs) = arg {
                        let lhs_col = args::identifier(lhs);
                        if !lhs_schema_cols.contains(&lhs_col) {
                            bail!("join error: Unknown column '{lhs_col}'");
                        }
                        lhs_cols.push(Column::new(Some(LHS_TABLE), lhs_col.clone()));

                        let rhs_col = args::identifier(rhs);
                        if !rhs_schema_cols.contains(&rhs_col) {
                            bail!("join error: Unknown column '{rhs_col}'");
                        }
                        rhs_cols.push(Column::new(Some(RHS_TABLE), rhs_col.clone()));

                        let lhs_type = lhs_schema
                            .field_with_unqualified_name(&lhs_col)
                            .map(|f| f.data_type());

                        let rhs_type = rhs_schema
                            .field_with_unqualified_name(&rhs_col)
                            .map(|f| f.data_type());

                        let have_same_type = lhs_type
                            .and_then(|lt| rhs_type.map(|rt| lt == rt))
                            .unwrap_or(false);
                        if !have_same_type {
                            bail!(
                                "join error: '{lhs_col}' and '{rhs_col}' don't have the same type"
                            );
                        }
                    }
                }

                (lhs_cols, rhs_cols)
            };

            let plan = if let JoinType::Cross = join_type {
                LogicalPlanBuilder::from(lhs_plan)
                    .cross_join(rhs_plan)?
                    .build()?
            } else {
                let join_type = match join_type {
                    JoinType::Inner => DFJoinType::Inner,
                    JoinType::Left => DFJoinType::Left,
                    JoinType::Anti => DFJoinType::LeftAnti,
                    _ => DFJoinType::Full,
                };

                LogicalPlanBuilder::from(lhs_plan)
                    .join(rhs_plan, join_type, (lhs_cols, rhs_cols.clone()), None)?
                    .build()?
            };

            // Remove righ table columns for inner and left join.
            let plan = match join_type {
                JoinType::Inner | JoinType::Left => remove_rhs_columns(plan, rhs_cols)?,
                _ => plan,
            };

            let plan = rename_duplicate_columns(plan)?;

            ctx.set_plan(plan);
        } else {
            bail!("join error: undefined dataframe variable '{rhs_df_name}'");
        }
    } else if ctx.is_grouping() {
        bail!("join error: must call summarize after a group_by");
    } else {
        bail!("join error: missing input dataframe");
    }

    Ok(())
}

fn remove_rhs_columns(plan: LogicalPlan, rhs_cols: Vec<Column>) -> Result<LogicalPlan> {
    let columns = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.qualified_column())
        .filter(|c| !rhs_cols.contains(c))
        .map(DFExpr::Column)
        .collect::<Vec<_>>();
    let plan = LogicalPlanBuilder::from(plan).project(columns)?.build()?;
    Ok(plan)
}

fn rename_duplicate_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut duplicates = HashSet::new();
    let mut found = HashSet::new();

    for field in plan.schema().fields() {
        if found.contains(field.name()) {
            duplicates.insert(field.name().to_owned());
        } else {
            found.insert(field.name().to_owned());
        }
    }

    let columns = plan
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let column = f.qualified_column();
            let is_rhs = column
                .relation
                .as_ref()
                .map(|r| r.table() == RHS_TABLE)
                .unwrap_or(false);
            let expr = DFExpr::Column(column.clone());
            if is_rhs && duplicates.contains(f.name()) {
                expr.alias(format!("{}_rhs", column.name))
            } else {
                expr
            }
        })
        .collect::<Vec<_>>();

    let plan = LogicalPlanBuilder::from(plan).project(columns)?.build()?;
    Ok(plan)
}
