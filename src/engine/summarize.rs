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
use anyhow::{anyhow, bail, Result};
use datafusion::{
    arrow::{
        array::ArrayRef,
        compute::{self, SortOptions},
        datatypes::DataType,
    },
    common::DFSchema,
    error::{DataFusionError, Result as DFResult},
    logical_expr::{
        aggregate_function::AggregateFunction, create_udaf, expr, expr_fn, lit, Accumulator,
        Expr as DFExpr, LogicalPlanBuilder, Volatility,
    },
    scalar::ScalarValue,
};
use hashbrown::{HashMap, HashSet};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

use crate::parser::{Expr, Operator};

use super::*;

/// Evaluates a summarize call.
///
/// Parameters are checked before evaluation by the typing module.
pub fn eval(args: &[Expr], ctx: &mut Context) -> Result<()> {
    if let Some(plan) = ctx.take_plan() {
        let schema = plan.schema();

        let exprs = eval_args(args, schema).map_err(|e| anyhow!("summarize error: {e}"))?;

        let group = ctx.take_group().unwrap_or_default();

        let plan = LogicalPlanBuilder::from(plan)
            .aggregate(group, exprs)?
            .build()?;

        ctx.set_plan(plan);
    } else {
        bail!("summarize error: missing input group or dataframe");
    }

    Ok(())
}

fn eval_args(args: &[Expr], schema: &DFSchema) -> Result<Vec<DFExpr>> {
    let mut aliases = HashSet::new();
    let mut columns = Vec::new();

    for arg in args {
        match arg {
            Expr::BinaryOp(lhs, Operator::Assign, rhs) => {
                let alias = args::identifier(lhs);
                if aliases.contains(&alias) {
                    bail!("summarize error: duplicate alias {alias}");
                }

                aliases.insert(alias.clone());

                let column = match rhs.as_ref() {
                    Expr::Function(name, _) if name == "n" => Ok(expr_fn::count(lit(1))),
                    Expr::Function(name, args) if name == "list" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::array_agg)
                    }
                    Expr::Function(name, args) if name == "max" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::max)
                    }
                    Expr::Function(name, args) if name == "mean" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::avg)
                    }
                    Expr::Function(name, args) if name == "median" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::median)
                    }
                    Expr::Function(name, args) if name == "min" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::min)
                    }
                    Expr::Function(name, args) if name == "quantile" => {
                        let column = args::identifier(&args[0]);
                        let pct = args::number(&args[1]);

                        if (0.0..=1.0).contains(&pct) {
                            let dt = schema
                                .field_with_unqualified_name(&column)
                                .map(|f| f.data_type())
                                .map_err(|_| anyhow!("quantile: unknown column {column}"))?;
                            Ok(quantile(args::str_to_col(column), pct, dt))
                        } else {
                            Err(anyhow!("quantile: Quantile value must [0, 1]"))
                        }
                    }
                    Expr::Function(name, args) if name == "sd" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::stddev)
                    }
                    Expr::Function(name, args) if name == "sum" => {
                        args::expr_to_col(&args[0], schema).map(expr_fn::sum)
                    }
                    Expr::Function(name, args) if name == "var" => {
                        args::expr_to_col(&args[0], schema).map(var)
                    }
                    _ => panic!("Unexpected summarize expression {rhs}"),
                }?;

                columns.push(column.alias(&alias));
            }
            _ => panic!("Unexpected summarize expression: {arg}"),
        }
    }

    Ok(columns)
}

fn var(expr: DFExpr) -> DFExpr {
    DFExpr::AggregateFunction(expr::AggregateFunction::new(
        AggregateFunction::Variance,
        vec![expr],
        false,
        None,
        None,
    ))
}

// This function implement an exact quantile as DataFusion only provide only
// approximate quantile.
fn quantile(expr: DFExpr, quantile: f64, data_type: &DataType) -> DFExpr {
    static LAST_CALL: AtomicU64 = AtomicU64::new(0);

    // We need to give a different name to each udaf to make sure that different
    // calls to quantile produce different results.
    let quantile = create_udaf(
        &format!("quantile-{}", LAST_CALL.fetch_add(1, Ordering::Relaxed)),
        vec![data_type.clone()],
        Arc::new(data_type.clone()),
        Volatility::Immutable,
        Arc::new(move |dt| Ok(Box::new(Quantile::new(dt, quantile)))),
        Arc::new(vec![DataType::UInt64]),
    );

    quantile.call(vec![expr])
}

type QuantileStates = Arc<Mutex<HashMap<u64, Vec<ArrayRef>>>>;

#[derive(Debug)]
struct Quantile {
    quantile: f64,
    state_id: u64,
    shared_states: QuantileStates,
    data_type: DataType,
    arrays: Vec<ArrayRef>,
}

impl Quantile {
    fn new(data_type: &DataType, quantile: f64) -> Self {
        static LAST_STATE_ID: AtomicU64 = AtomicU64::new(0);
        static STATES_HASH: OnceLock<QuantileStates> = OnceLock::new();

        let hash = STATES_HASH.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

        let state_id = LAST_STATE_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            quantile,
            state_id,
            shared_states: hash.clone(),
            data_type: data_type.clone(),
            arrays: Default::default(),
        }
    }
}

impl Accumulator for Quantile {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        let mut states = self.shared_states.lock();
        states.insert(self.state_id, self.arrays.clone());

        Ok(vec![ScalarValue::UInt64(Some(self.state_id))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        assert_eq!(values.len(), 1);
        let array = &values[0];

        assert_eq!(array.data_type(), &self.data_type);
        self.arrays.push(array.clone());

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        assert_eq!(states.len(), 1);

        let array = &states[0];
        assert!(matches!(array.data_type(), DataType::UInt64));
        for index in 0..array.len() {
            match ScalarValue::try_from_array(array, index)? {
                ScalarValue::UInt64(Some(id)) => {
                    let mut states = self.shared_states.lock();
                    if let Some(arrays) = states.remove(&id) {
                        self.arrays.extend(arrays);
                    } else {
                        // If this happens something is broken.
                        panic!("No state found for id {id}");
                    }
                }
                ScalarValue::UInt64(_) => {}
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "Unexpected state in quantile aggregator: {v:?}"
                    )))
                }
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        let arrays = self.arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        let values = compute::concat(&arrays)?;
        let length = values.len() - values.null_count();

        if length == 0 {
            return ScalarValue::try_from(values.data_type());
        }

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let idx = ((length - 1) as f64 * self.quantile).floor() as usize;
        let limit = (idx + 1).min(length);
        let sorted = compute::sort_limit(&values, Some(options), Some(limit))?;
        ScalarValue::try_from_array(&sorted, idx)
    }

    fn size(&self) -> usize {
        let arrays_size: usize = self.arrays.iter().map(|a| a.len()).sum();

        std::mem::size_of_val(self) + arrays_size + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}
