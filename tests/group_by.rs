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
use anyhow::Result;
use indoc::indoc;

use dply::interpreter;

#[test]
fn group_by_mean_sd_var() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            group_by(payment_type) |
            summarize(
                mean_price = mean(total_amount),
                std_price = sd(total_amount),
                var_price = var(total_amount),
                n = n()
            ) |
            arrange(desc(n)) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (5, 5)
            ┌──────────────┬────────────┬───────────┬────────────┬─────┐
            │ payment_type ┆ mean_price ┆ std_price ┆ var_price  ┆ n   │
            │ ---          ┆ ---        ┆ ---       ┆ ---        ┆ --- │
            │ str          ┆ f64        ┆ f64       ┆ f64        ┆ u32 │
            ╞══════════════╪════════════╪═══════════╪════════════╪═════╡
            │ Credit card  ┆ 22.378757  ┆ 16.095337 ┆ 259.059865 ┆ 185 │
            │ Cash         ┆ 18.458491  ┆ 12.545236 ┆ 157.382955 ┆ 53  │
            │ Unknown      ┆ 26.847778  ┆ 14.279152 ┆ 203.894169 ┆ 9   │
            │ Dispute      ┆ -0.5       ┆ 11.030866 ┆ 121.68     ┆ 2   │
            │ No charge    ┆ 8.8        ┆ 0.0       ┆ 0.0        ┆ 1   │
            └──────────────┴────────────┴───────────┴────────────┴─────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn group_by_min_max() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            group_by(payment_type) |
            summarize(
                min_price = min(total_amount),
                max_price = max(total_amount),
                n = n()
            ) |
            arrange(desc(n)) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (5, 4)
            ┌──────────────┬───────────┬───────────┬─────┐
            │ payment_type ┆ min_price ┆ max_price ┆ n   │
            │ ---          ┆ ---       ┆ ---       ┆ --- │
            │ str          ┆ f64       ┆ f64       ┆ u32 │
            ╞══════════════╪═══════════╪═══════════╪═════╡
            │ Credit card  ┆ 8.5       ┆ 84.36     ┆ 185 │
            │ Cash         ┆ 3.3       ┆ 63.1      ┆ 53  │
            │ Unknown      ┆ 9.96      ┆ 54.47     ┆ 9   │
            │ Dispute      ┆ -8.3      ┆ 7.3       ┆ 2   │
            │ No charge    ┆ 8.8       ┆ 8.8       ┆ 1   │
            └──────────────┴───────────┴───────────┴─────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn group_by_median_quantile() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            group_by(payment_type) |
            summarize(
                median_price = median(total_amount),
                q25_price = quantile(total_amount, .25),
                q50_price = quantile(total_amount, .50),
                q75_price = quantile(total_amount, .75),
                q95_price = quantile(total_amount, .95),
                n = n()
            ) |
            arrange(desc(n)) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (5, 7)
            ┌──────────────┬──────────────┬───────────┬───────────┬───────────┬───────────┬─────┐
            │ payment_type ┆ median_price ┆ q25_price ┆ q50_price ┆ q75_price ┆ q95_price ┆ n   │
            │ ---          ┆ ---          ┆ ---       ┆ ---       ┆ ---       ┆ ---       ┆ --- │
            │ str          ┆ f64          ┆ f64       ┆ f64       ┆ f64       ┆ f64       ┆ u32 │
            ╞══════════════╪══════════════╪═══════════╪═══════════╪═══════════╪═══════════╪═════╡
            │ Credit card  ┆ 16.56        ┆ 12.43     ┆ 16.56     ┆ 23.76     ┆ 64.114    ┆ 185 │
            │ Cash         ┆ 14.8         ┆ 11.8      ┆ 14.8      ┆ 22.3      ┆ 49.67     ┆ 53  │
            │ Unknown      ┆ 22.72        ┆ 18.17     ┆ 22.72     ┆ 28.39     ┆ 50.882    ┆ 9   │
            │ Dispute      ┆ -0.5         ┆ -4.4      ┆ -0.5      ┆ 3.4       ┆ 6.52      ┆ 2   │
            │ No charge    ┆ 8.8          ┆ 8.8       ┆ 8.8       ┆ 8.8       ┆ 8.8       ┆ 1   │
            └──────────────┴──────────────┴───────────┴───────────┴───────────┴───────────┴─────┘
       "#
        )
    );

    Ok(())
}