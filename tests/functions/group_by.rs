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

use super::assert_interpreter;

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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 5)
            payment_type|mean_price|std_price|var_price|n
            str|f64|f64|f64|i64
            ---
            Credit card|22.378757|16.095337|259.059865|185
            Cash|18.458491|12.545236|157.382955|53
            Unknown|26.847778|14.279152|203.894169|9
            Dispute|-0.5|11.030866|121.68|2
            No charge|8.8|null|null|1
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            payment_type|min_price|max_price|n
            str|f64|f64|i64
            ---
            Credit card|8.5|84.36|185
            Cash|3.3|63.1|53
            Unknown|9.96|54.47|9
            Dispute|-8.3|7.3|2
            No charge|8.8|8.8|1
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 7)
            payment_type|median_price|q25_price|q50_price|q75_price|q95_price|n
            str|f64|f64|f64|f64|f64|i64
            ---
            Credit card|16.56|12.43|16.56|23.76|56.09|185
            Cash|14.8|11.8|14.8|22.3|41.55|53
            Unknown|22.72|18.17|22.72|28.39|45.5|9
            Dispute|-0.5|-8.3|-8.3|-8.3|-8.3|2
            No charge|8.8|8.8|8.8|8.8|8.8|1
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn summarize_median_quantile() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            filter(shape_id <= 100) |
            summarize(
                median = median(shape_id),
                q25 = quantile(shape_id, .25),
                q50 = quantile(shape_id, .50),
                q75 = quantile(shape_id, .75),
                q95 = quantile(shape_id, .95),
                n = n()
            ) |
            arrange(desc(n)) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 6)
            median|q25|q50|q75|q95|n
            u32|u32|u32|u32|u32|i64
            ---
            50|25|50|75|95|100
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn group_by_list() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(payment_type, contains("amount")) |
            filter(total_amount < 8.5) |
            group_by(payment_type) |
            summarize(
                amounts = list(total_amount),
                fares = list(fare_amount)
            ) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (2, 3)
            payment_type|amounts|fares
            str|list[f64]|list[f64]
            ---
            Cash|[3.3, 7.8, 8.3]|[2.5, 7.0, 5.0]
            Dispute|[7.3, -8.3]|[4.0, -4.5]
            ---
       "#
        )
    );

    // Test inverse
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(payment_type, contains("amount")) |
            filter(total_amount < 8.5) |
            group_by(payment_type) |
            summarize(
                amounts = list(total_amount),
                fares = list(fare_amount)
            ) |
            unnest(amounts, fares) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (13, 3)
            payment_type|amounts|fares
            str|f64|f64
            ---
            Cash|3.3|2.5
            Cash|3.3|7.0
            Cash|3.3|5.0
            Cash|7.8|2.5
            Cash|7.8|7.0
            Cash|7.8|5.0
            Cash|8.3|2.5
            Cash|8.3|7.0
            Cash|8.3|5.0
            Dispute|7.3|4.0
            Dispute|7.3|-4.5
            Dispute|-8.3|4.0
            Dispute|-8.3|-4.5
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn summarize_list() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(payment_type, contains("amount")) |
            filter(total_amount < 8.5, fare_amount > 0 & fare_amount < 6.0) |
            summarize(
                amounts = list(total_amount),
                fares = list(fare_amount),
                n = n()
            ) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 3)
            amounts|fares|n
            list[f64]|list[f64]|i64
            ---
            [3.3, 7.3, 8.3]|[2.5, 4.0, 5.0]|3
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(payment_type, contains("amount")) |
            filter(total_amount < 8.5, fare_amount > 0 & fare_amount < 6.0) |
            summarize(
                amounts = list(total_amount),
                fares = list(fare_amount),
                n = n()
            ) |
            unnest(amounts, fares) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (9, 3)
            amounts|fares|n
            f64|f64|i64
            ---
            3.3|2.5|3
            3.3|4.0|3
            3.3|5.0|3
            7.3|2.5|3
            7.3|4.0|3
            7.3|5.0|3
            8.3|2.5|3
            8.3|4.0|3
            8.3|5.0|3
            ---
       "#
        )
    );

    Ok(())
}
