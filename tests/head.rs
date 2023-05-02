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
fn head() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                fare_amount,
                total_amount) |
            head()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 5)
            ┌─────────────────┬───────────────┬──────────────┬─────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ fare_amount ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---         ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64         ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪═════════════╪══════════════╡
            │ 1               ┆ 3.14          ┆ Credit card  ┆ 14.5        ┆ 22.56        │
            │ 2               ┆ 1.06          ┆ Cash         ┆ 6.5         ┆ 9.8          │
            │ 1               ┆ 2.36          ┆ Credit card  ┆ 11.5        ┆ 17.76        │
            │ 1               ┆ 5.2           ┆ Credit card  ┆ 18.0        ┆ 26.16        │
            │ 3               ┆ 0.0           ┆ Credit card  ┆ 12.5        ┆ 19.55        │
            │ 1               ┆ 2.39          ┆ Cash         ┆ 19.0        ┆ 22.3         │
            │ 2               ┆ 1.52          ┆ Cash         ┆ 8.5         ┆ 11.8         │
            │ 1               ┆ 0.51          ┆ Credit card  ┆ 6.0         ┆ 11.3         │
            │ 1               ┆ 0.98          ┆ Credit card  ┆ 12.0        ┆ 19.56        │
            │ 2               ┆ 2.14          ┆ Credit card  ┆ 9.0         ┆ 15.36        │
            └─────────────────┴───────────────┴──────────────┴─────────────┴──────────────┘
      "#
        )
    );

    Ok(())
}

#[test]
fn head_with_limit() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                fare_amount,
                total_amount) |
            head(5)
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (5, 5)
            ┌─────────────────┬───────────────┬──────────────┬─────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ fare_amount ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---         ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64         ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪═════════════╪══════════════╡
            │ 1               ┆ 3.14          ┆ Credit card  ┆ 14.5        ┆ 22.56        │
            │ 2               ┆ 1.06          ┆ Cash         ┆ 6.5         ┆ 9.8          │
            │ 1               ┆ 2.36          ┆ Credit card  ┆ 11.5        ┆ 17.76        │
            │ 1               ┆ 5.2           ┆ Credit card  ┆ 18.0        ┆ 26.16        │
            │ 3               ┆ 0.0           ┆ Credit card  ┆ 12.5        ┆ 19.55        │
            └─────────────────┴───────────────┴──────────────┴─────────────┴──────────────┘
      "#
        )
    );

    Ok(())
}
