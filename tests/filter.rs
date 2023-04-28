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
fn filter_lt() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(total_amount < 8.8) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (7, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 1               ┆ 0.0           ┆ Cash         ┆ 3.3          │
            │ 1               ┆ 0.43          ┆ Dispute      ┆ 7.3          │
            │ 1               ┆ 0.42          ┆ Credit card  ┆ 8.5          │
            │ 1               ┆ 0.49          ┆ Credit card  ┆ 8.76         │
            │ 4               ┆ 1.24          ┆ Cash         ┆ 7.8          │
            │ 6               ┆ 0.9           ┆ Cash         ┆ 8.3          │
            │ 2               ┆ 0.8           ┆ Dispute      ┆ -8.3         │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_lte() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(total_amount <= 8.8) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (11, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 1               ┆ 0.0           ┆ Cash         ┆ 3.3          │
            │ 1               ┆ 0.43          ┆ Dispute      ┆ 7.3          │
            │ 1               ┆ 0.42          ┆ Credit card  ┆ 8.5          │
            │ 1               ┆ 1.1           ┆ No charge    ┆ 8.8          │
            │ 1               ┆ 0.49          ┆ Credit card  ┆ 8.76         │
            │ 4               ┆ 1.24          ┆ Cash         ┆ 7.8          │
            │ 1               ┆ 1.06          ┆ Credit card  ┆ 8.8          │
            │ 1               ┆ 1.18          ┆ Cash         ┆ 8.8          │
            │ 6               ┆ 0.9           ┆ Cash         ┆ 8.3          │
            │ 5               ┆ 0.74          ┆ Cash         ┆ 8.8          │
            │ 2               ┆ 0.8           ┆ Dispute      ┆ -8.3         │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_gt() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(total_amount > 74.22) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (3, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 1               ┆ 19.55         ┆ Credit card  ┆ 77.6         │
            │ 1               ┆ 0.04          ┆ Credit card  ┆ 84.36        │
            │ 2               ┆ 16.36         ┆ Credit card  ┆ 77.64        │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_gte() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(total_amount >= 74.22) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (5, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 1               ┆ 19.55         ┆ Credit card  ┆ 77.6         │
            │ 1               ┆ 17.79         ┆ Credit card  ┆ 74.22        │
            │ 1               ┆ 0.04          ┆ Credit card  ┆ 84.36        │
            │ 2               ┆ 16.36         ┆ Credit card  ┆ 77.64        │
            │ 1               ┆ 16.63         ┆ Credit card  ┆ 74.22        │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_eq() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(passenger_count == 5) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 5               ┆ 0.48          ┆ Credit card  ┆ 9.13         │
            │ 5               ┆ 4.67          ┆ Cash         ┆ 21.3         │
            │ 5               ┆ 0.8           ┆ Credit card  ┆ 10.56        │
            │ 5               ┆ 1.04          ┆ Credit card  ┆ 11.76        │
            │ 5               ┆ 3.61          ┆ Credit card  ┆ 23.76        │
            │ 5               ┆ 0.55          ┆ Credit card  ┆ 11.76        │
            │ 5               ┆ 1.88          ┆ Credit card  ┆ 12.05        │
            │ 5               ┆ 1.09          ┆ Credit card  ┆ 14.04        │
            │ 5               ┆ 0.74          ┆ Cash         ┆ 8.8          │
            │ 5               ┆ 17.24         ┆ Credit card  ┆ 66.36        │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_and() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(payment_type != "Credit card" & passenger_count == 2) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (8, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 2               ┆ 1.06          ┆ Cash         ┆ 9.8          │
            │ 2               ┆ 1.52          ┆ Cash         ┆ 11.8         │
            │ 2               ┆ 2.88          ┆ Cash         ┆ 16.3         │
            │ 2               ┆ 4.55          ┆ Cash         ┆ 19.8         │
            │ 2               ┆ 1.51          ┆ Cash         ┆ 13.3         │
            │ 2               ┆ 1.3           ┆ Cash         ┆ 10.3         │
            │ 2               ┆ 0.8           ┆ Dispute      ┆ -8.3         │
            │ 2               ┆ 2.5           ┆ Cash         ┆ 11.05        │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_or() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(trip_distance == 3.6 | total_amount == 16.3) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (6, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 2               ┆ 2.88          ┆ Cash         ┆ 16.3         │
            │ 1               ┆ 3.6           ┆ Credit card  ┆ 24.5         │
            │ 2               ┆ 0.0           ┆ Credit card  ┆ 16.3         │
            │ 1               ┆ 1.5           ┆ Credit card  ┆ 16.3         │
            │ 1               ┆ 3.6           ┆ Credit card  ┆ 27.96        │
            │ 1               ┆ 2.24          ┆ Cash         ┆ 16.3         │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_with_parenthesis() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter(trip_distance == 3.6 | total_amount == 16.3 & payment_type == "Cash") |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (4, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 2               ┆ 2.88          ┆ Cash         ┆ 16.3         │
            │ 1               ┆ 3.6           ┆ Credit card  ┆ 24.5         │
            │ 1               ┆ 3.6           ┆ Credit card  ┆ 27.96        │
            │ 1               ┆ 2.24          ┆ Cash         ┆ 16.3         │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                total_amount) |
            filter((trip_distance == 2.63 | total_amount == 9.8) & payment_type == "Cash") |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (4, 4)
            ┌─────────────────┬───────────────┬──────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪══════════════╡
            │ 2               ┆ 1.06          ┆ Cash         ┆ 9.8          │
            │ 1               ┆ 2.63          ┆ Cash         ┆ 14.3         │
            │ 1               ┆ 1.18          ┆ Cash         ┆ 9.8          │
            │ 1               ┆ 1.2           ┆ Cash         ┆ 9.8          │
            └─────────────────┴───────────────┴──────────────┴──────────────┘
       "#
        )
    );

    Ok(())
}
