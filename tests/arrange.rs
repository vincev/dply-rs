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
fn arrange() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(passenger_count, total_amount) |
            arrange(desc(passenger_count), total_amount) |
            head()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 2)
            ┌─────────────────┬──────────────┐
            │ passenger_count ┆ total_amount │
            │ ---             ┆ ---          │
            │ i64             ┆ f64          │
            ╞═════════════════╪══════════════╡
            │ 6               ┆ 8.3          │
            │ 5               ┆ 8.8          │
            │ 5               ┆ 9.13         │
            │ 5               ┆ 10.56        │
            │ 5               ┆ 11.76        │
            │ 5               ┆ 11.76        │
            │ 5               ┆ 12.05        │
            │ 5               ┆ 14.04        │
            │ 5               ┆ 21.3         │
            │ 5               ┆ 23.76        │
            └─────────────────┴──────────────┘
        "#
        )
    );

    Ok(())
}

#[test]
fn arrange_desc() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(passenger_count, total_amount) |
            arrange(passenger_count, desc(total_amount)) |
            head()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 2)
            ┌─────────────────┬──────────────┐
            │ passenger_count ┆ total_amount │
            │ ---             ┆ ---          │
            │ i64             ┆ f64          │
            ╞═════════════════╪══════════════╡
            │ 0               ┆ 54.35        │
            │ 1               ┆ 84.36        │
            │ 1               ┆ 77.6         │
            │ 1               ┆ 74.22        │
            │ 1               ┆ 74.22        │
            │ 1               ┆ 74.2         │
            │ 1               ┆ 74.2         │
            │ 1               ┆ 70.69        │
            │ 1               ┆ 66.12        │
            │ 1               ┆ 63.1         │
            └─────────────────┴──────────────┘
        "#
        )
    );

    Ok(())
}
