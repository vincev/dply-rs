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
fn glimpse_parquet() -> Result<()> {
    let input = r#"parquet("tests/data/nyctaxi.parquet") | glimpse()"#;
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            Rows: 250
            Columns: 19
            +-----------------------+--------------+-----------------------------------------+
            | VendorID              | i64          | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, ... |
            | tpep_pickup_datetime  | datetime[ns] | 2022-11-22 19:27:01, 2022-11-27...      |
            | tpep_dropoff_datetime | datetime[ns] | 2022-11-22 19:45:53, 2022-11-27...      |
            | passenger_count       | i64          | 1, 2, 1, 1, 3, 1, 2, 1, 1, 2, 2, 1, ... |
            | trip_distance         | f64          | 3.14, 1.06, 2.36, 5.2, 0.0, 2.39, 1.... |
            | rate_code             | str          | "Standard", "Standard", "Standard",...  |
            | store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "N", "... |
            | PULocationID          | i64          | 234, 48, 142, 79, 237, 137, 107, 229... |
            | DOLocationID          | i64          | 141, 142, 236, 75, 230, 140, 162, 16... |
            | payment_type          | str          | "Credit card", "Cash", "Credit card"... |
            | fare_amount           | f64          | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8... |
            | extra                 | f64          | 1.0, 0.0, 0.0, 0.5, 3.0, 0.0, 0.0, 0... |
            | mta_tax               | f64          | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0... |
            | tip_amount            | f64          | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.... |
            | tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0... |
            | improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0... |
            | total_amount          | f64          | 22.56, 9.8, 17.76, 26.16, 19.55, 22.... |
            | congestion_surcharge  | f64          | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2... |
            | airport_fee           | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0... |
            +-----------------------+--------------+-----------------------------------------+
        "#
        )
    );

    Ok(())
}

#[test]
fn glimpse_csv() -> Result<()> {
    let input = r#"csv("tests/data/nyctaxi.csv") | glimpse()"#;
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            Rows: 250
            Columns: 19
            +-----------------------+--------+-----------------------------------------------+
            | VendorID              | i64    | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 2, ... |
            | tpep_pickup_datetime  | str    | "2022-11-22T19:27:01.000000000",...           |
            | tpep_dropoff_datetime | str    | "2022-11-22T19:45:53.000000000",...           |
            | passenger_count       | i64    | 1, 2, 1, 1, 3, 1, 2, 1, 1, 2, 2, 1, 1, 1, ... |
            | trip_distance         | f64    | 3.14, 1.06, 2.36, 5.2, 0.0, 2.39, 1.52, 0.... |
            | rate_code             | str    | "Standard", "Standard", "Standard",...        |
            | store_and_fwd_flag    | str    | "N", "N", "N", "N", "N", "N", "N", "N", "N... |
            | PULocationID          | i64    | 234, 48, 142, 79, 237, 137, 107, 229, 162,... |
            | DOLocationID          | i64    | 141, 142, 236, 75, 230, 140, 162, 161, 186... |
            | payment_type          | str    | "Credit card", "Cash", "Credit card", "Cre... |
            | fare_amount           | f64    | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8.5, 6.... |
            | extra                 | f64    | 1.0, 0.0, 0.0, 0.5, 3.0, 0.0, 0.0, 0.0, 1.... |
            | mta_tax               | f64    | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.... |
            | tip_amount            | f64    | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.0, 2.0... |
            | tolls_amount          | f64    | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
            | improvement_surcharge | f64    | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.... |
            | total_amount          | f64    | 22.56, 9.8, 17.76, 26.16, 19.55, 22.3, 11.... |
            | congestion_surcharge  | f64    | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.... |
            | airport_fee           | f64    | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
            +-----------------------+--------+-----------------------------------------------+
        "#
        )
    );

    Ok(())
}
