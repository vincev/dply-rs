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
            arrange(desc(passenger_count), total_amount) |
            glimpse()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            Rows: 250
            Columns: 19
            +-----------------------+--------------+-----------------------------------------+
            | VendorID              | i64          | 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, ... |
            | tpep_pickup_datetime  | datetime[ns] | 2022-11-24 14:17:43, 2022-11-18...      |
            | tpep_dropoff_datetime | datetime[ns] | 2022-11-24 14:22:12, 2022-11-18...      |
            | passenger_count       | i64          | 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, ... |
            | trip_distance         | f64          | 0.9, 0.74, 0.48, 0.8, 1.04, 0.55, 1.... |
            | rate_code             | str          | "Standard", "Standard", "Standard",...  |
            | store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "N", "... |
            | PULocationID          | i64          | 230, 238, 237, 237, 239, 162, 236, 1... |
            | DOLocationID          | i64          | 161, 151, 237, 236, 142, 162, 161, 7... |
            | payment_type          | str          | "Cash", "Cash", "Credit card", "Cred... |
            | fare_amount           | f64          | 5.0, 5.5, 5.0, 5.5, 6.5, 6.5, 7.0, 7... |
            | extra                 | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0... |
            | mta_tax               | f64          | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0... |
            | tip_amount            | f64          | 0.0, 0.0, 0.83, 1.76, 1.96, 1.96, 1.... |
            | tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0... |
            | improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0... |
            | total_amount          | f64          | 8.3, 8.8, 9.13, 10.56, 11.76, 11.76,... |
            | congestion_surcharge  | f64          | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2... |
            | airport_fee           | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0... |
            +-----------------------+--------------+-----------------------------------------+
        "#
        )
    );

    Ok(())
}

#[test]
fn arrange_desc() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            arrange(passenger_count, desc(total_amount)) |
            glimpse()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            Rows: 250
            Columns: 19
            +-----------------------+--------------+-----------------------------------------+
            | VendorID              | i64          | 1, 2, 2, 2, 2, 1, 1, 2, 2, 2, 2, 1, ... |
            | tpep_pickup_datetime  | datetime[ns] | 2022-11-03 19:39:20, 2022-11-06...      |
            | tpep_dropoff_datetime | datetime[ns] | 2022-11-03 20:14:02, 2022-11-06...      |
            | passenger_count       | i64          | 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ... |
            | trip_distance         | f64          | 10.5, 0.04, 19.55, 16.63, 17.79, 17.... |
            | rate_code             | str          | "Standard", "Negotiated", "JFK", "JF... |
            | store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "Y", "... |
            | PULocationID          | i64          | 163, 132, 132, 68, 161, 233, 262, 13... |
            | DOLocationID          | i64          | 257, 132, 262, 132, 132, 132, 132, 1... |
            | payment_type          | str          | "Credit card", "Credit card", "Credi... |
            | fare_amount           | f64          | 33.5, 70.0, 52.0, 52.0, 52.0, 52.0,...  |
            | extra                 | f64          | 3.5, 0.0, 4.5, 0.0, 0.0, 2.5, 2.5, 0... |
            | mta_tax               | f64          | 0.5, 0.0, 0.5, 0.5, 0.5, 0.5, 0.5, 0... |
            | tip_amount            | f64          | 10.0, 14.06, 10.0, 12.37, 12.37, 12.... |
            | tolls_amount          | f64          | 6.55, 0.0, 6.55, 6.55, 6.55, 6.55,...   |
            | improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0... |
            | total_amount          | f64          | 54.35, 84.36, 77.6, 74.22, 74.22, 74... |
            | congestion_surcharge  | f64          | 2.5, 0.0, 2.5, 2.5, 2.5, 2.5, 2.5, 2... |
            | airport_fee           | f64          | 0.0, 0.0, 1.25, 0.0, 0.0, 0.0, 0.0,...  |
            +-----------------------+--------------+-----------------------------------------+
        "#
        )
    );

    Ok(())
}
