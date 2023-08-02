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
fn rename() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                VendorID,
                ends_with("time"),
                ends_with("LocationID"),
                total_amount
            ) |
            rename(
                vendor_id = VendorID,
                pickup_datetime = tpep_pickup_datetime,
                dropoff_datetime = tpep_dropoff_datetime,
                pu_location_id = PULocationID,
                do_location_id = DOLocationID
            ) |
            head(1)
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (1, 6)
            vendor_id|pickup_datetime|dropoff_datetime|pu_location_id|do_location_id|total_amount
            i64|datetime[μs]|datetime[μs]|i64|i64|f64
            ---
            2|2022-11-22T19:27:01|2022-11-22T19:45:53|234|141|22.56
            ---
        "#
        )
    );

    Ok(())
}
