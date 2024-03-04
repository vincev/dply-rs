// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 6)
            vendor_id|pickup_datetime|dropoff_datetime|pu_location_id|do_location_id|total_amount
            i64|datetime[ns]|datetime[ns]|i64|i64|f64
            ---
            2|2022-11-22 19:27:01|2022-11-22 19:45:53|234|141|22.56
            ---
        "#
        )
    );

    Ok(())
}
