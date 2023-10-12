// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 5)
            passenger_count|trip_distance|payment_type|fare_amount|total_amount
            i64|f64|str|f64|f64
            ---
            1|3.14|Credit card|14.5|22.56
            2|1.06|Cash|6.5|9.8
            1|2.36|Credit card|11.5|17.76
            1|5.2|Credit card|18.0|26.16
            3|0.0|Credit card|12.5|19.55
            1|2.39|Cash|19.0|22.3
            2|1.52|Cash|8.5|11.8
            1|0.51|Credit card|6.0|11.3
            1|0.98|Credit card|12.0|19.56
            2|2.14|Credit card|9.0|15.36
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 5)
            passenger_count|trip_distance|payment_type|fare_amount|total_amount
            i64|f64|str|f64|f64
            ---
            1|3.14|Credit card|14.5|22.56
            2|1.06|Cash|6.5|9.8
            1|2.36|Credit card|11.5|17.76
            1|5.2|Credit card|18.0|26.16
            3|0.0|Credit card|12.5|19.55
            ---
      "#
        )
    );

    Ok(())
}
