// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn relocate_default() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            payment_type|passenger_count|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee
            str|i64|i64|datetime[ns]|datetime[ns]|f64|str|str|i64|i64|f64|f64|f64|f64|f64|f64|f64|f64|f64
            ---
            Credit card|1|2|2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|Standard|N|234|141|14.5|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn relocate_before_first() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count, before = VendorID) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            payment_type|passenger_count|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee
            str|i64|i64|datetime[ns]|datetime[ns]|f64|str|str|i64|i64|f64|f64|f64|f64|f64|f64|f64|f64|f64
            ---
            Credit card|1|2|2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|Standard|N|234|141|14.5|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn relocate_before() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count, before = fare_amount) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|passenger_count|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee
            i64|datetime[ns]|datetime[ns]|f64|str|str|i64|i64|str|i64|f64|f64|f64|f64|f64|f64|f64|f64|f64
            ---
            2|2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|Standard|N|234|141|Credit card|1|14.5|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn relocate_after() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count, after = fare_amount) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|fare_amount|payment_type|passenger_count|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee
            i64|datetime[ns]|datetime[ns]|f64|str|str|i64|i64|f64|str|i64|f64|f64|f64|f64|f64|f64|f64|f64
            ---
            2|2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|Standard|N|234|141|14.5|Credit card|1|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn relocate_after_last() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count, after = airport_fee) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee|payment_type|passenger_count
            i64|datetime[ns]|datetime[ns]|f64|str|str|i64|i64|f64|f64|f64|f64|f64|f64|f64|f64|f64|str|i64
            ---
            2|2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|Standard|N|234|141|14.5|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0|Credit card|1
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn relocate_same_col() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            relocate(payment_type, passenger_count, after = passenger_count) |
            head(1)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 19)
            VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|payment_type|trip_distance|rate_code|store_and_fwd_flag|PULocationID|DOLocationID|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|airport_fee
            i64|datetime[ns]|datetime[ns]|i64|str|f64|str|str|i64|i64|f64|f64|f64|f64|f64|f64|f64|f64|f64
            ---
            2|2022-11-22 19:27:01|2022-11-22 19:45:53|1|Credit card|3.14|Standard|N|234|141|14.5|1.0|0.5|3.76|0.0|0.3|22.56|2.5|0.0
            ---
        "#
        )
    );

    Ok(())
}
