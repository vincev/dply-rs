// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn select_columns() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                rate_code,
                tip_amount,
                tpep_pickup_datetime,
                airport_fee,
                tpep_dropoff_datetime
            ) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 5)
            rate_code|tip_amount|tpep_pickup_datetime|airport_fee|tpep_dropoff_datetime
            str|f64|datetime[ns]|f64|datetime[ns]
            ---
            Standard|3.76|2022-11-22 19:27:01|0.0|2022-11-22 19:45:53
            Standard|0.0|2022-11-27 16:43:26|0.0|2022-11-27 16:50:06
            Standard|2.96|2022-11-12 16:58:37|0.0|2022-11-12 17:12:31
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_rename() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                pickup_datetime = tpep_pickup_datetime,
                dropoff_datetime = tpep_dropoff_datetime,
                vendor_id = VendorID,
                pu_location_id = PULocationID
            ) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 4)
            pickup_datetime|dropoff_datetime|vendor_id|pu_location_id
            datetime[ns]|datetime[ns]|i64|i64
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53|2|234
            2022-11-27 16:43:26|2022-11-27 16:50:06|2|48
            2022-11-12 16:58:37|2022-11-12 17:12:31|2|142
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_starts_with() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(starts_with("tpep")) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[ns]|datetime[ns]
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53
            2022-11-27 16:43:26|2022-11-27 16:50:06
            2022-11-12 16:58:37|2022-11-12 17:12:31
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_not_starts_with() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance
            ) |
            select(!starts_with("tpep")) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 2)
            passenger_count|trip_distance
            i64|f64
            ---
            1|3.14
            2|1.06
            1|2.36
            1|5.2
            3|0.0
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_ends_with() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(ends_with("time")) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[ns]|datetime[ns]
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53
            2022-11-27 16:43:26|2022-11-27 16:50:06
            2022-11-12 16:58:37|2022-11-12 17:12:31
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_not_ends_with() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance
            ) |
            select(!ends_with("time")) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 2)
            passenger_count|trip_distance
            i64|f64
            ---
            1|3.14
            2|1.06
            1|2.36
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_contains() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(contains("time")) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[ns]|datetime[ns]
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53
            2022-11-27 16:43:26|2022-11-27 16:50:06
            2022-11-12 16:58:37|2022-11-12 17:12:31
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn select_not_contains() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance
            ) |
            select(!contains("time")) |
            head(3)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 2)
            passenger_count|trip_distance
            i64|f64
            ---
            1|3.14
            2|1.06
            1|2.36
            ---
        "#
        )
    );

    Ok(())
}
