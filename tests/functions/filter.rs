// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (7, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            1|0.0|Cash|3.3
            1|0.43|Dispute|7.3
            1|0.42|Credit card|8.5
            1|0.49|Credit card|8.76
            4|1.24|Cash|7.8
            6|0.9|Cash|8.3
            2|0.8|Dispute|-8.3
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (11, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            1|0.0|Cash|3.3
            1|0.43|Dispute|7.3
            1|0.42|Credit card|8.5
            1|1.1|No charge|8.8
            1|0.49|Credit card|8.76
            4|1.24|Cash|7.8
            1|1.06|Credit card|8.8
            1|1.18|Cash|8.8
            6|0.9|Cash|8.3
            5|0.74|Cash|8.8
            2|0.8|Dispute|-8.3
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            1|19.55|Credit card|77.6
            1|0.04|Credit card|84.36
            2|16.36|Credit card|77.64
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            1|19.55|Credit card|77.6
            1|17.79|Credit card|74.22
            1|0.04|Credit card|84.36
            2|16.36|Credit card|77.64
            1|16.63|Credit card|74.22
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            5|0.48|Credit card|9.13
            5|4.67|Cash|21.3
            5|0.8|Credit card|10.56
            5|1.04|Credit card|11.76
            5|3.61|Credit card|23.76
            5|0.55|Credit card|11.76
            5|1.88|Credit card|12.05
            5|1.09|Credit card|14.04
            5|0.74|Cash|8.8
            5|17.24|Credit card|66.36
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (8, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            2|1.06|Cash|9.8
            2|1.52|Cash|11.8
            2|2.88|Cash|16.3
            2|4.55|Cash|19.8
            2|1.51|Cash|13.3
            2|1.3|Cash|10.3
            2|0.8|Dispute|-8.3
            2|2.5|Cash|11.05
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (6, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            2|2.88|Cash|16.3
            1|3.6|Credit card|24.5
            2|0.0|Credit card|16.3
            1|1.5|Credit card|16.3
            1|3.6|Credit card|27.96
            1|2.24|Cash|16.3
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            2|2.88|Cash|16.3
            1|3.6|Credit card|24.5
            1|3.6|Credit card|27.96
            1|2.24|Cash|16.3
            ---
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

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 4)
            passenger_count|trip_distance|payment_type|total_amount
            i64|f64|str|f64
            ---
            2|1.06|Cash|9.8
            1|2.63|Cash|14.3
            1|1.18|Cash|9.8
            1|1.2|Cash|9.8
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_dates() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
          select(ends_with("time")) |
          filter(tpep_pickup_datetime < dt("2022-11-02")) |
          arrange(tpep_pickup_datetime) |
          show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (9, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[μs]|datetime[μs]
            ---
            2022-11-01T07:31:16|2022-11-01T08:19:44
            2022-11-01T10:45:13|2022-11-01T10:53:56
            2022-11-01T11:17:08|2022-11-01T12:08:15
            2022-11-01T11:33:46|2022-11-01T12:03:15
            2022-11-01T16:18:07|2022-11-01T16:27:30
            2022-11-01T17:43:51|2022-11-01T17:52:45
            2022-11-01T17:48:38|2022-11-01T17:59:55
            2022-11-01T19:25:41|2022-11-01T19:32:33
            2022-11-01T19:39:09|2022-11-01T19:45:10
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
          select(ends_with("time")) |
          filter(
            tpep_pickup_datetime > dt("2022-11-01 17:00:00") &
            tpep_pickup_datetime < dt("2022-11-02 02:00:00")
          ) |
          arrange(tpep_pickup_datetime) |
          show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[μs]|datetime[μs]
            ---
            2022-11-01T17:43:51|2022-11-01T17:52:45
            2022-11-01T17:48:38|2022-11-01T17:59:55
            2022-11-01T19:25:41|2022-11-01T19:32:33
            2022-11-01T19:39:09|2022-11-01T19:45:10
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
          select(ends_with("time")) |
          filter(
            tpep_pickup_datetime > dt("2022-11-02") &
            tpep_pickup_datetime < dt("2022-11-02 12:00:00")
          ) |
          arrange(tpep_pickup_datetime) |
          show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 2)
            tpep_pickup_datetime|tpep_dropoff_datetime
            datetime[μs]|datetime[μs]
            ---
            2022-11-02T02:02:12|2022-11-02T02:02:19
            2022-11-02T10:17:58|2022-11-02T10:36:07
            2022-11-02T10:40:38|2022-11-02T10:43:58
            2022-11-02T11:06:01|2022-11-02T11:35:00
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_list_contains() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
          filter(contains(floats, 3.5)) |
          select(floats) |
          head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 1)
            floats
            list[f64]
            ---
            [2.5, 3.5, 6.0, 23.0]
            [3.5, 15.0, 23.0]
            [2.5, 2.5, 3.5, 19.0]
            [3.5]
            [2.5, 3.5, 5.0, 5.0]
            [2.5, 3.5, 6.0]
            [3.5, 6.0]
            [3.5, 3.5, 6.0, 19.0]
            [3.5, 19.0, 19.0]
            [3.5, 15.0]
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
          filter(contains(ints, 3)) |
          select(ints) |
          head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 1)
            ints
            list[u32]
            ---
            [3, 88, 94]
            [3]
            [3, 15, 63]
            [3, 64, 93]
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
          filter(contains(tags, "g7")) |
          select(tags) |
          head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 1)
            tags
            list[str]
            ---
            [tag7]
            [tag2, tag4, tag7]
            [tag5, tag6, tag7, tag7]
            [tag2, tag3, tag7, tag8]
            [tag4, tag7, tag8]
            [tag2, tag2, tag2, tag7]
            [tag7]
            [tag5, tag7]
            [tag6, tag7]
            [tag5, tag6, tag7, tag9]
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_list_not_contains() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
          filter(!contains(ints, 3)) |
          select(ints) |
          head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 1)
            ints
            list[u32]
            ---
            [73]
            null
            [43, 97]
            null
            [65]
            [1, 22, 61, 87]
            null
            [36, 37, 44, 48]
            [6]
            null
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
          filter(!contains(tags, "tag1")) |
          select(tags) |
          head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 1)
            tags
            list[str]
            ---
            [tag2, tag5, tag8, tag8]
            [tag9]
            [tag5]
            [tag7]
            [tag2, tag3, tag4]
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_str_contains() -> Result<()> {
    // Detect payment types that contain 'no' ignoring case
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            filter(contains(payment_type, "(?i:no)")) |
            distinct(payment_type) |
            arrange(payment_type) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (2, 1)
            payment_type
            str
            ---
            No charge
            Unknown
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_str_not_contains() -> Result<()> {
    // Detect payment types that contain 'no' ignoring case
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            filter(!contains(payment_type, "(?i:no)")) |
            distinct(payment_type) |
            arrange(payment_type) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (3, 1)
            payment_type
            str
            ---
            Cash
            Credit card
            Dispute
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_is_null() -> Result<()> {
    // Detect payment types that contain 'no' ignoring case
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(ints, tags) |
            filter(is_null(ints) & contains(tags, "tag1")) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            ints|tags
            list[u32]|list[str]
            ---
            null|[tag1, tag3, tag5]
            null|[tag1, tag4, tag5, tag5]
            null|[tag1, tag3, tag7, tag8]
            null|[tag1, tag2, tag8]
            null|[tag1, tag9]
            null|[tag1, tag2, tag7]
            null|[tag1, tag2, tag6]
            null|[tag1, tag3]
            null|[tag1, tag7, tag9, tag9]
            null|[tag1, tag8]
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn filter_is_not_null() -> Result<()> {
    // Detect payment types that contain 'no' ignoring case
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(ints, tags) |
            filter(!is_null(ints) & contains(tags, "tag1")) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            ints|tags
            list[u32]|list[str]
            ---
            [6]|[tag1, tag3, tag6, tag9]
            [9, 23, 38, 92]|[tag1, tag5, tag9, tag9]
            [4]|[tag1, tag5, tag9]
            [8, 46, 49, 88]|[tag1]
            [11, 49]|[tag1, tag4, tag8, tag8]
            [47]|[tag1, tag6, tag9]
            [34, 77]|[tag1, tag7]
            [21, 28, 94]|[tag1, tag3, tag9]
            [17, 43]|[tag1, tag2, tag5, tag9]
            [26, 62]|[tag1, tag4, tag6]
            ---
       "#
        )
    );

    Ok(())
}
