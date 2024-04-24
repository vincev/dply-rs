// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn mutate_arith() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                ends_with("time"),
                trip_distance_mi = trip_distance
            ) |
            mutate(
                travel_time = tpep_dropoff_datetime - tpep_pickup_datetime,
                trip_distance_km = trip_distance_mi * 1.60934,
                avg_speed_km_h = trip_distance_km / (travel_time / 3.6e12)
            ) |
            relocate(trip_distance_km, after = trip_distance_mi) |
            head(10)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 6)
            tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance_mi|trip_distance_km|travel_time|avg_speed_km_h
            datetime[ns]|datetime[ns]|f64|f64|duration[ns]|f64
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|5.0533276|18m 52s|16.070653
            2022-11-27 16:43:26|2022-11-27 16:50:06|1.06|1.7059004|6m 40s|15.353104
            2022-11-12 16:58:37|2022-11-12 17:12:31|2.36|3.7980424|13m 54s|16.394428
            2022-11-30 22:24:08|2022-11-30 22:39:16|5.2|8.368568|15m 8s|33.179344
            2022-11-26 23:03:41|2022-11-26 23:23:48|0.0|0.0|20m 7s|0.0
            2022-11-30 14:46:43|2022-11-30 15:17:39|2.39|3.8463226|30m 56s|7.46054
            2022-11-22 14:36:34|2022-11-22 14:46:38|1.52|2.4461968|10m 4s|14.579981
            2022-11-28 09:54:14|2022-11-28 10:02:07|0.51|0.8207634|7m 53s|6.246825
            2022-11-09 17:39:58|2022-11-09 17:58:30|0.98|1.5771532|18m 32s|5.105892
            2022-11-20 00:33:58|2022-11-20 00:42:35|2.14|3.443988|8m 37s|23.981345
            ---
       "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            mutate(group_id = shape_id % 10 ) |
            select(group_id) |
            head(15)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (15, 1)
            group_id
            u32
            ---
            1
            2
            3
            4
            5
            6
            7
            8
            9
            0
            1
            2
            3
            4
            5
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_mean() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(trip_distance) |
            mutate(
                mean_trip_distance = mean(trip_distance),
                trip_distance_minus = trip_distance - mean(trip_distance),
                trip_distance_plus = trip_distance + mean(trip_distance)
            ) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            trip_distance|mean_trip_distance|trip_distance_minus|trip_distance_plus
            f64|f64|f64|f64
            ---
            3.14|3.45644|-0.31644|6.59644
            1.06|3.45644|-2.39644|4.51644
            2.36|3.45644|-1.09644|5.81644
            5.2|3.45644|1.74356|8.65644
            0.0|3.45644|-3.45644|3.45644
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_median() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(trip_distance) |
            mutate(
                median_trip_distance = median(trip_distance),
                trip_distance_minus = trip_distance - median(trip_distance),
                trip_distance_plus = trip_distance + median(trip_distance)
            ) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            trip_distance|median_trip_distance|trip_distance_minus|trip_distance_plus
            f64|f64|f64|f64
            ---
            3.14|1.88|1.26|5.02
            1.06|1.88|-0.82|2.94
            2.36|1.88|0.48|4.24
            5.2|1.88|3.32|7.08
            0.0|1.88|-1.88|1.88
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_min() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(trip_distance) |
            mutate(
                min_trip_distance = min(trip_distance),
                trip_distance_minus = trip_distance - min(trip_distance),
                trip_distance_plus = trip_distance + min(trip_distance)
            ) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            trip_distance|min_trip_distance|trip_distance_minus|trip_distance_plus
            f64|f64|f64|f64
            ---
            3.14|0.0|3.14|3.14
            1.06|0.0|1.06|1.06
            2.36|0.0|2.36|2.36
            5.2|0.0|5.2|5.2
            0.0|0.0|0.0|0.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_max() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(trip_distance) |
            mutate(
                max_trip_distance = max(trip_distance),
                trip_distance_minus = trip_distance - max(trip_distance),
                trip_distance_plus = trip_distance + max(trip_distance)
            ) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            trip_distance|max_trip_distance|trip_distance_minus|trip_distance_plus
            f64|f64|f64|f64
            ---
            3.14|20.4|-17.26|23.54
            1.06|20.4|-19.34|21.46
            2.36|20.4|-18.04|22.76
            5.2|20.4|-15.2|25.6
            0.0|20.4|-20.4|20.4
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_dt() -> Result<()> {
    // Convert datetime string to a datetime.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(trip_distance, tpep_pickup_datetime) |
            mutate(
                date_string = "2022-11-27 16:43:26",
                date_datetime = ymd_hms(date_string)
            ) |
            head(2)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (2, 4)
            trip_distance|tpep_pickup_datetime|date_string|date_datetime
            f64|datetime[ns]|str|datetime[ns]
            ---
            3.14|2022-11-22 19:27:01|2022-11-27 16:43:26|2022-11-27 16:43:26
            1.06|2022-11-27 16:43:26|2022-11-27 16:43:26|2022-11-27 16:43:26
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_len() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            mutate(
                ints_len = len(ints),
                floats_len = len(floats),
                tags_len = len(tags)
            ) |
            select(ints_len, floats_len, tags_len) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            ints_len|floats_len|tags_len
            u32|u32|u32
            ---
            3|4|4
            1|3|1
            0|4|1
            2|4|1
            0|4|3
            1|1|3
            4|1|4
            0|2|0
            4|0|0
            1|4|4
            ---
       "#
        )
    );

    // Lengths on strings
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(rate_code) |
            mutate(rate_len = len(rate_code)) |
            arrange(rate_code) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 3)
            rate_code|n|rate_len
            str|u32|u32
            ---
            JFK|11|3
            Negotiated|2|10
            Standard|228|8
            null|9|null
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_row_number() -> Result<()> {
    // When using the row() function we need to select another column otherwise we
    // get error from the planner.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            mutate(row = row() % 5) |
            select(row, rate_code) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            row|rate_code
            u64|str
            ---
            1|Standard
            2|Standard
            3|Standard
            4|Standard
            0|Standard
            1|Standard
            2|Standard
            3|Standard
            4|Standard
            0|Standard
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_field() -> Result<()> {
    // Extract a field from a struct.
    let input = indoc! {r#"
        parquet("tests/data/structs.parquet") |
            filter(!is_null(points)) |
            unnest(points) |
            mutate(
                x = field(points, x),
                y = field(points, y)
            ) |
            select(shape_id, x, y) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            shape_id|x|y
            u32|f32|f32
            ---
            1|-7.144482|-2.752852
            1|-3.377404|-2.862458
            1|-4.05302|6.336014
            3|-8.744724|-0.039072
            4|-0.807573|-7.81899
            5|-2.831063|5.288568
            6|4.039896|-3.030655
            7|4.160488|9.694407
            7|-7.926216|-4.505739
            7|8.11179|8.441616
            ---
       "#
        )
    );

    // Lengths on strings
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(rate_code) |
            mutate(rate_len = len(rate_code)) |
            arrange(rate_code) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (4, 3)
            rate_code|n|rate_len
            str|u32|u32
            ---
            JFK|11|3
            Negotiated|2|10
            Standard|228|8
            null|9|null
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn mutate_durations() -> Result<()> {
    // Convert from duration to integer
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            mutate(travel_time = tpep_dropoff_datetime - tpep_pickup_datetime) |
            select(travel_time) |
            mutate(
                travel_time_secs = secs(travel_time),
                travel_time_millis = millis(travel_time),
                travel_time_micros = micros(travel_time),
                travel_time_nanos = nanos(travel_time)
            )|
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 5)
            travel_time|travel_time_secs|travel_time_millis|travel_time_micros|travel_time_nanos
            duration[ns]|i64|i64|i64|i64
            ---
            18m 52s|1132|1132000|1132000000|1132000000000
            6m 40s|400|400000|400000000|400000000000
            13m 54s|834|834000|834000000|834000000000
            15m 8s|908|908000|908000000|908000000000
            20m 7s|1207|1207000|1207000000|1207000000000
            ---
       "#
        )
    );

    // Convert from integer to duration
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            mutate(travel_time = tpep_dropoff_datetime - tpep_pickup_datetime) |
            select(travel_time) |
            mutate(
                travel_time_secs = secs(travel_time),
                travel_time_millis = millis(travel_time),
                travel_time_micros = micros(travel_time),
                travel_time_nanos = nanos(travel_time)
            )|
            mutate(
                dtravel_time_millis = dmillis(travel_time_millis),
                dtravel_time_micros = dmicros(travel_time_micros),
                dtravel_time_nanos = dnanos(travel_time_nanos)
            )|
            select(travel_time, starts_with("dtravel")) |
            head(5)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 4)
            travel_time|dtravel_time_millis|dtravel_time_micros|dtravel_time_nanos
            duration[ns]|duration[μs]|duration[μs]|duration[μs]
            ---
            18m 52s|18m 52s|18m 52s|18m 52s
            6m 40s|6m 40s|6m 40s|6m 40s
            13m 54s|13m 54s|13m 54s|13m 54s
            15m 8s|15m 8s|15m 8s|15m 8s
            20m 7s|20m 7s|20m 7s|20m 7s
            ---
       "#
        )
    );

    Ok(())
}
