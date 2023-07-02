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
                avg_speed_km_h = trip_distance_km / (to_ns(travel_time) / 3.6e12)
            ) |
            relocate(trip_distance_km, after = trip_distance_mi) |
            head(10)
    "#};
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 6)
            tpep_pickup_datetime|tpep_dropoff_datetime|trip_distance_mi|trip_distance_km|travel_time|avg_speed_km_h
            datetime[μs]|datetime[μs]|f64|f64|interval[mdn]|f64
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53|3.14|5.053328|18m 52s|16.070653
            2022-11-27 16:43:26|2022-11-27 16:50:06|1.06|1.7059|6m 40s|15.353104
            2022-11-12 16:58:37|2022-11-12 17:12:31|2.36|3.798042|13m 54s|16.394428
            2022-11-30 22:24:08|2022-11-30 22:39:16|5.2|8.368568|15m 8s|33.179344
            2022-11-26 23:03:41|2022-11-26 23:23:48|0.0|0.0|20m 7s|0.0
            2022-11-30 14:46:43|2022-11-30 15:17:39|2.39|3.846323|30m 56s|7.46054
            2022-11-22 14:36:34|2022-11-22 14:46:38|1.52|2.446197|10m 4s|14.579981
            2022-11-28 09:54:14|2022-11-28 10:02:07|0.51|0.820763|7m 53s|6.246825
            2022-11-09 17:39:58|2022-11-09 17:58:30|0.98|1.577153|18m 32s|5.105892
            2022-11-20 00:33:58|2022-11-20 00:42:35|2.14|3.443988|8m 37s|23.981345
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
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
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
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
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
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
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
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
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
                date_datetime = dt(date_string)
            ) |
            head(2)
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (2, 4)
            trip_distance|tpep_pickup_datetime|date_string|date_datetime
            f64|datetime[μs]|str|datetime[ms]
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
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
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

    Ok(())
}
