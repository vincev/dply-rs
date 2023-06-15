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
fn df_variable() -> Result<()> {
    // Save dataframe with times to variable times_df and show both dataframes.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(ends_with("time"), passenger_count, trip_distance, total_amount) |
            times_df |
            select(passenger_count, trip_distance, total_amount) |
            head()

        times_df | head()
    "#};
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (10, 3)
            passenger_count|trip_distance|total_amount
            i64|f64|f64
            ---
            1|3.14|22.56
            2|1.06|9.8
            1|2.36|17.76
            1|5.2|26.16
            3|0.0|19.55
            1|2.39|22.3
            2|1.52|11.8
            1|0.51|11.3
            1|0.98|19.56
            2|2.14|15.36
            ---
            shape: (10, 5)
            tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|total_amount
            datetime[ns]|datetime[ns]|i64|f64|f64
            ---
            2022-11-22 19:27:01|2022-11-22 19:45:53|1|3.14|22.56
            2022-11-27 16:43:26|2022-11-27 16:50:06|2|1.06|9.8
            2022-11-12 16:58:37|2022-11-12 17:12:31|1|2.36|17.76
            2022-11-30 22:24:08|2022-11-30 22:39:16|1|5.2|26.16
            2022-11-26 23:03:41|2022-11-26 23:23:48|3|0.0|19.55
            2022-11-30 14:46:43|2022-11-30 15:17:39|1|2.39|22.3
            2022-11-22 14:36:34|2022-11-22 14:46:38|2|1.52|11.8
            2022-11-28 09:54:14|2022-11-28 10:02:07|1|0.51|11.3
            2022-11-09 17:39:58|2022-11-09 17:58:30|1|0.98|19.56
            2022-11-20 00:33:58|2022-11-20 00:42:35|2|2.14|15.36
            ---
      "#
        )
    );

    Ok(())
}
