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

use super::assert_interpreter;

#[test]
fn distinct() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            distinct(passenger_count) |
            arrange(passenger_count) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (8, 1)
            passenger_count
            i64
            ---
            0
            1
            2
            3
            4
            5
            6
            null
            ---
        "#
        )
    );

    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            distinct(passenger_count, store_and_fwd_flag) |
            arrange(passenger_count, store_and_fwd_flag) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            passenger_count|store_and_fwd_flag
            i64|str
            ---
            0|N
            1|N
            1|Y
            2|N
            2|Y
            3|N
            4|N
            5|N
            6|N
            null|null
            ---
        "#
        )
    );

    Ok(())
}
