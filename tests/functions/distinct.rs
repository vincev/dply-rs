// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
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
