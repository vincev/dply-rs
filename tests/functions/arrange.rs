// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn arrange() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(passenger_count, total_amount) |
            arrange(desc(passenger_count), total_amount) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            passenger_count|total_amount
            i64|f64
            ---
            6|8.3
            5|8.8
            5|9.13
            5|10.56
            5|11.76
            5|11.76
            5|12.05
            5|14.04
            5|21.3
            5|23.76
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn arrange_desc() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(passenger_count, total_amount) |
            arrange(passenger_count, desc(total_amount)) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            passenger_count|total_amount
            i64|f64
            ---
            0|54.35
            1|84.36
            1|77.6
            1|74.22
            1|74.22
            1|74.2
            1|74.2
            1|70.69
            1|66.12
            1|63.1
            ---
        "#
        )
    );

    Ok(())
}
