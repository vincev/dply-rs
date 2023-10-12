// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn count_column() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(payment_type) |
            arrange(payment_type) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 2)
            payment_type|n
            str|i64
            ---
            Cash|53
            Credit card|185
            Dispute|2
            No charge|1
            Unknown|9
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn count_sorted() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(payment_type, sort = true) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 2)
            payment_type|n
            str|i64
            ---
            Credit card|185
            Cash|53
            Unknown|9
            Dispute|2
            No charge|1
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn count_agg_column_name() -> Result<()> {
    // with a column n the add column must be nn.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(payment_type, n = passenger_count) |
            count(payment_type, sort = true) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 2)
            payment_type|nn
            str|i64
            ---
            Credit card|185
            Cash|53
            Unknown|9
            Dispute|2
            No charge|1
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn count_multi_cols() -> Result<()> {
    // with a column n the add column must be nn.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(payment_type, passenger_count) |
            arrange(payment_type, passenger_count) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (16, 3)
            payment_type|passenger_count|n
            str|i64|i64
            ---
            Cash|1|36
            Cash|2|7
            Cash|3|4
            Cash|4|3
            Cash|5|2
            Cash|6|1
            Credit card|0|1
            Credit card|1|144
            Credit card|2|21
            Credit card|3|8
            Credit card|4|3
            Credit card|5|8
            Dispute|1|1
            Dispute|2|1
            No charge|1|1
            Unknown|null|9
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn count_multi_cols_sorted() -> Result<()> {
    // with a column n the add column must be nn.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count(payment_type, passenger_count, sort = true) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (16, 3)
            payment_type|passenger_count|n
            str|i64|i64
            ---
            Credit card|1|144
            Cash|1|36
            Credit card|2|21
            Unknown|null|9
            Credit card|3|8
            Credit card|5|8
            Cash|2|7
            Cash|3|4
            Cash|4|3
            Credit card|4|3
            Cash|5|2
            Cash|6|1
            Credit card|0|1
            Dispute|1|1
            Dispute|2|1
            No charge|1|1
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn count_no_cols() -> Result<()> {
    // with a column n the add column must be nn.
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            count() |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (1, 1)
            n
            i64
            ---
            250
            ---
        "#
        )
    );

    Ok(())
}
