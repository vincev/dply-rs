// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn left_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_val = shape_id * 2) |
            filter(shape_id > 3) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(left_val = shape_id * 2) |
            left_join(right_df) |
            arrange(shape_id) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            shape_id|left_val|right_val
            u32|f64|f64
            ---
            1|2.0|null
            2|4.0|null
            3|6.0|null
            4|8.0|8.0
            5|10.0|10.0
            6|12.0|12.0
            7|14.0|14.0
            8|16.0|16.0
            9|18.0|18.0
            10|20.0|20.0
            ---
       "#
        )
    );

    // Join on different columns
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_key = shape_id * 2, right_val = shape_id * 2) |
            filter(shape_id > 3) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(left_key = shape_id * 2) |
            left_join(right_df, left_key == right_key) |
            arrange(shape_id) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 4)
            shape_id|left_key|shape_id_rhs|right_val
            u32|f64|u32|f64
            ---
            1|2.0|null|null
            2|4.0|null|null
            3|6.0|null|null
            4|8.0|4|8.0
            5|10.0|5|10.0
            6|12.0|6|12.0
            7|14.0|7|14.0
            8|16.0|8|16.0
            9|18.0|9|18.0
            10|20.0|10|20.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn inner_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_val = shape_id * 2) |
            filter(shape_id > 3) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(left_val = shape_id * 2) |
            inner_join(right_df) |
            arrange(shape_id) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            shape_id|left_val|right_val
            u32|f64|f64
            ---
            4|8.0|8.0
            5|10.0|10.0
            6|12.0|12.0
            7|14.0|14.0
            8|16.0|16.0
            9|18.0|18.0
            10|20.0|20.0
            11|22.0|22.0
            12|24.0|24.0
            13|26.0|26.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn outer_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_val = shape_id * 2) |
            filter(shape_id > 4) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            filter(shape_id < 8) |
            mutate(left_val = shape_id * 2) |
            outer_join(right_df) |
            arrange(shape_id, left_val, shape_id_rhs, right_val) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 4)
            shape_id|left_val|shape_id_rhs|right_val
            u32|f64|u32|f64
            ---
            1|2.0|null|null
            2|4.0|null|null
            3|6.0|null|null
            4|8.0|null|null
            5|10.0|5|10.0
            6|12.0|6|12.0
            7|14.0|7|14.0
            null|null|8|16.0
            null|null|9|18.0
            null|null|10|20.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn cross_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_val = shape_id * 2) |
            filter(shape_id < 3) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            filter(shape_id > 20 & shape_id < 24) |
            mutate(left_val = shape_id * 2) |
            cross_join(right_df) |
            arrange(shape_id) |
            show()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (6, 4)
            shape_id|left_val|shape_id_rhs|right_val
            u32|f64|u32|f64
            ---
            21|42.0|1|2.0
            21|42.0|2|4.0
            22|44.0|1|2.0
            22|44.0|2|4.0
            23|46.0|1|2.0
            23|46.0|2|4.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn multi_columns_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(
                right_c1 = shape_id,
                right_c2 = shape_id * 2,
                right_c3 = shape_id * 3
            ) |
            select(starts_with("right")) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(
                left_c1 = shape_id,
                left_c2 = shape_id * 2,
                left_c3 = shape_id * 3
            ) |
            inner_join(
                right_df,
                left_c1 == right_c1,
                left_c2 == right_c2,
                left_c3 == right_c3
            ) |
            arrange(shape_id) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 4)
            shape_id|left_c1|left_c2|left_c3
            u32|u32|f64|f64
            ---
            1|1|2.0|3.0
            2|2|4.0|6.0
            3|3|6.0|9.0
            4|4|8.0|12.0
            5|5|10.0|15.0
            6|6|12.0|18.0
            7|7|14.0|21.0
            8|8|16.0|24.0
            9|9|18.0|27.0
            10|10|20.0|30.0
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn anti_join() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            mutate(right_val = shape_id * 2) |
            filter(shape_id > 5) |
            right_df

        parquet("tests/data/lists.parquet") |
            select(shape_id) |
            filter(shape_id < 8) |
            mutate(left_val = shape_id * 2) |
            anti_join(right_df) |
            arrange(shape_id) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (5, 2)
            shape_id|left_val
            u32|f64
            ---
            1|2.0
            2|4.0
            3|6.0
            4|8.0
            5|10.0
            ---
       "#
        )
    );

    Ok(())
}
