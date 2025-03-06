// Copyright (C) 2023 Vince Vasta
// SPDX-License-Identifier: Apache-2.0
use anyhow::Result;
use indoc::indoc;

use super::assert_interpreter;

#[test]
fn unnest_ints() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            mutate(ints_len = len(ints)) |
            relocate(ints_len, ints, after = shape_id) |
            select(shape_id, ints_len, ints) |
            unnest(ints) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            shape_id|ints_len|ints
            u32|u32|u32
            ---
            1|3|3
            1|3|88
            1|3|94
            2|1|73
            3|0|null
            4|2|43
            4|2|97
            5|0|null
            6|1|65
            7|4|1
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn unnest_str() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            mutate(tags_len = len(tags)) |
            relocate(tags_len, tags, after = shape_id) |
            select(shape_id, tags_len, tags) |
            unnest(tags) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 3)
            shape_id|tags_len|tags
            u32|u32|str
            ---
            1|4|tag2
            1|4|tag5
            1|4|tag8
            1|4|tag8
            2|1|tag9
            3|1|tag5
            4|1|tag7
            5|3|tag2
            5|3|tag3
            5|3|tag4
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn unnest_floats() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/lists.parquet") |
            mutate(floats_len = len(floats)) |
            relocate(floats_len, floats, after = shape_id) |
            select(shape_id, floats_len, floats) |
            unnest(floats) |
            head(12)
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (12, 3)
            shape_id|floats_len|floats
            u32|u32|f64
            ---
            1|4|2.5
            1|4|3.5
            1|4|6.0
            1|4|23.0
            2|3|3.5
            2|3|15.0
            2|3|23.0
            3|4|1.0
            3|4|2.5
            3|4|6.0
            3|4|6.0
            4|4|2.5
            ---
       "#
        )
    );

    Ok(())
}

#[test]
fn unnest_structs() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/structs.parquet") |
            unnest(points) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 2)
            shape_id|points
            u32|struct[4]
            ---
            1|{"s1",0,-7.144482,-2.752852}
            1|{"s1",1,-3.377404,-2.862458}
            1|{"s1",2,-4.05302,6.336014}
            2|null
            3|{"s3",0,-8.744724,-0.039072}
            4|{"s4",0,-0.807573,-7.81899}
            5|{"s5",0,-2.831063,5.288568}
            6|{"s6",0,4.039896,-3.030655}
            7|{"s7",0,4.160488,9.694407}
            7|{"s7",1,-7.926216,-4.505739}
            ---
       "#
        )
    );

    // Unnest twice to extract the struct fields.
    let input = indoc! {r#"
        parquet("tests/data/structs.parquet") |
            unnest(points, points) |
            head()
    "#};

    assert_interpreter!(
        input,
        indoc!(
            r#"
            shape: (10, 5)
            shape_id|ptag|pid|x|y
            u32|str|i32|f32|f32
            ---
            1|s1|0|-7.144482|-2.752852
            1|s1|1|-3.377404|-2.862458
            1|s1|2|-4.05302|6.336014
            2|null|null|null|null
            3|s3|0|-8.744724|-0.039072
            4|s4|0|-0.807573|-7.81899
            5|s5|0|-2.831063|5.288568
            6|s6|0|4.039896|-3.030655
            7|s7|0|4.160488|9.694407
            7|s7|1|-7.926216|-4.505739
            ---
       "#
        )
    );

    Ok(())
}
