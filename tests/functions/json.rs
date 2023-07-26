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
fn json_load() -> Result<()> {
    let input = indoc! {r#"
        json("tests/data/github.json") |
            count() |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (1, 1)
            n
            i64
            ---
            4
            ---
        "#
        )
    );

    Ok(())
}

#[test]
fn json_field() -> Result<()> {
    let input = indoc! {r#"
        json("tests/data/github.json") |
            mutate(
                login = field(actor, login),
                head = field(payload, head)
            ) |
            select(login, head) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;
    println!("{output}");

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (4, 2)
            login|head
            str|str
            ---
            github-actions[bot]|a02be18dc2a0faa0faec14f50c8b190ca0b50034
            user2|13b4ac97a4f61aab3a4d866ba167c0708676cd88
            user3|null
            user4|1aad310db433d20a7fbff132e4b23a4b4e4461ed
            ---
        "#
        )
    );

    Ok(())
}
