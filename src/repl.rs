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

//! REPL for dply expressions.
use anyhow::{anyhow, Result};
use reedline::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::{eval, fuzzy, parser, signatures, typing};

/// Runs a REPL for evaluation
pub fn run() -> Result<()> {
    let evaluator = Arc::new(Evaluator::default());

    const HISTORY_NAME: &str = ".dply_history";

    let history_path = home::home_dir()
        .map(|h| h.join(HISTORY_NAME))
        .unwrap_or_else(|| PathBuf::from(HISTORY_NAME));

    let history_size = std::env::var("DPLY_HISTSIZE")
        .unwrap_or_else(|_| "2500".to_string())
        .parse::<usize>()
        .map_err(|_| anyhow!("Invalid DPLY_HISTSIZE value"))?;

    let history = Box::new(
        FileBackedHistory::with_file(history_size, history_path)
            .map_err(|e| anyhow!("Unable to create history file: {e}"))?,
    );

    let completer = Box::new(CustomCompleter {
        evaluator: evaluator.clone(),
    });

    let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));

    let mut keybindings = default_emacs_keybindings();
    add_menu_keybindings(&mut keybindings);

    let edit_mode = Box::new(Emacs::new(keybindings));

    let mut line_editor = Reedline::create()
        .with_completer(completer)
        .with_validator(Box::new(CustomValidator))
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_history(history)
        .with_edit_mode(edit_mode);

    println!("Welcome to dply {}", env!("CARGO_PKG_VERSION"));
    println!("Use Tab for completions, arrows to move around, and Enter for selection.");
    println!("Enter twice with an empty line to execute the pipeline.");
    println!("Read a file with 'parquet' or 'csv' to get columns completions.");
    println!("For columns only completions start completions with a dot.");

    let prompt = DefaultPrompt {
        left_prompt: DefaultPromptSegment::Empty,
        right_prompt: DefaultPromptSegment::Empty,
    };

    loop {
        let sig = line_editor.read_line(&prompt)?;
        match sig {
            Signal::Success(input) => {
                if let Err(e) = evaluator.eval(&input) {
                    println!("Error: {e}");
                }
            }
            Signal::CtrlD | Signal::CtrlC => {
                break Ok(());
            }
        }
    }
}

#[derive(Default)]
struct Evaluator {
    ctx: Mutex<eval::Context>,
}

impl Evaluator {
    fn eval(&self, input: &str) -> Result<()> {
        if !input.trim().trim_matches(';').is_empty() {
            let pipelines = parser::parse(input)?;
            typing::validate(&pipelines)?;

            let mut ctx = self.ctx.lock().unwrap();
            eval::eval(&mut ctx, &pipelines)?;
        }

        Ok(())
    }

    fn completions(&self, pattern: &str) -> Vec<String> {
        let ctx = self.ctx.lock().unwrap();

        // If pattern starts with a dot only complete columns and variables.
        let mut completions = if pattern.starts_with('.') {
            Vec::new()
        } else {
            signatures::completions(pattern)
        };

        completions.extend(ctx.completions());
        completions.extend(ctx.vars());

        completions.sort();
        completions.dedup();

        let matcher = fuzzy::Matcher::new(pattern.trim_start_matches('.'));

        completions.retain(|s| matcher.is_match(s));
        completions
    }
}

fn add_menu_keybindings(keybindings: &mut Keybindings) {
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );

    keybindings.add_binding(
        KeyModifiers::SHIFT,
        KeyCode::BackTab,
        ReedlineEvent::MenuPrevious,
    );
}

struct CustomCompleter {
    evaluator: Arc<Evaluator>,
}

impl Completer for CustomCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let line = &line[..pos];
        let prefix_pos = line
            .rfind(&[',', '|', ' ', '\t', '\n'])
            .map(|p| p + 1)
            .unwrap_or(0);

        let prefix = &line[prefix_pos..];
        if is_file_completion(prefix) {
            let prefix_pos = prefix_pos + prefix.find('"').unwrap_or(0) + 1;

            file_complete(&line[prefix_pos..])
                .unwrap_or_default()
                .into_iter()
                .map(|value| Suggestion {
                    value,
                    description: None,
                    extra: None,
                    span: Span::new(prefix_pos, pos),
                    append_whitespace: false,
                })
                .collect()
        } else {
            let prefix_pos = line
                .rfind(&['(', ',', '|', ' ', '\t', '\n'])
                .map(|p| p + 1)
                .unwrap_or(0);

            self.evaluator
                .completions(&line[prefix_pos..])
                .into_iter()
                .map(|value| Suggestion {
                    value,
                    description: None,
                    extra: None,
                    span: Span::new(prefix_pos, pos),
                    append_whitespace: false,
                })
                .collect()
        }
    }
}

fn is_file_completion(prefix: &str) -> bool {
    let is_file_function = prefix.starts_with("parquet(\"") | prefix.starts_with("csv(\"");
    is_file_function && prefix.matches('"').count() == 1
}

fn file_complete(prefix: &str) -> Result<Vec<String>> {
    let path = if prefix.starts_with('~') {
        home::home_dir()
            .map(|p| p.join(prefix.trim_start_matches(['~', '/'])))
            .unwrap_or_else(|| PathBuf::from(prefix))
    } else if prefix.is_empty() || !prefix.starts_with("./") {
        PathBuf::from(".")
    } else {
        PathBuf::from(prefix)
    };

    let mut paths = Vec::new();

    if path.is_dir() {
        paths.extend(read_dir(&path, prefix)?);
    } else if path.exists() {
        paths.push(path.to_string_lossy().into());
    } else if let Some(parent) = path.parent() {
        if parent.is_dir() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            paths.extend(read_dir(parent, name.as_ref())?);
        }
    }

    Ok(paths)
}

fn read_dir(path: &Path, filter: &str) -> Result<Vec<String>> {
    let mut paths = Vec::new();

    let matcher = fuzzy::Matcher::new(filter);

    for path in path
        .read_dir()?
        .filter_map(|de| de.map(|e| e.path()).ok())
        .filter(|p| matcher.is_match(p.to_string_lossy().as_ref()))
    {
        let path_str = if path.is_dir() {
            format!("{}/", path.to_string_lossy())
        } else {
            path.to_string_lossy().into()
        };

        paths.push(path_str);
    }

    Ok(paths)
}

/// Accept multi line input separated by empty line or semicolon.
struct CustomValidator;

impl Validator for CustomValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        match line.chars().last() {
            Some('\n') | Some(';') | None => ValidationResult::Complete,
            _ => ValidationResult::Incomplete,
        }
    }
}
