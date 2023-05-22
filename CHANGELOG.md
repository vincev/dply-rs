# dply changelog
Changes to the `dply` crate are documented in this file.

## 0.1.5 - Unreleased
### Changed 🔧
* Enable `unnest` to work on struct columns.
* Add `inner_join`, `left_join`, `cross_join`, and `outer_join`.
* Add semicolon pipeline separator.

## 0.1.4 - 2023-05-16
### Changed 🔧
* Add `unnest` function for list columns.

## 0.1.3 - 2023-05-15
### Changed 🔧
* Update to Polars 0.29
* `filter`: Add `contains` predicate for string and list columns.
* `filter`: Add `is_null` predicate.
* `summarize`: Now works on ungrouped data.
* `mutate`: Add `len` function for list columns.

## 0.1.2 - 2023-05-09
### Changed 🔧
* Add support for quoting column names (ex. `last name`).

## 0.1.1 - 2023-05-08
### Changed 🔧
* Simplify README.md created docs folder.

## 0.1.0 - 2023-05-08
* Initial release
