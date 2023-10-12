# dply changelog
Changes to the `dply` crate are documented in this file.

## 0.2.1 - 2023-10-12
### 🐛 Fixed
* Allow user to override file extensions when loading data.
### 🔧 Changed
* `mutate`: Rename `dt` to `ymd_hms` following the lubridate R package.
### ⭐ Added
* Add backticks to completions for columns names that are not alphanumeric.
* `mutate`: Add `dnanos`, `dmicros`, `dmillis`, and `dsecs` to convert numbers to duration.
* `mutate`: Add `nanos`, `micros`, `millis`, and `secs` to convert duration to numbers.


## 0.2.0 - 2023-08-14
### 🔧 Changed
* Use DataFusion as query engine.
* `parquet`: Speedup parquet writing for multiple partitions (about 40% increase).
* `parquet`: A folder path reads all parquet files in the folder.
* `parquet`: write now uses compression to reduce file size.
* `mutate`: Make `len` function to work on strings.
* Improve display precision for microsecond and nanosecond timestamps.
### ⭐ Added
* Added `json` function to read and write NdJSON files.
* Added `config` function to set display options (number of columns, and column and table width).
* `anti_join`: Added `anti_join` to select rows not found in the orther data frame.
* `mutate`: Add `field` function to extract fields from a JSON/struct objects.
* `mutate`: Add `row` function that returns the row number.
* Show execution time on dataframe header.

## 0.1.9 - 2023-06-28
### 🔧 Changed
* Improve glimpse formatting.
### 🐛 Fixed
* repl: Fix completions for absolute and tilde paths.
* repl: Keep all completion columns for big dataframes.

## 0.1.8 - 2023-06-19
### ⭐ Added
* repl: Add parenthesis to functions completions. (See Issue #27)
* repl: Completions starting with a `.` show only columns and variables.
* repl: Show completions for most recently used columns.

## 0.1.7 - 2023-06-11
### ⭐ Added
* Add REPL fuzzy matching.
### 🐛 Fixed
* Prevent out of bound REPL panic on completion.
* Clear evaluation context before REPL pipeline evaluation.

## 0.1.6 - 2023-06-08
### ⭐ Added
* Add `reedline` REPL.

## 0.1.5 - 2023-05-29
### ⭐ Added
* Add `inner_join`, `left_join`, `cross_join`, and `outer_join`.
* Add semicolon pipeline separator.
* Enable `unnest` to work on struct columns.
* `summarize`: Add `list` function for creating list columns from grouped data.

### 🔧 Changed
* Update to Polars 0.30

## 0.1.4 - 2023-05-16
### ⭐ Added
* Add `unnest` function for list columns.

## 0.1.3 - 2023-05-15
### ⭐ Added
* `filter`: Add `contains` predicate for string and list columns.
* `filter`: Add `is_null` predicate.
* `mutate`: Add `len` function for list columns.

### 🔧 Changed
* Update to Polars 0.29
* `summarize`: Now works on ungrouped data.

## 0.1.2 - 2023-05-09
### ⭐ Added
* Add support for quoting column names (ex. `last name`).

## 0.1.1 - 2023-05-08
### 🔧 changed
* Simplify README.md created docs folder.

## 0.1.0 - 2023-05-08
* Initial release
