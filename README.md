dply is a command line tool for viewing, querying, and writing csv and parquet
files, inspired by [dplyr](https://dplyr.tidyverse.org/index.html) and powered by
[polars](https://github.com/pola-rs/polars).

## Usage overview

A dply pipeline consists of a number of functions to read, transform, or write
data to disk.

The following is an example of a three steps pipeline that reads a parquet file
selects all columns that contain amount and shows some of the data[^1]:

```
$ dply -c 'parquet("nyctaxi.parquet") | select(contains("amount")) | head()'
shape: (10, 4)
┌─────────────┬────────────┬──────────────┬──────────────┐
│ fare_amount ┆ tip_amount ┆ tolls_amount ┆ total_amount │
│ ---         ┆ ---        ┆ ---          ┆ ---          │
│ f64         ┆ f64        ┆ f64          ┆ f64          │
╞═════════════╪════════════╪══════════════╪══════════════╡
│ 14.5        ┆ 3.76       ┆ 0.0          ┆ 22.56        │
│ 6.5         ┆ 0.0        ┆ 0.0          ┆ 9.8          │
│ 11.5        ┆ 2.96       ┆ 0.0          ┆ 17.76        │
│ 18.0        ┆ 4.36       ┆ 0.0          ┆ 26.16        │
│ 12.5        ┆ 3.25       ┆ 0.0          ┆ 19.55        │
│ 19.0        ┆ 0.0        ┆ 0.0          ┆ 22.3         │
│ 8.5         ┆ 0.0        ┆ 0.0          ┆ 11.8         │
│ 6.0         ┆ 2.0        ┆ 0.0          ┆ 11.3         │
│ 12.0        ┆ 3.26       ┆ 0.0          ┆ 19.56        │
│ 9.0         ┆ 2.56       ┆ 0.0          ┆ 15.36        │
└─────────────┴────────────┴──────────────┴──────────────┘
```

A simple pipeline can be passed as a command line argument with the `-c` flag or
as standard input, for more complex pipelines is convenient to store the pipeline
in a file and run dply with the file name as a command line argument.

For example the NYC taxi test file [^1] has a `payment_type` and `total_amount`
columns, let's say we want to find out for all payment types the minimum,
maximum, and mean amount paid and the number of payments for each type sorted in
descending order, we can write the following pipeline in a dply file:

```
# Compute some statistics on the payment types
parquet("nyctaxi.parquet") |
    group_by(payment_type) |
    summarize(
        mean_price = mean(total_amount),
        min_price = min(total_amount),
        max_price = max(total_amount),
        n = n()
    ) |
    arrange(desc(n)) |
    show()
```

and then run the script:

```
$ dply payments.dply
shape: (5, 5)
┌──────────────┬────────────┬───────────┬───────────┬─────┐
│ payment_type ┆ mean_price ┆ min_price ┆ max_price ┆ n   │
│ ---          ┆ ---        ┆ ---       ┆ ---       ┆ --- │
│ str          ┆ f64        ┆ f64       ┆ f64       ┆ u32 │
╞══════════════╪════════════╪═══════════╪═══════════╪═════╡
│ Credit card  ┆ 22.378757  ┆ 8.5       ┆ 84.36     ┆ 185 │
│ Cash         ┆ 18.458491  ┆ 3.3       ┆ 63.1      ┆ 53  │
│ Unknown      ┆ 26.847778  ┆ 9.96      ┆ 54.47     ┆ 9   │
│ Dispute      ┆ -0.5       ┆ -8.3      ┆ 7.3       ┆ 2   │
│ No charge    ┆ 8.8        ┆ 8.8       ┆ 8.8       ┆ 1   │
└──────────────┴────────────┴───────────┴───────────┴─────┘
```

[^1]: The file `nyctaxi.parquet` in the [tests/data][tests-data] folder is a
250 rows parquet file sampled from the [NYC trip record data][nyc-trips].

[nyc-trips]: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
[tests-data]: https://github.com/vincev/dply-rs/tree/main/tests/data

## Supported functions

`dply` supports the following functions:

- [arrange](docs/functions.md#arrange) Sorts rows by column values
- [count](docs/functions.md#count) Counts columns unique values
- [csv](docs/functions.md#csv) Reads or writes a dataframe in CSV format
- [distinct](docs/functions.md#distinct) Retains unique rows
- [filter](docs/functions.md#filter) Filters rows that satisfy given predicates
- [glimpse](docs/functions.md#glimpse) Shows a dataframe overview
- [group by and summarize](docs/functions.md#group_by-and-summarize) Performs grouped aggregations
- [head](docs/functions.md#head) Shows the first few dataframe rows in table format
- [mutate](docs/functions.md#mutate) Creates or mutate columns
- [parquet](docs/functions.md#parquet) Reads or writes a dataframe in Parquet format
- [relocate](docs/functions.md#relocate) Moves columns positions
- [rename](docs/functions.md#rename) Renames columns
- [select](docs/functions.md#select) Selects columns
- [show](docs/functions.md#show) Shows all dataframe rows

more examples can be found in the [tests folder](tests).

## Installation

Binaries generated by the release Github action for Linux, macOS (x86), and
Windows are available in the [releases page][github-releases].

[github-releases]: https://github.com/vincev/dply-rs/releases/latest

You can also install `dply` using [Cargo](https://crates.io/install):

```bash
cargo install dply
```

or by building it from this repository:

```bash
git clone https://github.com/vincev/dply-rs
cd dply-rs
cargo install --path .
```