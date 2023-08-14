dply is a command line tool for viewing, querying, and writing csv and parquet
files, inspired by [dplyr](https://dplyr.tidyverse.org/index.html) and powered by
[DataFusion](https://github.com/apache/arrow-datafusion).

## Usage overview

A dply pipeline consists of a number of functions to read, transform, or write
Parquet or CSV files.

### Conversions between CSV, NdJSON, and Parquet files

The functions `csv`, `json` and `parquet` read and write data for their respective
formats. The following two steps pipeline converts a Parquet file to NdJSON:

```
$ dply -c 'parquet("nyctaxi.parquet") | json("nyctaxi.json")'
```

We can use a `select` step if we want to convert a subset of the columns:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(ends_with("time"), payment_type) |
    json("nyctaxi.json")'
$ head -2 nyctaxi.json| jq
{
  "payment_type": "Credit card",
  "tpep_dropoff_datetime": "2022-11-22T19:45:53",
  "tpep_pickup_datetime": "2022-11-22T19:27:01"
}
{
  "payment_type": "Cash",
  "tpep_dropoff_datetime": "2022-11-27T16:50:06",
  "tpep_pickup_datetime": "2022-11-27T16:43:26"
}
```

### Extracting nested fields from nested NdJSON

To extract a nested field in a NdJSON file we can use the `field` function in a
`mutate` step. The following example extracts the `sha` from the list of
`commits` in the `payload` object:

```
$ dply -c 'json("./tests/data/github.json") |
    mutate(commits = field(payload, commits)) |
    unnest(commits) |
    mutate(sha = field(commits, sha)) |
    select(sha) |
    show()'
shape: (4, 1)
┌──────────────────────────────────────────┐
│ sha                                      │
│ ---                                      │
│ str                                      │
╞══════════════════════════════════════════╡
│ a02be18dc2a0faa0faec14f50c8b190ca0b50034 │
│ ac97a4ab3a4d86f61a6ba167c06cd8813b470867 │
│ null                                     │
│ e4b233f1323a4b4e4461ed1aad31d20a7fbf0db4 │
└──────────────────────────────────────────┘
```

Complex NdJSON files can be converted to Parquet for faster query processing:

```
$ dply -c 'json("github.json") | parquet("github.parquet")'
```

### Grouping, sorting columns, and saving results to a file

The following pipeline reads a Parquet file[^1], group rows by `payment_type`,
computes the minimum, mean, and maximum fare for each payment type, saves the
result to `fares.csv` CSV file, and shows the result:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    group_by(payment_type) |
    summarize(
        min_price = min(total_amount),
        mean_price = mean(total_amount),
        max_price = max(total_amount)
    ) |
    arrange(payment_type) |
    csv("fares.csv") |
    show()'
shape: (5, 4)
┌──────────────┬───────────┬────────────┬───────────┐
│ payment_type ┆ min_price ┆ mean_price ┆ max_price │
│ ---          ┆ ---       ┆ ---        ┆ ---       │
│ str          ┆ f64       ┆ f64        ┆ f64       │
╞══════════════╪═══════════╪════════════╪═══════════╡
│ Cash         ┆ -61.85    ┆ 18.07      ┆ 86.55     │
│ Credit card  ┆ 4.56      ┆ 22.969491  ┆ 324.72    │
│ Dispute      ┆ -55.6     ┆ -0.145161  ┆ 54.05     │
│ No charge    ┆ -16.3     ┆ 0.086667   ┆ 19.8      │
│ Unknown      ┆ 9.96      ┆ 28.893333  ┆ 85.02     │
└──────────────┴───────────┴────────────┴───────────┘
```

Running dply without any parameter starts the interactive client:

<img src="./docs/demo.gif" alt="Dply demo">

[^1]: The file `nyctaxi.parquet` in the [tests/data][tests-data] folder is a
250 rows parquet file sampled from the [NYC trip record data][nyc-trips].

[nyc-trips]: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
[tests-data]: https://github.com/vincev/dply-rs/tree/main/tests/data

## Supported functions

`dply` supports the following functions:

- [arrange](docs/functions.md#arrange) Sorts rows by column values
- [count](docs/functions.md#count) Counts columns unique values
- [config](docs/functions.md#config) Configure display format options
- [csv](docs/functions.md#csv) Reads or writes a dataframe in CSV format
- [distinct](docs/functions.md#distinct) Retains unique rows
- [filter](docs/functions.md#filter) Filters rows that satisfy given predicates
- [glimpse](docs/functions.md#glimpse) Shows a dataframe overview
- [group by and summarize](docs/functions.md#group_by-and-summarize) Performs grouped aggregations
- [head](docs/functions.md#head) Shows the first few dataframe rows in table format
- [joins](docs/functions.md#joins) Left, inner, outer and cross joins
- [json](docs/functions.md#json) Reads or writes a dataframe in JSON format
- [mutate](docs/functions.md#mutate) Creates or mutate columns
- [parquet](docs/functions.md#parquet) Reads or writes a dataframe in Parquet format
- [relocate](docs/functions.md#relocate) Moves columns positions
- [rename](docs/functions.md#rename) Renames columns
- [select](docs/functions.md#select) Selects columns
- [show](docs/functions.md#show) Shows all dataframe rows
- [unnest](docs/functions.md#unnest) Expands list columns into rows

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
