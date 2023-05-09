# Table of Contents

1. [Supported functions](#supported-functions)
2. [Pipeline variables](#pipeline-variables)
3. [Quoting column names](#quoting-column-names)

## Supported functions

`dply` supports the following functions:

- [arrange](#arrange) Sorts rows by column values
- [count](#count) Counts columns unique values
- [csv](#csv) Reads or writes a dataframe in CSV format
- [distinct](#distinct) Retains unique rows
- [filter](#filter) Filters rows that satisfy given predicates
- [glimpse](#glimpse) Shows a dataframe overview
- [group by and summarize](#group_by-and-summarize) Performs grouped aggregations
- [head](#head) Shows the first few dataframe rows in table format
- [mutate](#mutate) Creates or mutate columns
- [parquet](#parquet) Reads or writes a dataframe in Parquet format
- [relocate](#relocate) Moves columns positions
- [rename](#rename) Renames columns
- [select](#select) Selects columns
- [show](#show) Shows all dataframe rows

more examples can be found in the [tests folder][tests-dir].

[tests-dir]: https://github.com/vincev/dply-rs/tree/main/tests/data

### arrange

`arrange` sorts the rows of its input dataframe according to the values of the
given columns:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    count(payment_type, VendorID) |
    arrange(payment_type, n) |
    show()'
shape: (8, 3)
┌──────────────┬──────────┬─────┐
│ payment_type ┆ VendorID ┆ n   │
│ ---          ┆ ---      ┆ --- │
│ str          ┆ i64      ┆ u32 │
╞══════════════╪══════════╪═════╡
│ Cash         ┆ 1        ┆ 12  │
│ Cash         ┆ 2        ┆ 41  │
│ Credit card  ┆ 1        ┆ 37  │
│ Credit card  ┆ 2        ┆ 148 │
│ Dispute      ┆ 2        ┆ 2   │
│ No charge    ┆ 1        ┆ 1   │
│ Unknown      ┆ 2        ┆ 4   │
│ Unknown      ┆ 1        ┆ 5   │
└──────────────┴──────────┴─────┘
```

To invert the ordering of a column use the `desc` function:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    count(payment_type, VendorID) |
    arrange(desc(payment_type), n) |
    show()'
shape: (8, 3)
┌──────────────┬──────────┬─────┐
│ payment_type ┆ VendorID ┆ n   │
│ ---          ┆ ---      ┆ --- │
│ str          ┆ i64      ┆ u32 │
╞══════════════╪══════════╪═════╡
│ Unknown      ┆ 2        ┆ 4   │
│ Unknown      ┆ 1        ┆ 5   │
│ No charge    ┆ 1        ┆ 1   │
│ Dispute      ┆ 2        ┆ 2   │
│ Credit card  ┆ 1        ┆ 37  │
│ Credit card  ┆ 2        ┆ 148 │
│ Cash         ┆ 1        ┆ 12  │
│ Cash         ┆ 2        ┆ 41  │
└──────────────┴──────────┴─────┘
```

### count

`count` counts the number of unique values in the given columns:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    count(payment_type, VendorID) |
    show()'
shape: (8, 3)
┌──────────────┬──────────┬─────┐
│ payment_type ┆ VendorID ┆ n   │
│ ---          ┆ ---      ┆ --- │
│ str          ┆ i64      ┆ u32 │
╞══════════════╪══════════╪═════╡
│ Cash         ┆ 1        ┆ 12  │
│ Cash         ┆ 2        ┆ 41  │
│ Credit card  ┆ 1        ┆ 37  │
│ Credit card  ┆ 2        ┆ 148 │
│ Dispute      ┆ 2        ┆ 2   │
│ No charge    ┆ 1        ┆ 1   │
│ Unknown      ┆ 1        ┆ 5   │
│ Unknown      ┆ 2        ┆ 4   │
└──────────────┴──────────┴─────┘
```

passing `sort = true` sorts the counters in descending order:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    count(payment_type, VendorID, sort=true) |
    show()'
shape: (8, 3)
┌──────────────┬──────────┬─────┐
│ payment_type ┆ VendorID ┆ n   │
│ ---          ┆ ---      ┆ --- │
│ str          ┆ i64      ┆ u32 │
╞══════════════╪══════════╪═════╡
│ Credit card  ┆ 2        ┆ 148 │
│ Cash         ┆ 2        ┆ 41  │
│ Credit card  ┆ 1        ┆ 37  │
│ Cash         ┆ 1        ┆ 12  │
│ Unknown      ┆ 1        ┆ 5   │
│ Unknown      ┆ 2        ┆ 4   │
│ Dispute      ┆ 2        ┆ 2   │
│ No charge    ┆ 1        ┆ 1   │
└──────────────┴──────────┴─────┘
```

### csv

When `csv` is called as the first step in a pipeline it reads a csv file from disk:

```
$ dply -c 'csv("nyctaxi.csv") |
    select(passenger_count, trip_distance, total_amount) |
    head(5)'
shape: (5, 3)
┌─────────────────┬───────────────┬──────────────┐
│ passenger_count ┆ trip_distance ┆ total_amount │
│ ---             ┆ ---           ┆ ---          │
│ i64             ┆ f64           ┆ f64          │
╞═════════════════╪═══════════════╪══════════════╡
│ 1               ┆ 3.14          ┆ 22.56        │
│ 2               ┆ 1.06          ┆ 9.8          │
│ 1               ┆ 2.36          ┆ 17.76        │
│ 1               ┆ 5.2           ┆ 26.16        │
│ 3               ┆ 0.0           ┆ 19.55        │
└─────────────────┴───────────────┴──────────────┘
```

when called after the first step it writes the active dataframe to disk:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(passenger_count, payment_type, trip_distance, total_amount) |
    csv("trips.csv", overwrite = true) |
    count(passenger_count, payment_type, sort = true) |
    csv("payments.csv")'
$ ls *.csv
nyctaxi.csv  payments.csv trips.csv
```

By default `csv` generates an error if the file already exists, to overwrite the
file pass `overwrite = true`.

### distinct

`distinct` keeps unique rows in the input dataframe:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    distinct(payment_type, VendorID) |
    arrange(payment_type, VendorID) |
    show()'
shape: (8, 2)
┌──────────────┬──────────┐
│ payment_type ┆ VendorID │
│ ---          ┆ ---      │
│ str          ┆ i64      │
╞══════════════╪══════════╡
│ Cash         ┆ 1        │
│ Cash         ┆ 2        │
│ Credit card  ┆ 1        │
│ Credit card  ┆ 2        │
│ Dispute      ┆ 2        │
│ No charge    ┆ 1        │
│ Unknown      ┆ 1        │
│ Unknown      ┆ 2        │
└──────────────┴──────────┘
```

when called without any columns it shows the distinct rows in the input dataframe.

### filter

`filter` retains all the rows whose column values satisfy the given predicates.
For each predicate the left hand side of each condition must specify a column,
predicates that are comma separated are applied one after the other:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(payment_type, trip_distance, total_amount) |
    filter(payment_type == "Cash", trip_distance < 2, total_amount < 10) |
    show()'
shape: (8, 3)
┌──────────────┬───────────────┬──────────────┐
│ payment_type ┆ trip_distance ┆ total_amount │
│ ---          ┆ ---           ┆ ---          │
│ str          ┆ f64           ┆ f64          │
╞══════════════╪═══════════════╪══════════════╡
│ Cash         ┆ 1.06          ┆ 9.8          │
│ Cash         ┆ 0.0           ┆ 3.3          │
│ Cash         ┆ 1.24          ┆ 7.8          │
│ Cash         ┆ 1.18          ┆ 8.8          │
│ Cash         ┆ 1.18          ┆ 9.8          │
│ Cash         ┆ 0.9           ┆ 8.3          │
│ Cash         ┆ 0.74          ┆ 8.8          │
│ Cash         ┆ 1.2           ┆ 9.8          │
└──────────────┴───────────────┴──────────────┘
```

`filter` supports logical `&` and `|` in predicates, their priority is right
associative, the following predicate will return all rows whose payment is `Cash`
or rows whose `trip_distance < 2` and `total_amount < 10`:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(payment_type, trip_distance, total_amount) |
    filter(payment_type == "Cash" | trip_distance < 2 & total_amount < 10) |
    glimpse()'
Rows: 68
Columns: 3
+---------------+--------+----------------------------------------------------+
| payment_type  | str    | "Cash", "Cash", "Cash", "Credit card", "Cash",...  |
| trip_distance | f64    | 1.06, 2.39, 1.52, 0.48, 2.88, 4.67, 1.6, 0.0,...   |
| total_amount  | f64    | 9.8, 22.3, 11.8, 9.13, 16.3, 21.3, 12.8, 3.3, 7... |
+---------------+--------+----------------------------------------------------+
```

we can use parenthesis to change the priority:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(payment_type, trip_distance, total_amount) |
    filter((payment_type == "Cash" | trip_distance < 2) & total_amount < 10) |
    glimpse()'
Rows: 23
Columns: 3
+---------------+--------+----------------------------------------------------+
| payment_type  | str    | "Cash", "Credit card", "Cash", "Dispute", "Cred... |
| trip_distance | f64    | 1.06, 0.48, 0.0, 0.43, 0.42, 0.66, 1.1, 0.49, 0.5  |
| total_amount  | f64    | 9.8, 9.13, 3.3, 7.3, 8.5, 9.36, 8.8, 8.76, 9.8     |
+---------------+--------+----------------------------------------------------+
```

To compare dates use the `dt` function, it can parse a string with a date-time
`YYYY-MM-DD HH:MM:SS` or a date `YYYY-MM-DD`:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(ends_with("time")) |
    filter(tpep_pickup_datetime < dt("2022-11-01 12:00:00")) |
    show()'
shape: (4, 2)
┌──────────────────────┬───────────────────────┐
│ tpep_pickup_datetime ┆ tpep_dropoff_datetime │
│ ---                  ┆ ---                   │
│ datetime[ns]         ┆ datetime[ns]          │
╞══════════════════════╪═══════════════════════╡
│ 2022-11-01 10:45:13  ┆ 2022-11-01 10:53:56   │
│ 2022-11-01 07:31:16  ┆ 2022-11-01 08:19:44   │
│ 2022-11-01 11:33:46  ┆ 2022-11-01 12:03:15   │
│ 2022-11-01 11:17:08  ┆ 2022-11-01 12:08:15   │
└──────────────────────┴───────────────────────┘
```

### glimpse

`glimpse` displays an overview of the input dataframe by showing each column in a
row with its type and a few values. This format is convenient when a dataframe
has many columns and a table view doesn't fit in the terminal.

```
$ dply -c 'parquet("nyctaxi.parquet") | glimpse()'
Rows: 250
Columns: 19
+-----------------------+--------------+---------------------------------------------+
| VendorID              | i64          | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 2... |
| tpep_pickup_datetime  | datetime[ns] | 2022-11-22 19:27:01, 2022-11-27 16:43:26... |
| tpep_dropoff_datetime | datetime[ns] | 2022-11-22 19:45:53, 2022-11-27 16:50:06... |
| passenger_count       | i64          | 1, 2, 1, 1, 3, 1, 2, 1, 1, 2, 2, 1, 1, 1... |
| trip_distance         | f64          | 3.14, 1.06, 2.36, 5.2, 0.0, 2.39, 1.52,...  |
| rate_code             | str          | "Standard", "Standard", "Standard",...      |
| store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "N", "N",...  |
| PULocationID          | i64          | 234, 48, 142, 79, 237, 137, 107, 229, 16... |
| DOLocationID          | i64          | 141, 142, 236, 75, 230, 140, 162, 161, 1... |
| payment_type          | str          | "Credit card", "Cash", "Credit card",...    |
| fare_amount           | f64          | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8.5,...  |
| extra                 | f64          | 1.0, 0.0, 0.0, 0.5, 3.0, 0.0, 0.0, 0.0,...  |
| mta_tax               | f64          | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5,...  |
| tip_amount            | f64          | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.0, 2... |
| tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,...  |
| improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3,...  |
| total_amount          | f64          | 22.56, 9.8, 17.76, 26.16, 19.55, 22.3,...   |
| congestion_surcharge  | f64          | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5,...  |
| airport_fee           | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,...  |
+-----------------------+--------------+---------------------------------------------+
```

As `glimpse` consumes the input dataframe it must be the last function in a
pipeline.

### group_by and summarize

`group_by` and `summarize` work together to compute aggregations on groups of
values. `group_by` specifies which columns to use for the groups and `summarize`
specifies which aggregate operations to compute.

`summarize` supports the following aggregate functions, `max`, `min`, `mean`,
`median`, `sd`, `sum`, `var` and `quantile`.

A call to `group_by` must always be followed by a `summarize`.

For example to compute the mean, standard deviation, minimum and maximum price
paid and number of rows for each payment type:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    group_by(payment_type) |
    summarize(
        mean_price = mean(total_amount),
        std_price = sd(total_amount),
        min_price = min(total_amount),
        max_price = max(total_amount),
        n = n()
    ) |
    arrange(desc(n)) |
    show()'
shape: (5, 6)
┌──────────────┬────────────┬───────────┬───────────┬───────────┬─────┐
│ payment_type ┆ mean_price ┆ std_price ┆ min_price ┆ max_price ┆ n   │
│ ---          ┆ ---        ┆ ---       ┆ ---       ┆ ---       ┆ --- │
│ str          ┆ f64        ┆ f64       ┆ f64       ┆ f64       ┆ u32 │
╞══════════════╪════════════╪═══════════╪═══════════╪═══════════╪═════╡
│ Credit card  ┆ 22.378757  ┆ 16.095337 ┆ 8.5       ┆ 84.36     ┆ 185 │
│ Cash         ┆ 18.458491  ┆ 12.545236 ┆ 3.3       ┆ 63.1      ┆ 53  │
│ Unknown      ┆ 26.847778  ┆ 14.279152 ┆ 9.96      ┆ 54.47     ┆ 9   │
│ Dispute      ┆ -0.5       ┆ 11.030866 ┆ -8.3      ┆ 7.3       ┆ 2   │
│ No charge    ┆ 8.8        ┆ 0.0       ┆ 8.8       ┆ 8.8       ┆ 1   │
└──────────────┴────────────┴───────────┴───────────┴───────────┴─────┘
```

See [tests][tests-folder] for more examples.

### head

`head` shows the first few rows from a dataframe, an optional parameter can be
used to change the number of rows that are shown.

`head` must be the last step in a pipeline as it consumes the input dataframe.

### mutate

`mutate` creates new columns by applying transformations to existing columns. For
example to add column for trip duration and average speed in km/h:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(ends_with("time"), trip_distance_mi = trip_distance) |
    mutate(
        travel_time_ns = tpep_dropoff_datetime - tpep_pickup_datetime,
        trip_distance_km = trip_distance_mi * 1.60934,
        avg_speed_km_h = trip_distance_km / (travel_time_ns / 3.6e12)
    ) |
    select(travel_time_ns, trip_distance_km, avg_speed_km_h) |
    arrange(desc(travel_time_ns)) |
    head(5)'
shape: (5, 3)
┌────────────────┬──────────────────┬────────────────┐
│ travel_time_ns ┆ trip_distance_km ┆ avg_speed_km_h │
│ ---            ┆ ---              ┆ ---            │
│ duration[ns]   ┆ f64              ┆ f64            │
╞════════════════╪══════════════════╪════════════════╡
│ 1h 6m          ┆ 28.179543        ┆ 25.617767      │
│ 1h 2m 39s      ┆ 28.630159        ┆ 27.419146      │
│ 55m 48s        ┆ 26.763324        ┆ 28.777768      │
│ 53m 45s        ┆ 19.988003        ┆ 22.312189      │
│ 51m 7s         ┆ 14.966862        ┆ 17.567885      │
└────────────────┴──────────────────┴────────────────┘
```

`mutate` supports also `mean`, `max`, `min`, `median`, and `dt` functions see
[tests][tests-folder] for more examples.

### parquet

When `parquet` is called as the first step in a pipeline it reads a parquet file
from disk:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(passenger_count, trip_distance, total_amount) |
    head(5)'
shape: (5, 3)
┌─────────────────┬───────────────┬──────────────┐
│ passenger_count ┆ trip_distance ┆ total_amount │
│ ---             ┆ ---           ┆ ---          │
│ i64             ┆ f64           ┆ f64          │
╞═════════════════╪═══════════════╪══════════════╡
│ 1               ┆ 3.14          ┆ 22.56        │
│ 2               ┆ 1.06          ┆ 9.8          │
│ 1               ┆ 2.36          ┆ 17.76        │
│ 1               ┆ 5.2           ┆ 26.16        │
│ 3               ┆ 0.0           ┆ 19.55        │
└─────────────────┴───────────────┴──────────────┘
```

when called after the first step it writes the active dataframe to disk:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(passenger_count, payment_type, trip_distance, total_amount) |
    parquet("trips.parquet", overwrite = true) |
    count(passenger_count, payment_type, sort = true) |
    parquet("payments.parquet")'

$ ls *.parquet
nyctaxi.parquet  payments.parquet trips.parquet
```

By default `parquet` generates an error if the file already exists, to overwrite
the file pass `overwrite = true`.

### relocate

`relocate` moves column in the dataframe, by default the given columns are moved
before the first column:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    relocate(passenger_count, payment_type, total_amount) |
    glimpse()'
Rows: 250
Columns: 19
+-----------------------+--------------+----------------------------------------------------+
| passenger_count       | i64          | 1, 2, 1, 1, 3, 1, 2, 1, 1, 2, 2, 1, 1, 1, 1, 5,... |
| payment_type          | str          | "Credit card", "Cash", "Credit card", "Credit...   |
| total_amount          | f64          | 22.56, 9.8, 17.76, 26.16, 19.55, 22.3, 11.8, 11... |
| VendorID              | i64          | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 2, 2, 2,... |
| tpep_pickup_datetime  | datetime[ns] | 2022-11-22 19:27:01, 2022-11-27 16:43:26,...       |
| tpep_dropoff_datetime | datetime[ns] | 2022-11-22 19:45:53, 2022-11-27 16:50:06,...       |
| trip_distance         | f64          | 3.14, 1.06, 2.36, 5.2, 0.0, 2.39, 1.52, 0.51,...   |
| rate_code             | str          | "Standard", "Standard", "Standard", "Standard",... |
| store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "N", "N", "N", "N... |
| PULocationID          | i64          | 234, 48, 142, 79, 237, 137, 107, 229, 162, 48,...  |
| DOLocationID          | i64          | 141, 142, 236, 75, 230, 140, 162, 161, 186, 239... |
| fare_amount           | f64          | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8.5, 6.0, 12... |
| extra                 | f64          | 1.0, 0.0, 0.0, 0.5, 3.0, 0.0, 0.0, 0.0, 1.0, 0.... |
| mta_tax               | f64          | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.... |
| tip_amount            | f64          | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.0, 2.0, 3.2... |
| tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
| improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.... |
| congestion_surcharge  | f64          | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.... |
| airport_fee           | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
+-----------------------+--------------+----------------------------------------------------+
```

`relocate` also supports the options `before = column` and `after = column` to
move columns before or after a specific column, see [tests][tests-folder]
for examples.

### rename

`rename` renames columns, each rename has `new_name = old_name` format:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    rename(
        vendor_id = VendorID,
        pu_location_id = PULocationID,
        do_location_id = DOLocationID
    ) |
    glimpse()'
Rows: 250
Columns: 19
+-----------------------+--------------+----------------------------------------------------+
| vendor_id             | i64          | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 2, 2, 2,... |
| tpep_pickup_datetime  | datetime[ns] | 2022-11-22 19:27:01, 2022-11-27 16:43:26,...       |
| tpep_dropoff_datetime | datetime[ns] | 2022-11-22 19:45:53, 2022-11-27 16:50:06,...       |
| passenger_count       | i64          | 1, 2, 1, 1, 3, 1, 2, 1, 1, 2, 2, 1, 1, 1, 1, 5,... |
| trip_distance         | f64          | 3.14, 1.06, 2.36, 5.2, 0.0, 2.39, 1.52, 0.51,...   |
| rate_code             | str          | "Standard", "Standard", "Standard", "Standard",... |
| store_and_fwd_flag    | str          | "N", "N", "N", "N", "N", "N", "N", "N", "N", "N... |
| pu_location_id        | i64          | 234, 48, 142, 79, 237, 137, 107, 229, 162, 48,...  |
| do_location_id        | i64          | 141, 142, 236, 75, 230, 140, 162, 161, 186, 239... |
| payment_type          | str          | "Credit card", "Cash", "Credit card", "Credit...   |
| fare_amount           | f64          | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8.5, 6.0, 12... |
| extra                 | f64          | 1.0, 0.0, 0.0, 0.5, 3.0, 0.0, 0.0, 0.0, 1.0, 0.... |
| mta_tax               | f64          | 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.... |
| tip_amount            | f64          | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.0, 2.0, 3.2... |
| tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
| improvement_surcharge | f64          | 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.... |
| total_amount          | f64          | 22.56, 9.8, 17.76, 26.16, 19.55, 22.3, 11.8, 11... |
| congestion_surcharge  | f64          | 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.... |
| airport_fee           | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
+-----------------------+--------------+----------------------------------------------------+
```

### select

`select` keeps the columns specified in its arguments and optionally rename them.
It accepts column names and `starts_with`, `ends_with` and `contains` predicates:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(
        vendor_id = VendorID,
        ends_with("time"),
        contains("amount")
    ) |
    glimpse()'
Rows: 250
Columns: 7
+-----------------------+--------------+----------------------------------------------------+
| vendor_id             | i64          | 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 1, 2, 2, 2,... |
| tpep_pickup_datetime  | datetime[ns] | 2022-11-22 19:27:01, 2022-11-27 16:43:26,...       |
| tpep_dropoff_datetime | datetime[ns] | 2022-11-22 19:45:53, 2022-11-27 16:50:06,...       |
| fare_amount           | f64          | 14.5, 6.5, 11.5, 18.0, 12.5, 19.0, 8.5, 6.0, 12... |
| tip_amount            | f64          | 3.76, 0.0, 2.96, 4.36, 3.25, 0.0, 0.0, 2.0, 3.2... |
| tolls_amount          | f64          | 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.... |
| total_amount          | f64          | 22.56, 9.8, 17.76, 26.16, 19.55, 22.3, 11.8, 11... |
+-----------------------+--------------+----------------------------------------------------+
```

Any of the predicates functions can be negated with `!`:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(!contains("a")) |
    head(5)'
shape: (5, 1)
┌──────────┐
│ VendorID │
│ ---      │
│ i64      │
╞══════════╡
│ 2        │
│ 2        │
│ 2        │
│ 2        │
│ 1        │
└──────────┘
```

### show

`show` displays all the rows in the input dataframe in table format. `show` must
be the last step in a pipeline as it consumes the input dataframe.

## Pipeline variables

Pipeline variables store a pipeline progress that can be used in another pipeline,
in the following example the `fare_amounts` variable stores the result of the
parent `select` that is then used by another `group_by`:

```
$ dply -c 'parquet("nyctaxi.parquet") |
    select(ends_with("time"), payment_type, contains("amount")) |
    fare_amounts |
    group_by(payment_type) |
    summarize(mean_amount = mean(total_amount)) |
    head()

fare_amounts |
    group_by(payment_type) |
    summarize(mean_tips = mean(tip_amount)) |
    head()'
shape: (5, 2)
┌──────────────┬─────────────┐
│ payment_type ┆ mean_amount │
│ ---          ┆ ---         │
│ str          ┆ f64         │
╞══════════════╪═════════════╡
│ Credit card  ┆ 22.378757   │
│ Cash         ┆ 18.458491   │
│ Dispute      ┆ -0.5        │
│ Unknown      ┆ 26.847778   │
│ No charge    ┆ 8.8         │
└──────────────┴─────────────┘
shape: (5, 2)
┌──────────────┬───────────┐
│ payment_type ┆ mean_tips │
│ ---          ┆ ---       │
│ str          ┆ f64       │
╞══════════════╪═══════════╡
│ Credit card  ┆ 3.469784  │
│ Cash         ┆ 0.0       │
│ Dispute      ┆ 0.0       │
│ Unknown      ┆ 3.082222  │
│ No charge    ┆ 0.0       │
└──────────────┴───────────┘
```

[tests-folder]: https://github.com/vincev/dply-rs/tree/main/tests

## Quoting column names

To reference columns whose name contains characters that are not alphanumeric or
underscores you can quote the column using back ticks, the following example uses
the `travel time ns` column that contains words separated by spaces:

```
dply -c 'parquet("nyctaxi.parquet") |
    select(ends_with("time")) |
    mutate(`travel time ns` = tpep_dropoff_datetime - tpep_pickup_datetime) |
    select(`travel time ns`) |
    arrange(desc(`travel time ns`)) |
    head(2)'
shape: (2, 1)
┌────────────────┐
│ travel time ns │
│ ---            │
│ duration[ns]   │
╞════════════════╡
│ 1h 6m          │
│ 1h 2m 39s      │
└────────────────┘
```
