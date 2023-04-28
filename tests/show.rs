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
fn show() -> Result<()> {
    let input = indoc! {r#"
        parquet("tests/data/nyctaxi.parquet") |
            select(
                passenger_count,
                trip_distance,
                payment_type,
                fare_amount,
                total_amount) |
            filter(total_amount < 15) |
            show()
    "#};
    let output = interpreter::eval_to_string(input)?;

    assert_eq!(
        output,
        indoc!(
            r#"
            shape: (104, 5)
            ┌─────────────────┬───────────────┬──────────────┬─────────────┬──────────────┐
            │ passenger_count ┆ trip_distance ┆ payment_type ┆ fare_amount ┆ total_amount │
            │ ---             ┆ ---           ┆ ---          ┆ ---         ┆ ---          │
            │ i64             ┆ f64           ┆ str          ┆ f64         ┆ f64          │
            ╞═════════════════╪═══════════════╪══════════════╪═════════════╪══════════════╡
            │ 2               ┆ 1.06          ┆ Cash         ┆ 6.5         ┆ 9.8          │
            │ 2               ┆ 1.52          ┆ Cash         ┆ 8.5         ┆ 11.8         │
            │ 1               ┆ 0.51          ┆ Credit card  ┆ 6.0         ┆ 11.3         │
            │ 2               ┆ 0.85          ┆ Credit card  ┆ 5.5         ┆ 10.56        │
            │ 5               ┆ 0.48          ┆ Credit card  ┆ 5.0         ┆ 9.13         │
            │ 1               ┆ 1.42          ┆ Credit card  ┆ 8.0         ┆ 14.75        │
            │ 1               ┆ 1.43          ┆ Credit card  ┆ 7.5         ┆ 12.96        │
            │ 1               ┆ 1.6           ┆ Cash         ┆ 9.0         ┆ 12.8         │
            │ 1               ┆ 0.0           ┆ Cash         ┆ 2.5         ┆ 3.3          │
            │ 1               ┆ 1.25          ┆ Credit card  ┆ 7.0         ┆ 13.0         │
            │ 1               ┆ 0.43          ┆ Dispute      ┆ 4.0         ┆ 7.3          │
            │ 1               ┆ 0.5           ┆ Credit card  ┆ 4.5         ┆ 10.56        │
            │ 1               ┆ 1.1           ┆ Credit card  ┆ 5.0         ┆ 10.8         │
            │ 1               ┆ 2.63          ┆ Cash         ┆ 11.0        ┆ 14.3         │
            │ 1               ┆ 0.82          ┆ Credit card  ┆ 7.5         ┆ 11.8         │
            │ 1               ┆ 0.42          ┆ Credit card  ┆ 3.5         ┆ 8.5          │
            │ 1               ┆ 1.29          ┆ Credit card  ┆ 7.5         ┆ 12.42        │
            │ 5               ┆ 0.8           ┆ Credit card  ┆ 5.5         ┆ 10.56        │
            │ 1               ┆ 1.54          ┆ Credit card  ┆ 8.5         ┆ 14.75        │
            │ 1               ┆ 0.66          ┆ Credit card  ┆ 4.0         ┆ 9.36         │
            │ 3               ┆ 1.61          ┆ Credit card  ┆ 8.0         ┆ 12.8         │
            │ 1               ┆ 1.02          ┆ Credit card  ┆ 6.5         ┆ 12.42        │
            │ 1               ┆ 2.9           ┆ Cash         ┆ 11.0        ┆ 14.3         │
            │ 1               ┆ 1.1           ┆ No charge    ┆ 5.5         ┆ 8.8          │
            │ 5               ┆ 1.04          ┆ Credit card  ┆ 6.5         ┆ 11.76        │
            │ 1               ┆ 1.5           ┆ Credit card  ┆ 8.0         ┆ 12.8         │
            │ 1               ┆ 1.09          ┆ Credit card  ┆ 9.0         ┆ 14.3         │
            │ 1               ┆ 0.49          ┆ Credit card  ┆ 4.0         ┆ 8.76         │
            │ 1               ┆ 1.62          ┆ Credit card  ┆ 9.0         ┆ 14.76        │
            │ 1               ┆ 1.24          ┆ Credit card  ┆ 6.0         ┆ 12.36        │
            │ 1               ┆ 1.01          ┆ Credit card  ┆ 5.5         ┆ 11.16        │
            │ 1               ┆ 1.14          ┆ Credit card  ┆ 6.5         ┆ 12.36        │
            │ 1               ┆ 1.04          ┆ Credit card  ┆ 5.5         ┆ 11.16        │
            │ 1               ┆ 0.5           ┆ Credit card  ┆ 4.5         ┆ 9.8          │
            │ 2               ┆ 1.16          ┆ Credit card  ┆ 10.0        ┆ 14.8         │
            │ 1               ┆ 0.8           ┆ Credit card  ┆ 6.5         ┆ 12.95        │
            │ 1               ┆ 1.9           ┆ Cash         ┆ 7.5         ┆ 10.8         │
            │ 1               ┆ 0.67          ┆ Credit card  ┆ 6.0         ┆ 11.16        │
            │ 1               ┆ 2.84          ┆ Cash         ┆ 9.5         ┆ 13.3         │
            │ 1               ┆ 1.28          ┆ Cash         ┆ 7.0         ┆ 10.3         │
            │ 1               ┆ 0.46          ┆ Credit card  ┆ 5.5         ┆ 10.56        │
            │ 1               ┆ 1.07          ┆ Credit card  ┆ 5.5         ┆ 11.16        │
            │ 1               ┆ 1.29          ┆ Credit card  ┆ 7.0         ┆ 12.36        │
            │ 1               ┆ 1.33          ┆ Cash         ┆ 10.0        ┆ 13.8         │
            │ 1               ┆ 1.01          ┆ Credit card  ┆ 6.5         ┆ 11.76        │
            │ 2               ┆ 1.51          ┆ Cash         ┆ 9.0         ┆ 13.3         │
            │ 1               ┆ 0.64          ┆ Credit card  ┆ 5.5         ┆ 10.56        │
            │ 5               ┆ 0.55          ┆ Credit card  ┆ 6.5         ┆ 11.76        │
            │ 5               ┆ 1.88          ┆ Credit card  ┆ 7.0         ┆ 12.05        │
            │ 1               ┆ 0.88          ┆ Credit card  ┆ 7.5         ┆ 13.56        │
            │ 4               ┆ 1.24          ┆ Cash         ┆ 7.0         ┆ 7.8          │
            │ 1               ┆ 1.39          ┆ Cash         ┆ 8.0         ┆ 12.3         │
            │ 3               ┆ 1.03          ┆ Cash         ┆ 9.0         ┆ 12.3         │
            │ null            ┆ 0.5           ┆ Unknown      ┆ 4.5         ┆ 9.96         │
            │ 1               ┆ 1.23          ┆ Credit card  ┆ 8.0         ┆ 14.75        │
            │ 1               ┆ 1.06          ┆ Credit card  ┆ 5.0         ┆ 8.8          │
            │ 1               ┆ 1.44          ┆ Credit card  ┆ 8.0         ┆ 12.55        │
            │ 1               ┆ 0.69          ┆ Credit card  ┆ 5.0         ┆ 10.56        │
            │ 1               ┆ 0.7           ┆ Credit card  ┆ 4.5         ┆ 9.36         │
            │ 1               ┆ 1.57          ┆ Cash         ┆ 11.5        ┆ 14.8         │
            │ 2               ┆ 1.26          ┆ Credit card  ┆ 6.5         ┆ 12.96        │
            │ 1               ┆ 1.76          ┆ Credit card  ┆ 8.5         ┆ 12.8         │
            │ 1               ┆ 2.59          ┆ Cash         ┆ 10.5        ┆ 13.55        │
            │ 2               ┆ 1.3           ┆ Cash         ┆ 7.0         ┆ 10.3         │
            │ 1               ┆ 1.13          ┆ Credit card  ┆ 6.0         ┆ 11.16        │
            │ 1               ┆ 0.98          ┆ Credit card  ┆ 6.0         ┆ 12.36        │
            │ 1               ┆ 1.3           ┆ Credit card  ┆ 6.0         ┆ 11.62        │
            │ 1               ┆ 1.28          ┆ Credit card  ┆ 7.0         ┆ 11.8         │
            │ 1               ┆ 0.65          ┆ Credit card  ┆ 7.5         ┆ 14.16        │
            │ 1               ┆ 1.72          ┆ Credit card  ┆ 9.0         ┆ 13.8         │
            │ 1               ┆ 0.68          ┆ Credit card  ┆ 4.5         ┆ 9.36         │
            │ 1               ┆ 1.18          ┆ Cash         ┆ 7.0         ┆ 8.8          │
            │ 1               ┆ 1.78          ┆ Credit card  ┆ 7.5         ┆ 12.43        │
            │ 1               ┆ 1.5           ┆ Cash         ┆ 8.5         ┆ 12.8         │
            │ 1               ┆ 0.97          ┆ Credit card  ┆ 5.0         ┆ 11.16        │
            │ 1               ┆ 0.8           ┆ Credit card  ┆ 8.0         ┆ 13.56        │
            │ 3               ┆ 0.57          ┆ Credit card  ┆ 5.0         ┆ 9.96         │
            │ 1               ┆ 1.2           ┆ Credit card  ┆ 7.0         ┆ 11.3         │
            │ 1               ┆ 1.18          ┆ Cash         ┆ 6.5         ┆ 9.8          │
            │ 1               ┆ 0.75          ┆ Credit card  ┆ 5.5         ┆ 9.68         │
            │ 2               ┆ 1.44          ┆ Credit card  ┆ 8.0         ┆ 13.3         │
            │ 1               ┆ 1.3           ┆ Credit card  ┆ 7.0         ┆ 10.38        │
            │ 1               ┆ 0.8           ┆ Credit card  ┆ 5.5         ┆ 11.4         │
            │ 5               ┆ 1.09          ┆ Credit card  ┆ 7.5         ┆ 14.04        │
            │ 1               ┆ 1.15          ┆ Cash         ┆ 7.0         ┆ 10.8         │
            │ 6               ┆ 0.9           ┆ Cash         ┆ 5.0         ┆ 8.3          │
            │ 5               ┆ 0.74          ┆ Cash         ┆ 5.5         ┆ 8.8          │
            │ 3               ┆ 2.24          ┆ Cash         ┆ 9.5         ┆ 13.8         │
            │ 1               ┆ 1.0           ┆ Credit card  ┆ 6.5         ┆ 12.7         │
            │ 1               ┆ 0.61          ┆ Credit card  ┆ 5.0         ┆ 10.56        │
            │ 1               ┆ 1.72          ┆ Credit card  ┆ 6.5         ┆ 11.76        │
            │ 1               ┆ 1.59          ┆ Credit card  ┆ 8.5         ┆ 14.16        │
            │ 1               ┆ 1.32          ┆ Credit card  ┆ 10.0        ┆ 14.63        │
            │ 1               ┆ 0.76          ┆ Credit card  ┆ 7.5         ┆ 12.96        │
            │ 1               ┆ 1.91          ┆ Cash         ┆ 10.5        ┆ 13.8         │
            │ 1               ┆ 1.2           ┆ Cash         ┆ 6.5         ┆ 9.8          │
            │ 1               ┆ 1.05          ┆ Credit card  ┆ 7.0         ┆ 12.36        │
            │ 1               ┆ 0.97          ┆ Credit card  ┆ 8.0         ┆ 14.76        │
            │ 1               ┆ 0.7           ┆ Credit card  ┆ 5.5         ┆ 11.6         │
            │ 1               ┆ 1.65          ┆ Credit card  ┆ 7.5         ┆ 14.16        │
            │ 2               ┆ 0.8           ┆ Dispute      ┆ -4.5        ┆ -8.3         │
            │ 1               ┆ 1.2           ┆ Credit card  ┆ 6.0         ┆ 11.15        │
            │ 2               ┆ 2.5           ┆ Cash         ┆ 9.0         ┆ 11.05        │
            │ 1               ┆ 0.82          ┆ Credit card  ┆ 5.0         ┆ 9.8          │
            └─────────────────┴───────────────┴──────────────┴─────────────┴──────────────┘
      "#
        )
    );

    Ok(())
}
