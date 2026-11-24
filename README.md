# NYC Yellow Taxi Analytics with PySpark

This project analyzes 2023–2024 Yellow Taxi records using Databricks Free Edition serverless Spark and Photon. Its 24 monthly Parquet inputs total 1.24 GiB compressed. Parallel scans, filtering, joins, and aggregations across tens of millions of retained trips make it appropriate for demonstrating distributed processing.

## Run

1. Download all 24 monthly Yellow Taxi Parquet files for 2023 and 2024, and taxi_zone_lookup.csv from the official TLC page linked below.
2. Upload the inputs into `/Volumes/workspace/default/nyc_taxi`.
3. Import `nyc_taxi_pipeline.ipynb` into Databricks and run cells in order. Spark and dbutils are provided by Databricks. No pandas conversion of the trip data is used.
4. Inspect Parquet outputs in the `outputs` subdirectory. Raw and generated data are excluded from this repository.

## Pipeline coverage

| Requirement | Implementation |
| --- | --- |
| Dataset over 1 GB | 24 files, measured 1.24 GiB |
| Efficient loading | Parquet column selection, per-file schema normalization, unionByName |
| Filters and column transformations | Distance, fare, passenger, duration and date filters; time features and tip percentage |
| Join | Broadcast left join with Taxi Zone lookup |
| groupBy | Monthly borough counts, sums and averages |
| SQL query 1 | Annual borough comparison and LAG for year-over-year change |
| SQL query 2 | Borough peak hours ranked using ROW_NUMBER |
| Partitioning and output | Cleaned Parquet partitioned by year and month; three Parquet summaries |
| Execution plan | Formatted explain output in notebook and execution_plan.txt |
| Lazy evaluation | filter/withColumn/select followed by count action |

## Actions versus transformations

Spark transformations such as filter(), withColumn(), and select() are lazy: they construct a logical execution plan without immediately processing all rows. The count() action triggers execution and returns the number of matching trips to Python. In this example, it returned 1,126,477 trips. Because counting rows does not require amount_per_mile, Spark can prune that calculation from the executed plan.

## Performance analysis

The pipeline processes 24 monthly Yellow Taxi Parquet files from 2023–2024 totaling 1.24 GiB. Each file is read with its own schema, numeric fields are normalized, and the resulting DataFrames are combined using unionByName without a shuffle. Selecting required columns and filtering invalid distances, fares, passenger counts, durations, and dates before downstream aggregation reduces the data carried through the pipeline. The small zone lookup is broadcast for the left join, avoiding repartitioning the large trip table for that join. Aggregations still require a shuffle; partial aggregation reduces the volume exchanged.

The cleaned output is partitioned by pickup_year and pickup_month. The January 2024 query profile explicitly shows partition filters pickup_year = 2024 and pickup_month = 1, and scan-level filters trip_distance IS NOT NULL and trip_distance > 2. Only trip_distance, total_amount, and pickup_borough are required from the Parquet data; year and month come from partition attributes. The profile reports one file and one partition read, 9.29 MB read, 2,711,747 rows scanned, and 1,126,477 output rows after filtering. It returned eight borough groups in 897 ms server wall-clock time; the Python collect timer reported 1.11 seconds including client overhead.

The measured bottleneck was storage access: the scan took 512 ms, including 509 ms of cloud storage request duration, while partial grouping took 26 ms. No disk spilling was reported, and 94% of aggregated task time was in Photon. Repartitioning before the output write is an intentional shuffle to organize files by month; the cleaned write took 25.48 seconds. Other shuffles remain necessary for groupBy and SQL window functions. These observations describe individual runs, not a controlled before/after speedup benchmark; different queries and caching conditions cannot be compared as equivalent workloads.

## Key findings and interpretation

- In the cleaned sample, Manhattan trips increased from 31,425,326 in 2023 to 31,538,106 in 2024 (+0.36%); Queens declined 0.84%.
- Brooklyn increased 64.19% from a much smaller base (179,725 to 295,089 trips). These changes describe retained Yellow Taxi records, not all transportation demand.
- The busiest 2024 pickup hour was 18:00–18:59 in Manhattan, 16:00–16:59 in Queens, 08:00–08:59 in Brooklyn, and 09:00–09:59 in the Bronx. Staten Island has very few observations and should be interpreted cautiously.
- total_amount includes passenger charges such as taxes and tips and is not driver net income. Missing passenger counts and records outside the cleaning thresholds are excluded, so conclusions apply to this filtered population.

## Data source and reproducibility

Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Inputs: yellow_tripdata_2023-01.parquet through yellow_tripdata_2024-12.parquet, plus taxi_zone_lookup.csv. Upload these into /Volumes/workspace/default/nyc_taxi and run the notebook from top to bottom. Results are written under outputs/cleaned_trips, outputs/monthly_summary, outputs/yearly_comparison, and outputs/peak_hours. Re-running the output cell replaces these generated results.

## Execution evidence

The [notebook](nyc_taxi_pipeline.ipynb) and [HTML export](nyc_taxi_pipeline.html) include executed results. The complete formatted plan is preserved in [execution_plan.txt](execution_plan.txt). The following screenshots document the physical plan, successful output writing, and measured query performance. Photon uses `DictionaryFilters` and `RequiredDataFilters` labels in this plan rather than a literal `PushedFilters` field.

### Physical execution plan

The scan shows year/month partition filters, the distance filter, and a ReadSchema limited to three data columns. Partial aggregation precedes the required shuffle.

![Physical execution plan](screenshots/%E6%88%AA%E5%B1%8F2026-11-24%2014.19.31.png)

### Successful pipeline execution

Cleaned Parquet was written in 25.48 seconds. Three analytical outputs were also saved, and the output directories show year/month partitioning. Directory entries display size 0; this is not the size of their contents.

![Successful pipeline execution](screenshots/%E6%88%AA%E5%B1%8F2026-11-24%2014.26.04.png)

### Query Details and scan profile

The profile shows 897 ms server wall-clock duration, 9.29 MB read, one file and one partition read, 2,711,747 rows scanned, and zero disk spill. The scan emitted 1,126,477 rows after filtering; cloud-storage request duration was 509 ms.

![Query Details and scan profile](screenshots/%E6%88%AA%E5%B1%8F2026-11-24%2014.25.40.png)

## Scope

MLlib and explicit caching benchmarks are optional extensions and are not implemented. Reported times are observations from individual runs, not controlled speedup comparisons.
