# Cloud Analytics Data Pipeline

An end-to-end cloud data pipeline: PySpark for distributed processing, AWS S3 for cloud storage, and AWS Redshift Serverless as the data warehouse. Built as a follow-up to my first project (a local pandas/SQLite ETL pipeline), this one moves the same kind of work into real cloud infrastructure.

## What it does

1. **Extract** (`spark_extract.py`) — reads raw retail transaction data (525K+ rows) using PySpark
2. **Transform** (`spark_transform.py`) — cleans the data (removes cancellations, nulls, duplicates), computes `TotalPrice`
3. **Load** (`spark_load.py`) — writes the cleaned data to Parquet, the standard columnar format for cloud pipelines
4. **Cloud storage** — uploads the Parquet output to an S3 bucket
5. **Data warehouse** — loads the S3 data into Redshift Serverless, with `SORTKEY` and `DISTSTYLE` applied to the table

## What I learned

- **PySpark vs pandas**: same logical transformations, different syntax (`.filter()` instead of `df[condition]`, `.withColumn()` instead of direct assignment). Verified both pipelines produced identical row counts (400,916 clean rows) as a sanity check.
- **Parquet over CSV**: columnar, compressed, and the actual format cloud data tools expect — roughly 6.5MB for the same data that would be significantly larger as CSV.
- **IAM permissions in practice**: hit and resolved a real permission chain — my working IAM user couldn't create IAM roles by default (a deliberate AWS security boundary), needed temporary elevated access to create a role, then had to correctly associate and set it as the *default* role on the Redshift namespace before it would actually work.
- **Redshift Spectrum will try to read anything in the folder**: a `COPY` command failed because leftover `.crc` checksum files (a Spark artifact) were sitting alongside the real `.parquet` files in S3 — Redshift tried to parse them as data. Fixed by cleaning the S3 folder to contain only genuine Parquet files.
- **Sort keys and distribution styles**: applied `SORTKEY (invoice_date, country)` and `DISTSTYLE ALL` to the Redshift table. At this dataset's size (400K rows), the measured performance difference was within normal query variance — sort keys show their real value at much larger scale or with date-range-filtered queries, not full-table aggregations on a small dataset. I'd rather report that honestly than claim an inflated speedup.

## Tech stack

Python, PySpark, AWS S3, AWS Redshift Serverless, AWS CLI, IAM

## Note on cost management

Redshift Serverless was used for a single focused session and the workgroup/namespace were deleted immediately after to avoid ongoing charges. The S3 bucket (a few cents/month at most) was kept as evidence of the working pipeline.