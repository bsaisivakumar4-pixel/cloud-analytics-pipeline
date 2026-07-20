"""
LOAD stage - PySpark version.

Writes the cleaned Spark DataFrame to Parquet, the standard columnar
format for cloud data pipelines. This local file is also exactly what
we'll upload to S3 in the next phase -- same file, same format, just
a different destination.
"""

OUTPUT_PATH = "data/processed/orders_parquet"


def load(df, path: str = OUTPUT_PATH):
    (
        df.write
        .mode("overwrite")   # re-running the pipeline won't create duplicates
        .parquet(path)
    )
    print(f"[load] Wrote cleaned data to {path}")


if __name__ == "__main__":
    from spark_extract import get_spark_session, extract
    from spark_transform import transform

    spark = get_spark_session()
    raw_df = extract(spark, "../order-etl-project/data/raw/online_retail.csv")
    clean_df = transform(raw_df)
    load(clean_df)