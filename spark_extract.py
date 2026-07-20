"""
EXTRACT stage - PySpark version.

Same job as before: load raw data into memory, no business logic.
Difference: instead of a pandas DataFrame, this returns a Spark
DataFrame, which can be distributed across multiple machines at
scale (even though right now, on your Mac, it's just running on
one machine pretending to be a small cluster).
"""

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Start (or reuse) a local Spark session."""
    return (
        SparkSession.builder
        .appName("CloudAnalyticsPipeline")
        .master("local[*]")   # run locally, use all available CPU cores
        .getOrCreate()
    )


def extract(spark: SparkSession, path: str):
    """Read the raw CSV into a Spark DataFrame."""
    df = spark.read.csv(
        path,
        header=True,        # first row is column names
        inferSchema=True,   # let Spark guess column types automatically
    )
    print(f"[extract] Loaded {df.count():,} rows")
    return df


if __name__ == "__main__":
    spark = get_spark_session()
    df = extract(spark, "../order-etl-project/data/raw/online_retail.csv")
    df.printSchema()
    df.show(5)