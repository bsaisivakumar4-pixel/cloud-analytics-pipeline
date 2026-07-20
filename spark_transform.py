"""
TRANSFORM stage - PySpark version.

Same cleaning logic as the pandas version: drop missing customer IDs,
remove cancelled orders, filter bad values, compute TotalPrice.
Difference: Spark distributes this work across partitions/cores
instead of processing it all in one process like pandas does.
"""

from pyspark.sql.functions import col


def transform(df):
    original_count = df.count()

    # 1. Drop rows with no Customer ID
    df = df.filter(col("Customer ID").isNotNull())

    # 2. Drop cancelled orders -- Invoice starting with 'C'
    df = df.filter(~col("Invoice").startswith("C"))

    # 3. Drop non-positive quantity or price
    df = df.filter((col("Quantity") > 0) & (col("Price") > 0))

    # 4. Drop duplicate rows
    df = df.dropDuplicates()

    # 5. Derive TotalPrice -- new column, computed from existing ones
    df = df.withColumn("TotalPrice", col("Quantity") * col("Price"))

    cleaned_count = df.count()
    dropped = original_count - cleaned_count
    print(
        f"[transform] {original_count:,} rows in -> {cleaned_count:,} rows out "
        f"({dropped:,} dropped, {dropped/original_count:.1%})"
    )

    return df


if __name__ == "__main__":
    from spark_extract import get_spark_session, extract

    spark = get_spark_session()
    raw_df = extract(spark, "../order-etl-project/data/raw/online_retail.csv")
    clean_df = transform(raw_df)
    clean_df.show(5)
    clean_df.printSchema()