from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def normalize_columns(df):
    for col in df.columns:
        df = df.withColumnRenamed(
            col,
            col.lower().strip()
        )
    return df


def main():
    spark = (
        SparkSession.builder
        .appName("silver_tbproducts")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    # Paths
    bronze_path = "/data/bronze/mysql/tbproducts"
    silver_path = "/data/silver/tbproducts"

    # Read Bronze
    df = (
        spark.read
        .format("parquet")
        .load(bronze_path)
    )

    # Normalize column names
    df = normalize_columns(df)

    # Select and cast columns based on MySQL metadata
    df = (
        df.select(
            F.col("code").cast("int").alias("code"),
            F.col("description").cast("string").alias("description"),
            F.col("salevalue").cast("decimal(18,2)").alias("salevalue"),
            F.col("active").cast("int").alias("active"),
            F.col("category").cast("int").alias("category")
        )
    )

    # Basic data quality rules
    df = (
        df.filter(F.col("code").isNotNull())
    )

    # Optional business normalization
    df = (
        df.withColumn(
            "active",
            F.when(F.col("active").isNull(), F.lit(0))
             .otherwise(F.col("active"))
        )
    )

    # Remove duplicates by business key
    df = df.dropDuplicates(["code"])

    # Write Silver
    (
        df.write
          .mode("overwrite")
          .parquet(silver_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
