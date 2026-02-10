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
        .appName("silver_tbcategories")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    # Paths
    bronze_path = "/data/bronze/mysql/tbcategories"
    silver_path = "/data/silver/tbcategories"

    # Read Bronze
    df = (
        spark.read
        .format("parquet")
        .load(bronze_path)
    )

    # Normalize columns
    df = normalize_columns(df)

    # Select and type columns
    df = (
        df.select(
            F.col("code").cast("int").alias("code"),
            F.col("description").cast("string").alias("description")
        )
    )

    # Basic data quality rules
    df = (
        df.filter(F.col("code").isNotNull())
          .filter(F.col("description").isNotNull())
    )

    # Remove duplicates
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
