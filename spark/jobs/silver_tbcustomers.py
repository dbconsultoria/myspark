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
        .appName("silver_tbcustomers")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    # Paths
    bronze_path = "/data/bronze/mysql/tbcustomers"
    silver_path = "/data/silver/tbcustomers"

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
            F.col("name").cast("string").alias("name"),
            F.col("address").cast("string").alias("address"),
            F.col("phone").cast("string").alias("phone"),
            F.col("email").cast("string").alias("email"),
            F.col("birthdate").cast("timestamp").alias("birthdate")
        )
    )

    # Basic data quality rules
    df = (
        df.filter(F.col("code").isNotNull())
    )

    # Optional normalization (trim strings)
    df = (
        df.withColumn("name", F.trim(F.col("name")))
          .withColumn("email", F.trim(F.col("email")))
          .withColumn("phone", F.trim(F.col("phone")))
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
