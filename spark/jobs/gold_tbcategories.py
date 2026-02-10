from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType


def main():
    spark = (
        SparkSession.builder
        .appName("gold_dim_categories")
        .getOrCreate()
    )

    # Paths
    silver_path = "file:///data/silver/tbcategories"
    gold_path = "file:///data/gold/dim_categories"

    # Read from Silver
    df_silver = spark.read.parquet(silver_path)

    # Gold transformation (dimension)
    df_gold = (
        df_silver
        .select(
            col("code").cast(IntegerType()).alias("category_id"),
            col("description").cast(StringType()).alias("category_description")
        )
        .dropDuplicates(["category_id"])
    )

    # Write to Gold
    (
        df_gold
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
