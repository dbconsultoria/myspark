from pyspark.sql import SparkSession  # Import Spark session builder
from pyspark.sql import functions as F  # Import Spark SQL functions


# Function to normalize column names (lowercase and trimmed)
def normalize_columns(df):
    for col in df.columns:  # Iterate through dataframe columns
        df = df.withColumnRenamed(
            col,
            col.lower().strip()  # Normalize column name format
        )
    return df  # Return normalized dataframe


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("silver_tborderdetail")  # Define application name
        .config("spark.hadoop.fs.defaultFS", "file:///")  # Use local filesystem
        .getOrCreate()  # Create or reuse Spark session
    )

    # Define Bronze and Silver paths
    bronze_path = "/data/bronze/mysql/tborderdetail"
    silver_path = "/data/silver/tborderdetail"

    # Read Bronze parquet data
    df = (
        spark.read
        .format("parquet")  # Specify parquet format
        .load(bronze_path)  # Load Bronze dataset
    )

    # Normalize column names
    df = normalize_columns(df)

    # Select columns and enforce correct data types
    df = (
        df.select(
            F.col("product").cast("int").alias("product"),      # Product foreign key
            F.col("orders").cast("int").alias("orders"),        # Order foreign key
            F.col("quantity").cast("int").alias("quantity"),    # Quantity purchased
            F.col("salesvalue").cast("decimal(18,2)").alias("salesvalue")  # Monetary value
        )
    )

    # Apply basic data quality rules
    df = (
        df.filter(F.col("orders").isNotNull())   # Ensure order reference exists
          .filter(F.col("product").isNotNull())  # Ensure product reference exists
    )

    # Replace null numeric values with defaults (optional normalization)
    df = (
        df.withColumn(
            "quantity",
            F.when(F.col("quantity").isNull(), F.lit(0))
             .otherwise(F.col("quantity"))
        )
    )

    # Remove duplicates using composite business key
    df = df.dropDuplicates(["orders", "product"])

    # Write cleaned dataset to Silver layer
    (
        df.write
          .mode("overwrite")  # Replace previous Silver data
          .parquet(silver_path)  # Persist as parquet
    )

    # Stop Spark session
    spark.stop()


# Script entry point
if __name__ == "__main__":
    main()
