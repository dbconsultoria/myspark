from pyspark.sql import SparkSession  # Import Spark session builder
from pyspark.sql import functions as F  # Import Spark SQL functions


# Function to normalize column names (lowercase and trimmed)
def normalize_columns(df):
    for col in df.columns:  # Iterate through all dataframe columns
        df = df.withColumnRenamed(
            col,
            col.lower().strip()  # Convert column name to lowercase and remove spaces
        )
    return df  # Return dataframe with normalized column names


def main():
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("silver_tborders")  # Define Spark application name
        .config("spark.hadoop.fs.defaultFS", "file:///")  # Force local filesystem usage
        .getOrCreate()  # Create or reuse existing Spark session
    )

    # Define Bronze and Silver storage paths
    bronze_path = "/data/bronze/mysql/tborders"
    silver_path = "/data/silver/tborders"

    # Read Bronze layer parquet files
    df = (
        spark.read
        .format("parquet")  # Specify parquet file format
        .load(bronze_path)  # Load data from Bronze path
    )

    # Normalize column names to avoid case sensitivity issues
    df = normalize_columns(df)

    # Select required columns and enforce correct data types
    df = (
        df.select(
            F.col("code").cast("int").alias("code"),  # Order primary key
            F.col("customer").cast("int").alias("customer"),  # Customer foreign key
            F.col("orderdate").cast("timestamp").alias("orderdate")  # Order timestamp
        )
    )

    # Apply basic data quality filters
    df = (
        df.filter(F.col("code").isNotNull())  # Ensure primary key exists
          .filter(F.col("orderdate").isNotNull())  # Ensure timestamp exists
    )

    # Remove duplicate records based on business key
    df = df.dropDuplicates(["code"])

    # Write cleaned data to Silver layer
    (
        df.write
          .mode("overwrite")  # Replace previous Silver data
          .parquet(silver_path)  # Save as parquet files
    )

    # Stop Spark session gracefully
    spark.stop()


# Entry point of the script
if __name__ == "__main__":
    main()
