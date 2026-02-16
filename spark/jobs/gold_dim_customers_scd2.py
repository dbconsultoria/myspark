from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, when,
    max as spark_max, row_number
)
from pyspark.sql.window import Window

# --------------------------------------------------
# Spark Session Initialization
# --------------------------------------------------
# Create Spark session for customer dimension SCD Type 2 processing
spark = SparkSession.builder.appName("gold_dim_customers_scd2").getOrCreate()

# Define input path from silver layer and output path to gold layer
silver_path = "file:///data/silver/tbcustomers"
gold_path = "file:///data/gold/dim_customers"


# Helper function to print log messages with INFO prefix
def log(msg):
    print(f"[INFO] {msg}")


# Log the start of the SCD Type 2 process
log("Starting SCD Type 2 for dim_customers")

# --------------------------------------------------
# Read Silver Layer Data
# --------------------------------------------------
# Read parquet files from silver layer and select/rename columns for dimension
silver_df = spark.read.parquet(silver_path).select(
    col("code").alias("customer_code"),  # Customer business key
    col("Name").alias("customer_name"),  # Customer full name
    col("Address").alias("customer_address"),  # Customer address
    col("Phone").alias("customer_phone"),  # Customer phone number
    col("Email").alias("customer_email"),  # Customer email address
    col("BirthDate").alias("birth_date")  # Customer birth date
)

# Log successful loading of silver data
log("Silver loaded")

# --------------------------------------------------
# Check Gold Layer Existence
# --------------------------------------------------
try:
    # Attempt to read existing gold dimension table
    gold_df = spark.read.parquet(gold_path)
    # Set flag indicating gold table exists (incremental mode)
    gold_exists = True
    # Log that we're running in incremental mode
    log("Gold detected – incremental mode")
except Exception:
    # If reading fails, gold table doesn't exist yet
    gold_exists = False
    # Log that we're running initial load
    log("Gold not found – initial load")

# --------------------------------------------------
# INITIAL LOAD - First time population
# --------------------------------------------------
if not gold_exists:
    # Create initial dimension with SCD Type 2 columns
    final_gold_df = (
        silver_df
        # Generate surrogate key starting from 0
        .withColumn("customer_sk", row_number().over(Window.orderBy("customer_code")) - 1)
        # Set valid_from to current date for all initial records
        .withColumn("valid_from", current_date())
        # Set valid_to to NULL (records are currently valid)
        .withColumn("valid_to", lit(None).cast("date"))
        # Mark all initial records as current
        .withColumn("is_current", lit(True))
    )

    # Write initial gold dimension table (overwrite mode)
    final_gold_df.write.mode("overwrite").parquet(gold_path)
    # Log successful initial load
    log("Initial Gold created")

# --------------------------------------------------
# INCREMENTAL SCD2 - Handle updates and new records
# --------------------------------------------------
else:
    # Filter gold table to get only currently active records (is_current = True)
    active_gold_df = gold_df.filter(col("is_current"))

    # Join silver (new data) with active gold (current state) to detect changes
    comparison_df = (
        silver_df.alias("s")
        # Left join to capture both existing and new customers
        .join(active_gold_df.alias("g"), "customer_code", "left")
        .select(
            col("customer_code"),  # Business key
            col("s.customer_name").alias("new_name"),  # New name
            col("s.customer_address").alias("new_address"),  # New address
            col("s.customer_phone").alias("new_phone"),  # New phone
            col("s.customer_email").alias("new_email"),  # New email
            col("s.birth_date").alias("new_birth_date"),  # New birth date
            col("g.customer_name").alias("old_name"),  # Current name
            col("g.customer_address").alias("old_address"),  # Current address
            col("g.customer_phone").alias("old_phone"),  # Current phone
            col("g.customer_email").alias("old_email"),  # Current email
            col("g.birth_date").alias("old_birth_date"),  # Current birth date
            col("g.customer_sk")  # Existing surrogate key
        )
    )

    # Identify new customers (not in gold yet) - old_name is NULL
    new_df = comparison_df.filter(col("old_name").isNull())

    # Identify changed customers - exists in gold but at least one attribute changed
    changed_df = comparison_df.filter(
        col("old_name").isNotNull() &  # Record exists in gold
        (
                (col("new_name") != col("old_name")) |  # Name changed
                (col("new_address") != col("old_address")) |  # Address changed
                (col("new_phone") != col("old_phone")) |  # Phone changed
                (col("new_email") != col("old_email")) |  # Email changed
                (col("new_birth_date") != col("old_birth_date"))  # Birth date changed (rare but possible correction)
        )
    )

    # Check if there are any changes to process
    if new_df.limit(1).count() == 0 and changed_df.limit(1).count() == 0:
        # No changes detected - exit without modifying gold
        log("No changes detected – Gold unchanged")
        spark.stop()
        exit(0)

    # Log that changes were detected and SCD2 will be applied
    log("Changes detected – applying SCD2")

    # Cache gold dataframe for reuse (performance optimization)
    gold_cached = gold_df.cache()

    # Get the maximum surrogate key to generate new keys
    max_sk = gold_cached.agg(spark_max("customer_sk")).first()[0] or 0

    # Close old versions - set is_current to False and valid_to to current date
    expired_gold = gold_cached.withColumn(
        "is_current",
        # Set is_current to False for customers that have changed
        when(
            col("customer_code").isin(
                # Get list of changed customer codes
                [r.customer_code for r in changed_df.select("customer_code").distinct().collect()]
            ),
            lit(False)
        ).otherwise(col("is_current"))  # Keep original value for unchanged records
    ).withColumn(
        "valid_to",
        # Set valid_to to current date when record becomes inactive
        when(col("is_current") == False, current_date()).otherwise(col("valid_to"))
    )

    # Create new versions for both new customers and changed customers
    new_versions = (
        # Select new customers with new attribute values
        new_df.select(
            "customer_code",
            col("new_name").alias("customer_name"),
            col("new_address").alias("customer_address"),
            col("new_phone").alias("customer_phone"),
            col("new_email").alias("customer_email"),
            col("new_birth_date").alias("birth_date")
        )
        # Union with changed customers (new versions)
        .union(
            changed_df.select(
                "customer_code",
                col("new_name").alias("customer_name"),
                col("new_address").alias("customer_address"),
                col("new_phone").alias("customer_phone"),
                col("new_email").alias("customer_email"),
                col("new_birth_date").alias("birth_date")
            )
        )
        # Generate new surrogate keys starting after max_sk
        .withColumn(
            "customer_sk",
            row_number().over(Window.orderBy("customer_code")) + max_sk
        )
        # Set valid_from to current date for new versions
        .withColumn("valid_from", current_date())
        # Set valid_to to NULL (records are currently valid)
        .withColumn("valid_to", lit(None).cast("date"))
        # Mark new versions as current
        .withColumn("is_current", lit(True))
    )

    # Combine expired records with new versions to create final gold table
    final_gold_df = expired_gold.unionByName(new_versions)

    # Write updated gold dimension (overwrite entire table)
    final_gold_df.write.mode("overwrite").parquet(gold_path)

    # Unpersist cached dataframe to free memory
    gold_cached.unpersist()

    # Log successful update
    log("Gold updated successfully")

# Stop Spark session
spark.stop()

# Log completion of SCD Type 2 process
log("SCD Type 2 process completed")