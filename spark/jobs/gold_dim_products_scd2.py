from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, when,
    max as spark_max, row_number
)
from pyspark.sql.window import Window

# --------------------------------------------------
# Spark Session Initialization
# --------------------------------------------------
# Create Spark session for product dimension SCD Type 2 processing
spark = SparkSession.builder.appName("gold_dim_products_scd2").getOrCreate()

# Define input path from silver layer and output path to gold layer
silver_path = "file:///data/silver/tbproducts"
gold_path = "file:///data/gold/dim_products"


# Helper function to print log messages with INFO prefix
def log(msg):
    print(f"[INFO] {msg}")


# Log the start of the SCD Type 2 process
log("Starting SCD Type 2 for dim_products")

# --------------------------------------------------
# Read Silver Layer Data
# --------------------------------------------------
# Read parquet files from silver layer and select/rename columns for dimension
silver_df = spark.read.parquet(silver_path).select(
    col("code").alias("product_code"),  # Product business key
    col("description").alias("product_description"),  # Product description
    col("salevalue").alias("sale_value"),  # Sale price
    col("active").alias("is_active"),  # Active flag (1 or 0)
    col("category").alias("category_code")  # Foreign key to category dimension
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
        .withColumn("product_sk", row_number().over(Window.orderBy("product_code")) - 1)
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
        # Left join to capture both existing and new products
        .join(active_gold_df.alias("g"), "product_code", "left")
        .select(
            col("product_code"),  # Business key
            col("s.product_description").alias("new_desc"),  # New description
            col("s.sale_value").alias("new_sale_value"),  # New sale value
            col("s.is_active").alias("new_is_active"),  # New active flag
            col("s.category_code").alias("new_category_code"),  # New category FK
            col("g.product_description").alias("old_desc"),  # Current description
            col("g.sale_value").alias("old_sale_value"),  # Current sale value
            col("g.is_active").alias("old_is_active"),  # Current active flag
            col("g.category_code").alias("old_category_code"),  # Current category FK
            col("g.product_sk")  # Existing surrogate key
        )
    )

    # Identify new products (not in gold yet) - old_desc is NULL
    new_df = comparison_df.filter(col("old_desc").isNull())

    # Identify changed products - exists in gold but at least one attribute changed
    changed_df = comparison_df.filter(
        col("old_desc").isNotNull() &  # Record exists in gold
        (
                (col("new_desc") != col("old_desc")) |  # Description changed
                (col("new_sale_value") != col("old_sale_value")) |  # Price changed
                (col("new_is_active") != col("old_is_active")) |  # Active status changed
                (col("new_category_code") != col("old_category_code"))  # Category changed
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
    max_sk = gold_cached.agg(spark_max("product_sk")).first()[0] or 0

    # Close old versions - set is_current to False and valid_to to current date
    expired_gold = gold_cached.withColumn(
        "is_current",
        # Set is_current to False for products that have changed
        when(
            col("product_code").isin(
                # Get list of changed product codes
                [r.product_code for r in changed_df.select("product_code").distinct().collect()]
            ),
            lit(False)
        ).otherwise(col("is_current"))  # Keep original value for unchanged records
    ).withColumn(
        "valid_to",
        # Set valid_to to current date when record becomes inactive
        when(col("is_current") == False, current_date()).otherwise(col("valid_to"))
    )

    # Create new versions for both new products and changed products
    new_versions = (
        # Select new products with new attribute values
        new_df.select(
            "product_code",
            col("new_desc").alias("product_description"),
            col("new_sale_value").alias("sale_value"),
            col("new_is_active").alias("is_active"),
            col("new_category_code").alias("category_code")
        )
        # Union with changed products (new versions)
        .union(
            changed_df.select(
                "product_code",
                col("new_desc").alias("product_description"),
                col("new_sale_value").alias("sale_value"),
                col("new_is_active").alias("is_active"),
                col("new_category_code").alias("category_code")
            )
        )
        # Generate new surrogate keys starting after max_sk
        .withColumn(
            "product_sk",
            row_number().over(Window.orderBy("product_code")) + max_sk
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