from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, round as spark_round
)

# --------------------------------------------------
# Spark Session Initialization
# --------------------------------------------------
# Create Spark session for fact table processing
spark = SparkSession.builder.appName("gold_fact_orders").getOrCreate()

# Define input paths from silver layer and output path to gold layer
silver_orders_path = "file:///data/silver/tborders"
silver_orderdetail_path = "file:///data/silver/tborderdetail"
gold_fact_path = "file:///data/gold/fact_orders"

# Helper function to print log messages with INFO prefix
def log(msg):
    print(f"[INFO] {msg}")

# Log the start of the fact table creation process
log("Starting fact_orders creation")

# --------------------------------------------------
# Read Silver Layer Data
# --------------------------------------------------
# Read orders table from silver layer
orders_df = spark.read.parquet(silver_orders_path).select(
    col("code").alias("order_code"),  # Order business key
    col("customer").alias("customer_code"),  # Foreign key to customer dimension
    col("orderdate").alias("order_date")  # Order timestamp
)

# Log successful loading of orders data
log("Orders table loaded from silver")

# Read order details table from silver layer
orderdetail_df = spark.read.parquet(silver_orderdetail_path).select(
    col("orders").alias("order_code"),  # Foreign key to orders table
    col("product").alias("product_code"),  # Foreign key to product dimension
    col("quantity"),  # Quantity sold
    col("salesvalue").alias("unit_price")  # Unit price
)

# Log successful loading of order details data
log("Order details table loaded from silver")

# --------------------------------------------------
# Join Orders with Order Details
# --------------------------------------------------
# Join orders with order details to get complete order information
joined_df = (
    orderdetail_df.alias("od")
    .join(
        orders_df.alias("o"),
        col("od.order_code") == col("o.order_code"),
        "inner"  # Inner join to keep only orders with details
    )
    .select(
        col("o.order_code"),  # Order identifier
        col("o.customer_code"),  # Customer who placed the order
        col("o.order_date"),  # When the order was placed
        col("od.product_code"),  # Product ordered
        col("od.quantity"),  # Quantity ordered
        col("od.unit_price")  # Unit price of the product
    )
)

# Log successful join
log("Orders and order details joined")

# --------------------------------------------------
# Calculate Line Total and Aggregate Fact Table
# --------------------------------------------------
# Calculate line total (quantity * unit_price) and aggregate by order, customer, date, and product
fact_df = (
    joined_df
    # Calculate total sales value for each line item
    .withColumn("line_total", col("quantity") * col("unit_price"))
    # Group by all dimension foreign keys
    .groupBy(
        "order_code",
        "customer_code",
        "order_date",
        "product_code"
    )
    # Aggregate measures
    .agg(
        spark_sum("quantity").alias("total_quantity"),  # Total quantity for this product in this order
        spark_sum("line_total").alias("total_sales"),  # Total sales value (quantity * price)
        count("*").alias("line_count")  # Number of detail lines (should be 1 per product per order)
    )
    # Round total_sales to 2 decimal places
    .withColumn("total_sales", spark_round(col("total_sales"), 2))
    # Reorder columns for better readability
    .select(
        "order_code",
        "customer_code",
        "order_date",
        "product_code",
        "total_quantity",
        "total_sales",
        "line_count"
    )
)

# Log successful fact table calculation
log("Fact table aggregated")

# --------------------------------------------------
# Display Sample Data Before Writing
# --------------------------------------------------
# Show sample data and statistics
print("\n" + "="*80)
print("FACT TABLE SAMPLE DATA")
print("="*80)
fact_df.show(20, truncate=False)

# Display fact table statistics
print("\n" + "="*80)
print("FACT TABLE STATISTICS")
print("="*80)
fact_df.describe().show()

# Count total records
total_records = fact_df.count()
log(f"Total fact records: {total_records}")

# --------------------------------------------------
# Write Fact Table to Gold Layer
# --------------------------------------------------
# Write fact table to gold layer in parquet format
fact_df.write.mode("overwrite").parquet(gold_fact_path)

# Log successful write
log("Fact table written to gold layer")

# --------------------------------------------------
# Validate Written Data
# --------------------------------------------------
# Read back the written data to validate
validation_df = spark.read.parquet(gold_fact_path)

# Display validation info
print("\n" + "="*80)
print("VALIDATION - SCHEMA OF WRITTEN FACT TABLE")
print("="*80)
validation_df.printSchema()

# Count records in written file
written_count = validation_df.count()
log(f"Validation: {written_count} records written successfully")

# Verify counts match
if written_count == total_records:
    log("✓ Validation successful: Record counts match")
else:
    log(f"✗ Warning: Record count mismatch - Expected {total_records}, Got {written_count}")

# --------------------------------------------------
# Summary Statistics
# --------------------------------------------------
# Create temporary view for SQL queries
fact_df.createOrReplaceTempView("fact_orders")

# Show summary by customer
print("\n" + "="*80)
print("TOP 10 CUSTOMERS BY TOTAL SALES")
print("="*80)
spark.sql("""
    SELECT
        customer_code,
        COUNT(DISTINCT order_code) as total_orders,
        SUM(total_quantity) as total_items,
        ROUND(SUM(total_sales), 2) as total_revenue
    FROM fact_orders
    GROUP BY customer_code
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

# Show summary by product
print("\n" + "="*80)
print("TOP 10 PRODUCTS BY TOTAL SALES")
print("="*80)
spark.sql("""
    SELECT
        product_code,
        COUNT(DISTINCT order_code) as times_ordered,
        SUM(total_quantity) as total_quantity_sold,
        ROUND(SUM(total_sales), 2) as total_revenue
    FROM fact_orders
    GROUP BY product_code
    ORDER BY total_revenue DESC
    LIMIT 10
""").show(truncate=False)

# Show daily sales summary
print("\n" + "="*80)
print("DAILY SALES SUMMARY (LAST 10 DAYS)")
print("="*80)
spark.sql("""
    SELECT
        DATE(order_date) as sales_date,
        COUNT(DISTINCT order_code) as total_orders,
        COUNT(DISTINCT customer_code) as unique_customers,
        SUM(total_quantity) as total_items,
        ROUND(SUM(total_sales), 2) as daily_revenue
    FROM fact_orders
    GROUP BY DATE(order_date)
    ORDER BY sales_date DESC
    LIMIT 10
""").show(truncate=False)

# Stop Spark session
spark.stop()

# Log completion
log("Fact table creation completed successfully")