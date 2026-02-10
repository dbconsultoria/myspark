from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, when,
    max as spark_max, row_number
)
from pyspark.sql.window import Window

# --------------------------------------------------
# Spark
# --------------------------------------------------
spark = SparkSession.builder.appName("gold_dim_categories_scd2").getOrCreate()

silver_path = "file:///data/silver/tbcategories"
gold_path = "file:///data/gold/dim_categories"

def log(msg):
    print(f"[INFO] {msg}")

log("Starting SCD Type 2 for dim_categories")

# --------------------------------------------------
# Read Silver
# --------------------------------------------------
silver_df = spark.read.parquet(silver_path).select(
    col("code").alias("category_code"),
    col("description").alias("category_description")
)

log("Silver loaded")

# --------------------------------------------------
# Check Gold existence
# --------------------------------------------------
try:
    gold_df = spark.read.parquet(gold_path)
    gold_exists = True
    log("Gold detected – incremental mode")
except Exception:
    gold_exists = False
    log("Gold not found – initial load")

# --------------------------------------------------
# INITIAL LOAD
# --------------------------------------------------
if not gold_exists:
    final_gold_df = (
        silver_df
        .withColumn("category_sk", row_number().over(Window.orderBy("category_code")) - 1)
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    final_gold_df.write.mode("overwrite").parquet(gold_path)
    log("Initial Gold created")

# --------------------------------------------------
# INCREMENTAL SCD2
# --------------------------------------------------
else:
    active_gold_df = gold_df.filter(col("is_current"))

    comparison_df = (
        silver_df.alias("s")
        .join(active_gold_df.alias("g"), "category_code", "left")
        .select(
            col("category_code"),
            col("s.category_description").alias("new_desc"),
            col("g.category_description").alias("old_desc"),
            col("g.category_sk")
        )
    )

    new_df = comparison_df.filter(col("old_desc").isNull())
    changed_df = comparison_df.filter(
        col("old_desc").isNotNull() & (col("new_desc") != col("old_desc"))
    )

    if new_df.limit(1).count() == 0 and changed_df.limit(1).count() == 0:
        log("No changes detected – Gold unchanged")
        spark.stop()
        exit(0)

    log("Changes detected – applying SCD2")

    gold_cached = gold_df.cache()
    max_sk = gold_cached.agg(spark_max("category_sk")).first()[0] or 0

    # Close old records
    expired_gold = gold_cached.withColumn(
        "is_current",
        when(
            col("category_code").isin(
                [r.category_code for r in changed_df.select("category_code").distinct().collect()]
            ),
            lit(False)
        ).otherwise(col("is_current"))
    ).withColumn(
        "valid_to",
        when(col("is_current") == False, current_date()).otherwise(col("valid_to"))
    )

    # New versions
    new_versions = (
        new_df.select("category_code", col("new_desc").alias("category_description"))
        .union(changed_df.select("category_code", col("new_desc").alias("category_description")))
        .withColumn(
            "category_sk",
            row_number().over(Window.orderBy("category_code")) + max_sk
        )
        .withColumn("valid_from", current_date())
        .withColumn("valid_to", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    final_gold_df = expired_gold.unionByName(new_versions)

    final_gold_df.write.mode("overwrite").parquet(gold_path)
    gold_cached.unpersist()

    log("Gold updated successfully")

spark.stop()
log("SCD Type 2 process completed")
