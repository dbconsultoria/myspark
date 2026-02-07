from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Create Spark session
spark = (
    SparkSession.builder
    .appName("bronze_tborderdetail")
    .getOrCreate()
)

# JDBC connection properties
jdbc_url = "jdbc:mysql://mysql_mydb:3306/mydb"
jdbc_properties = {
    "user": "myusr",
    "password": "mypswd",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read source table (full load)
df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="tborderdetail",
        properties=jdbc_properties
    )
)

# Add minimal ingestion metadata
df_bronze = (
    df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("mysql"))
)

# Write to Bronze layer
(
    df_bronze
    .write
    .mode("overwrite")  # full reload Bronze
    .parquet("file:///data/bronze/mysql/tborderdetail")
)

spark.stop()
