from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("mysql-connection-test")
    .getOrCreate()
)

df = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://mysql_mydb:3306/mydb")
    .option("dbtable", "information_schema.tables")
    .option("user", "myusr")
    .option("password", "mypswd")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
)

df.show(5)
spark.stop()
