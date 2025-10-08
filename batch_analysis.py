from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Batch Transaction Analysis") \
    .getOrCreate()

# Step 2: Load the JSON data
df = spark.read.json("transactions.json")

# Step 3: Show schema and a few records
df.printSchema()
df.show(5, truncate=False)

# Step 4: Commission Summary
commission_df = df.groupBy("merchant_category").agg(
    sum("commission_amount").alias("total_commission"),
    avg("commission_amount").alias("avg_commission"),
    count("*").alias("txn_count")
)

commission_df = commission_df.withColumn(
    "commission_per_txn", col("total_commission") / col("txn_count")
)

# Step 5: Show the result
commission_df.show(truncate=False)

# Optional: Save as CSV
# commission_df.coalesce(1).write.csv("commission_summary.csv", header=True)

spark.stop()
