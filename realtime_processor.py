from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp, hour, when
from pyspark.sql.types import *
import psycopg2

# Define schema of incoming Kafka JSON
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", LongType()),
    StructField("risk_level", LongType()),
    StructField("payment_method", StringType()),
    StructField("device_info", StructType([
        StructField("os", StringType())
    ]))
])

# Create Spark session
spark = SparkSession.builder \
    .appName("RealTime Fraud Detector") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read Kafka stream
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "darooghe.transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (bytes) to string and parse JSON
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")

# Convert timestamp and extract hour
parsed_df = json_df.withColumn("ts", to_timestamp(col("timestamp"))) \
    .withColumn("hour", hour("ts"))

# Apply fraud detection rules
alert_df = parsed_df.withColumn("alert_reason",
    when((col("hour") >= 0) & (col("hour") < 6), "LATE_HOURS")
    .when((col("risk_level") > 2) & (col("amount") > 1000000), "HIGH_RISK_HIGH_AMOUNT")
    .when((col("payment_method") == "mobile") &
          (~col("device_info.os").isin("iOS", "Android")), "DEVICE_MISMATCH")
)

# Only keep rows with alerts
final_alerts = alert_df.filter(col("alert_reason").isNotNull()) \
    .selectExpr("CAST(transaction_id AS STRING) AS key",
                "to_json(struct(*)) AS value")

# Function to write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_data = batch_df.select(
        "transaction_id", "timestamp", "amount", "risk_level",
        "payment_method", "device_info.os", "alert_reason"
    ).collect()

    conn = psycopg2.connect(
        dbname="kafka_alerts",
        user="kafka_user",
        password="kafka_pass",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    for row in batch_data:
        cur.execute("""
            INSERT INTO alerts (transaction_id, timestamp, amount, risk_level, payment_method, device_os, alert_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            row["transaction_id"], row["timestamp"], row["amount"], row["risk_level"],
            row["payment_method"], row["os"], row["alert_reason"]
        ))

    conn.commit()
    cur.close()
    conn.close()

# Start streaming query to PostgreSQL
query = alert_df.filter(col("alert_reason").isNotNull()) \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/fraud-checkpoint") \
    .start()

# Also stream alerts to Kafka
kafka_query = final_alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "darooghe.alerts") \
    .option("checkpointLocation", "/tmp/fraud-kafka-checkpoint") \
    .start()

query.awaitTermination()
kafka_query.awaitTermination()
