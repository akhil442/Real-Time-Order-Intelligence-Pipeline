from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    count,
    countDistinct,
    when,
    expr
)
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    DoubleType
)
import json

# ---------------------------------------
# Spark Session
# ---------------------------------------
spark = SparkSession.builder \
    .appName("StreamQualityMonitor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------
# Schema
# ---------------------------------------
schema = StructType() \
    .add("event_id", StringType()) \
    .add("event_type", StringType()) \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("product_id", StringType()) \
    .add("product_category", StringType()) \
    .add("quantity", IntegerType()) \
    .add("unit_price", DoubleType()) \
    .add("total_amount", DoubleType()) \
    .add("payment_method", StringType()) \
    .add("payment_status", StringType()) \
    .add("shipping_city", StringType()) \
    .add("shipping_state", StringType()) \
    .add("event_timestamp", StringType())

# ---------------------------------------
# Read Kafka stream
# ---------------------------------------
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce.orders.raw") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------
# Parse JSON
# ---------------------------------------
parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("event_timestamp"))

# ---------------------------------------
# Add quality flags
# ---------------------------------------
monitor_df = parsed_df.withColumn(
    "is_bad_record",
    when(
        col("event_id").isNull() |
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull() |
        col("product_category").isNull() |
        col("quantity").isNull() |
        col("unit_price").isNull() |
        col("total_amount").isNull() |
        col("payment_method").isNull() |
        col("payment_status").isNull() |
        col("shipping_city").isNull() |
        col("shipping_state").isNull() |
        col("event_time").isNull(),
        1
    ).otherwise(0)
).withColumn(
    "is_invalid_total_amount",
    when(col("total_amount") <= 0, 1).otherwise(0)
).withColumn(
    "is_invalid_quantity",
    when(col("quantity") <= 0, 1).otherwise(0)
).withColumn(
    "is_invalid_payment_status",
    when(~col("payment_status").isin("SUCCESS", "FAILED"), 1).otherwise(0)
).withColumn(
    "is_late_event",
    when(
        col("event_time") < expr("current_timestamp() - interval 2 minutes"),
        1
    ).otherwise(0)
)

METRICS_FILE = "/tmp/stream_metrics/metrics.json"

# ---------------------------------------
# Batch-level monitoring
# ---------------------------------------
def process_batch(batch_df, batch_id):
    print("\n" + "=" * 80)
    print(f"BATCH ID: {batch_id}")
    print("=" * 80)

    if batch_df.isEmpty():
        print("No records in this batch.")
        return

    summary = batch_df.agg(
        count("*").alias("raw_record_count"),
        countDistinct("event_id").alias("unique_event_id_count"),
        countDistinct("order_id").alias("unique_order_id_count")
    ).collect()[0]

    raw_record_count = summary["raw_record_count"]
    unique_event_id_count = summary["unique_event_id_count"]
    unique_order_id_count = summary["unique_order_id_count"]
    duplicate_count = raw_record_count - unique_event_id_count

    quality = batch_df.agg(
        count(when(col("is_bad_record") == 1, True)).alias("bad_record_count"),
        count(when(col("is_invalid_total_amount") == 1, True)).alias("invalid_total_amount_count"),
        count(when(col("is_invalid_quantity") == 1, True)).alias("invalid_quantity_count"),
        count(when(col("is_invalid_payment_status") == 1, True)).alias("invalid_payment_status_count"),
        count(when(col("is_late_event") == 1, True)).alias("late_event_count")
    ).collect()[0]

    metrics_payload = {
        "raw_record_count": int(raw_record_count),
        "unique_event_id_count": int(unique_event_id_count),
        "unique_order_id_count": int(unique_order_id_count),
        "duplicate_events_count": int(duplicate_count),
        "bad_record_count": int(quality["bad_record_count"]),
        "invalid_total_amount_count": int(quality["invalid_total_amount_count"]),
        "invalid_quantity_count": int(quality["invalid_quantity_count"]),
        "invalid_payment_status_count": int(quality["invalid_payment_status_count"]),
        "late_event_count": int(quality["late_event_count"])
    }

    with open(METRICS_FILE, "w") as f:
        json.dump(metrics_payload, f)

    print(f"RAW RECORD COUNT         : {raw_record_count}")
    print(f"UNIQUE EVENT ID COUNT    : {unique_event_id_count}")
    print(f"UNIQUE ORDER ID COUNT    : {unique_order_id_count}")
    print(f"DUPLICATE EVENTS IN BATCH: {duplicate_count}")
    print(f"BAD RECORD COUNT         : {quality['bad_record_count']}")
    print(f"INVALID TOTAL AMOUNT     : {quality['invalid_total_amount_count']}")
    print(f"INVALID QUANTITY         : {quality['invalid_quantity_count']}")
    print(f"INVALID PAYMENT STATUS   : {quality['invalid_payment_status_count']}")
    print(f"LATE EVENT COUNT         : {quality['late_event_count']}")

# ---------------------------------------
# Start streaming query
# ---------------------------------------
query = monitor_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()