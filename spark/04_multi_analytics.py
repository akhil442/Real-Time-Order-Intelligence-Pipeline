from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, round, window, count, when
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# -------------------------------
# 1. Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("KafkaMultiAnalytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# 2. Define Schema
# -------------------------------
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

# -------------------------------
# 3. Read Kafka Stream
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce.orders.raw") \
    .option("startingOffsets", "earliest") \
    .load()

# -------------------------------
# 4. Parse JSON Data
# -------------------------------
parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("event_timestamp").cast("timestamp"))

# -------------------------------
# 5. Data Quality Checks
# -------------------------------
validated_df = parsed_df.withColumn(
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
        col("event_time").isNull() |
        (col("quantity") <= 0) |
        (col("unit_price") <= 0) |
        (col("total_amount") <= 0) |
        (~col("payment_status").isin("SUCCESS", "FAILED")),
        True
    ).otherwise(False)
)

# -------------------------------
# 6. Split Clean vs Quarantine
# -------------------------------
quarantine_df = validated_df.filter(col("is_bad_record") == True)

candidate_clean_df = validated_df.filter(col("is_bad_record") == False).drop("is_bad_record")

# Dedup + late-event handling only on valid records
clean_df = candidate_clean_df \
    .withWatermark("event_time", "2 minutes") \
    .dropDuplicates(["event_id"])

# -------------------------------
# 7. Analytics on CLEAN stream only
# -------------------------------

# 1. Category Revenue
category_df = clean_df.groupBy("product_category") \
    .agg(round(sum("total_amount"), 2).alias("total_sales"))

# 2. Failed Payments
failed_df = clean_df.filter(col("payment_status") == "FAILED") \
    .groupBy("payment_method") \
    .count()

# 3. Top Cities
city_df = clean_df.groupBy("shipping_city") \
    .agg(round(sum("total_amount"), 2).alias("city_sales"))

# 4. Orders per Minute (Window)
orders_df = clean_df.groupBy(window(col("event_time"), "1 minute")) \
    .agg(count("*").alias("orders_count"))

# -------------------------------
# 8. OUTPUT STREAMS
# -------------------------------

# A. Store CLEAN trusted data
clean_query = clean_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/raw_orders_clean") \
    .option("checkpointLocation", "/tmp/checkpoints/raw_orders_clean") \
    .outputMode("append") \
    .start()

# B. Store QUARANTINED bad data
quarantine_query = quarantine_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/quarantine_orders") \
    .option("checkpointLocation", "/tmp/checkpoints/quarantine_orders") \
    .outputMode("append") \
    .start()

# C. Analytics → Console
q1 = category_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

q2 = failed_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

q3 = city_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

q4 = orders_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

# -------------------------------
# 9. Keep Streaming Running
# -------------------------------
spark.streams.awaitAnyTermination()