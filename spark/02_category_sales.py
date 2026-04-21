from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, round
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("CategorySales") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce.orders.raw") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

agg_df = parsed_df.groupBy("product_category") \
    .agg(round(sum("total_amount"), 2).alias("total_sales"))

query = agg_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()