from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BasicStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce.orders.raw") \
    .option("startingOffsets", "earliest") \
    .load()

query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()