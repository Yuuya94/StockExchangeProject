"""
Still working on it
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Structured Streaming from Kafka") \
    .getOrCreate()

sdfIBM = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "IBM") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest")\
    .load()\
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# sdfAAPL = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "AAPL") \
#     .option("startingOffsets", "latest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")
#
# sdfAMZN = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "AMZN") \
#     .option("startingOffsets", "latest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")
#
# sdfTSLA = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "TSLA") \
#     .option("startingOffsets", "latest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")
#
# sdfBABA = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "BABA") \
#     .option("startingOffsets", "latest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")
