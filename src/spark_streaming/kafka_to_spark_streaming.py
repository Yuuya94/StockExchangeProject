"""
Still working on it
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, FloatType, \
    IntegerType
from pyspark.sql.functions import split, from_json, regexp_extract, regexp_replace

StockSchema = StructType([StructField("Volume", IntegerType()),
                          StructField("Value", FloatType()),
                          StructField("Timestamp", TimestampType())])

CurrencyRateSchema = StructType([StructField("From Currency Code", StringType()),
                                 StructField("To Currency Code", StringType()),
                                 StructField("Exchange Rate", FloatType()),
                                 StructField("Last Refreshed", TimestampType())])


# def parse_data_from_kafka_message(sdf, schema):
#     assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
#     sdf = sdf.withColumn("value", from_json(sdf.value, MapType(StringType(), StringType())))
#     for idx, field in enumerate(schema):  # now expand col to multiple top-level columns
#         # logging.error("field.name : " + field.name)
#         # logging.error("col.getItem(idx) : " + col.getItem(idx))
#         temp = sdf.withColumn('new', split(sdf.value.getItem("Time Series (5min)"), "{"))
#         return temp
#         # sdf = sdf.withColumn(field.name, sdf.value.getItem("Time Series (5min)").cast(field.dataType))
#         # return sdf.select([field.name for field in schema])
#
#     # col = split(sdf['value'], ',')  # split attributes to nested array in one Column
#     # logging.error(col)
#     # for idx, field in enumerate(schema):     # now expand col to multiple top-level columns
#     #     logging.error("field.name : " + field.name)
#     #     logging.error("col.getItem(idx) : " + col.getItem(idx))
#     #     sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
#     #     return sdf.select([field.name for field in schema])

def parse_currency_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    sdf = sdf.withColumn("value", from_json(sdf['value'], MapType(StringType(), StringType())))
    for idx, field in enumerate(schema):  # now expand col to multiple top-level columns
        sdf = sdf.withColumn(field.name, sdf['value'].getItem(field.name).cast(field.dataType))
    sdf = sdf.drop("value")
    return sdf


def parse_stocks_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    sdf = sdf.withColumn('valueJSON', from_json(sdf['value'], MapType(StringType(), StringType()))).drop('value')
    for idx, field in enumerate(schema):  # now expand col to multiple top-level columns
        sdf = sdf.withColumn(field.name, sdf['valueJSON'].getItem(field.name).cast(field.dataType))
    sdf = sdf.withColumn('Stock', sdf['topic']).drop('topic').drop('valueJSON')
    return sdf


spark = SparkSession.builder.appName("Spark Structured Streaming from Kafka").getOrCreate()
#
# sdUSDtoRUB = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "USD_and_RUB,USD_and_EUR") \
#     .option("startingOffsets", "earliest") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")

sdIBM = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "IBM,AAPL,AMZN,TSLA,BABA") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")

# sdUSDtoRUB = parse_currency_data_from_kafka_message(sdUSDtoRUB, CurrencyRateSchema)

sdIBM = parse_stocks_data_from_kafka_message(sdIBM, StockSchema)

# query_currency = sdUSDtoRUB.writeStream.outputMode("append").format("console").start().awaitTermination()

query_stock = sdIBM.writeStream.outputMode("append").format("console").start().awaitTermination()