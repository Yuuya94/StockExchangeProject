import pyspark.sql
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType, FloatType, \
    IntegerType
from cassandra.cluster import Cluster

StockSchema = StructType([StructField("Volume", IntegerType()),
                          StructField("Value", FloatType()),
                          StructField("Timestamp", TimestampType())])

CurrencyRateSchema = StructType([StructField("From Currency Code", StringType()),
                                 StructField("To Currency Code", StringType()),
                                 StructField("Exchange Rate", FloatType()),
                                 StructField("Last Refreshed", TimestampType())])

spark = SparkSession.builder.appName("Spark Structured Streaming from Kafka").config(
    "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", False).getOrCreate()

spark.sparkContext.setLogLevel('WARN')

auth_provider = PlainTextAuthProvider(
    username='cassandra', password='cassandra')


def parse_currency_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    sdf = sdf.withColumn("value", from_json(sdf['value'], MapType(StringType(), StringType())))
    for idx, field in enumerate(schema):  # now expand col to multiple top-level columns
        sdf = sdf.withColumn(field.name, sdf['value'].getItem(field.name).cast(field.dataType))
    sdf = sdf.drop("value")
    sdf = sdf.withColumn("time", to_timestamp(col("Last Refreshed"), "yyyy-MM-dd HH:mm:ss.SSS"))
    sdf = sdf.drop("Last Refreshed")
    sdf = sdf.withColumnRenamed('From Currency Code', 'from_currency')
    sdf = sdf.withColumnRenamed('To Currency Code', 'to_currency')
    sdf = sdf.withColumnRenamed('Exchange Rate', 'exchange_rate')
    return sdf


def parse_stocks_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    sdf = sdf.withColumn('valueJSON', from_json(sdf['value'], MapType(StringType(), StringType()))).drop('value')
    for idx, field in enumerate(schema):  # now expand col to multiple top-level columns
        sdf = sdf.withColumn(field.name, sdf['valueJSON'].getItem(field.name).cast(field.dataType))
    sdf = sdf.withColumn('Stock', sdf['topic']).drop('topic').drop('valueJSON')
    sdf = sdf.withColumnRenamed('Stock', 'symbol')
    sdf = sdf.withColumnRenamed('Timestamp', 'time')
    sdf = sdf.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss.SSS"))
    sdf = sdf.withColumnRenamed('Volume', 'volume')
    sdf = sdf.withColumnRenamed('Value', 'value')
    return sdf


sdCurrencies = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "USD_and_RUB,USD_and_EUR") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

sdStocks = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "IBM,AAPL,AMZN,TSLA,BABA") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")

sdCurrencies = parse_currency_data_from_kafka_message(sdCurrencies, CurrencyRateSchema)

sdStocks = parse_stocks_data_from_kafka_message(sdStocks, StockSchema)


def foreach_cassandra_stocks(df):
    cluster = Cluster(["127.0.0.1"], auth_provider=auth_provider)  # Eventually modify
    session = cluster.connect("stock_exchange")
    session.execute("""INSERT INTO stocks (symbol, value, time, volume) VALUES (%s, %s, %s, %s)""",
                    (df.symbol, df.value, df.time, df.volume))


def foreach_cassandra_currencies(df):
    cluster = Cluster(["127.0.0.1"], auth_provider=auth_provider)  # Eventually modify
    session = cluster.connect("stock_exchange")
    session.execute(
        """INSERT INTO currencies (from_currency, to_currency, time, exchange_rate) VALUES (%s, %s, %s, %s)""",
        (df.from_currency, df.to_currency, df.time, df.exchange_rate))


def foreach_cassandra_currencies_stats(df):
    cluster = Cluster(["127.0.0.1"], auth_provider=auth_provider)  # Eventually modify
    session = cluster.connect("stock_exchange")
    session.execute(
        """INSERT INTO currencies (range_time, from_currency, to_currency, max_exchange_rate, min_exchange_rate, change) VALUES (%s, %s, %s, %s, %s, %s)""",
        (df.range_time, df.from_currency, df.to_currency, df.max_exchange_rate, df.min_exchange_rate, df.change))


windowCurrencyStats = sdCurrencies.withWatermark("time", "15 minutes"). \
    groupBy(window(col("time"), "15 minutes"), col("from_currency"), col("to_currency")) \
    .agg(pyspark.sql.functions.min("exchange_rate").alias('min_exchange_rate'),
         pyspark.sql.functions.max("exchange_rate").alias('max_exchange_rate'),
         pyspark.sql.functions.last("exchange_rate").alias('last_exchange_rate'),
         pyspark.sql.functions.first("exchange_rate").alias('first_exchange_rate'))

# Calculate the change between the first and last values of the window
windowCurrencyStats = windowCurrencyStats.withColumn("change",
                                                     (windowCurrencyStats['last_exchange_rate'] / windowCurrencyStats[
                                                         'first_exchange_rate']) - 1)
windowCurrencyStats = windowCurrencyStats.withColumnRenamed("window", "range_time")
windowCurrencyStats = windowCurrencyStats.drop('first_exchange_rate').drop('last_exchange_rate')

# debug queryWindow = windowCurrencyStats.writeStream.outputMode('append').format('console').option('truncate', \
# 'false').start()
# queryWindow.awaitTermination()


cassandra_query_currencies = sdCurrencies.writeStream.foreach(foreach_cassandra_currencies).start()
cassandra_query_stocks = sdStocks.writeStream.foreach(foreach_cassandra_stocks).start()
cassandra_query_currencies_stats = sdCurrencies.writeStream.foreach(foreach_cassandra_currencies_stats).start()
cassandra_query_stocks.awaitTermination()
cassandra_query_currencies.awaitTermination()
cassandra_query_currencies_stats.awaitTermination()
