import datetime

from kafka_to_spark_streaming import sdStocks

def save_hdfs():

    sdStocks.writeStream() \
        .queryName("Persist the processed data") \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://localhost:8020/tmp/stocks") \
        .option("checkpointLocation", "hdfs://localhost:8020/tmp/checkpoints/") \
        .partitionBy("time") \
        .option("truncate", False) \
        .start()