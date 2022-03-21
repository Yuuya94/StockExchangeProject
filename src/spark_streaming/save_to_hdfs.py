
import datetime

from kafka_to_spark_streaming import sdIBM

def save_hdfs():

    # now_time = datetime.datetime.now()
    # now_time = now_time.strftime("%d-%m-%Y")

    # hdfs_path = "hdfs://localhost:8020/tmp/"

    hdfs_path = "hdfs://localhost:8020/tmp/spark/"

    # query = sdIBM.writeStream() \
    # .outputMode("append") \
    # .option("checkpointLocation", "hdfs://localhost:8020/spark/") \
    # .option("path", hdfs_path) \
    # .start()

    sdIBM.writeStream() \
        .queryName("Persist the processed data") \
        .outputMode("append") \
        .format("parquet") \
        .option("path", hdfs_path + "stock") \
        .option("checkpointLocation", hdfs_path + "stock") \
        .start() \
        .awaitTermination()

