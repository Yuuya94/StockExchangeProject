from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka_to_spark_streaming import sdIBM

def receive():


    sc=SparkContext(appName='StockCalcul')
    ssc=StreamingContext(sc, 2)


    ssc.awaitTermination(timeout=5)
    rdd = sc.parallelize(sdIBM)
    print(rdd.collect())


    # save to HDFS
    rdd.saveAsTextFile('hdfs://localhost:8020/spark/')
    #
    # # read from HDFS
    # rdd = sc.textFile("hdfs://127.0.0.1:8020/spark/")


    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate

if __name__ == '__main__':

    receive()

# ds = sdfIBM \
#   .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("IBM") \
#   .start()