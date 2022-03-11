from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka_to_spark_streaming import sdfIBM

def receive():


    sc=SparkContext(appName='KafkaWordCount')
    ssc=StreamingContext(sc, 2)

    ssc.awaitTermination(timeout=5)
    lines = ssc.queueStream(sdfIBM)
    print(lines)

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