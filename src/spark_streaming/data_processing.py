from pyspark import SparkContext


def receive():

    sc=SparkContext(appName='KafkaWordCount')
    text_IBM = sc.textFile("hdfs://localhost:8020/tmp/stocks/IBM.text")



if __name__ == '__main__':

    receive()

# ds = sdfIBM \
#   .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("IBM") \
#   .start()