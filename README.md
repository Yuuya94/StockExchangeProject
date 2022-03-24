# StockExchangeProject

Architecture Big Data Project from IMT Atlantique


Made by : Thierry JIAO and Xinyan DU

## Description

Our project is about the study of StockExchange through a Big Data Architecture.
The goal is to process financial stocks through Lambda Architecture (batch and real-time).

## Getting Started

### Dependencies

* Python
* Docker & docker-compose
* Spark
* Hadoop

### Installing

First, please install the python packages :

```bash
$ pip install -r requirements.txt
```

Please install the docker containers by executing these commands :

```bash
$ cd docker
$ sudo docker-compose up -d
```

You can check the status of the containers :

```bash
$ sudo docker ps
```

You can also verify that cassandra is running :

```bash
$ cqlsh localhost -u cassandra -p cassandra
```


Then, run spark master and one spark worker. 

## Executing program

Run the "create_topics.py" script :

```bash
$ python ./src/kafka/create_topics.py
```

Then, run the "create_tables.py" script :

```bash
$ python ./src/cassandra/create_tables.py
```

After that, run the following command :

```bash
$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --master spark://[spark_master_address]:7077 ./src/spark_streaming/kafka_to_spark_streaming.py
```

At this moment, the spark streaming script will be submitted to Spark master.

Now we have to feed the Kafka Broker.

Run this :

```bash
$ python ./src/kafka/input_kafka_producer.py
```

As a result, data will be sent to Kafka and Spark Streaming will decode, clean and insert it to Cassandra.