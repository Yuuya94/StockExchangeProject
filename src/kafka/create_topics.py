import time

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic

symbol_stocks = ['IBM', 'AAPL', 'AMZN', 'TSLA', 'BABA']
currencies = [('USD', 'RUB'), ('USD', 'EUR')]

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='admin')

consumer = KafkaConsumer(group_id='consumer', bootstrap_servers=['localhost:9092'])

# Delete topics
admin_client.delete_topics(list(consumer.topics()))
time.sleep(2)

# Create topics
topic_list = []
for stock in symbol_stocks:
    topic_list.append(NewTopic(name=stock, num_partitions=1, replication_factor=1
                               , topic_configs={'retention.ms': '86400000'}))
for (currency1, currency2) in currencies:
    topic_list.append(NewTopic(name=currency1 + "_and_" + currency2, num_partitions=1, replication_factor=1
                               , topic_configs={'retention.ms': '86400000'}))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
