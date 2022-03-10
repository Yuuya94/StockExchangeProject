import json
import time
import urllib.request
import logging
from json import dumps

from kafka import KafkaProducer

interval_time = 5  # min

api_keys = ['TJ1BIZPW8CW92HPV', '719F2LKYBXADC9JS', 'CJDRP98HYVLSQ2WN', 'KQ953784UIXYDCGE', 'L0DNST10AIKFNMIT',
            'N127QDY29MMRV8Y2', 'OWO2UGRNDJU4O723', 'O6038C7684U34G04', 'XHHKK0VSMLTK0Z7M', 'FKZ8Y0Q50DVE8FGQ']
index_api_keys = 0

symbol_stocks = ['IBM', 'AAPL', 'AMZN', 'TSLA', 'BABA']

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    # Request for stocks values
    for stock in symbol_stocks:
        url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + stock + "&interval=" + str(
            interval_time) + "min&apikey=" + api_keys[index_api_keys]
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode())
        if 'Meta Data' in data:
            producer.send(stock, value=data)
            producer.flush()
            logging.info(stock + ' stocks successfully sent to Kafka Broker')
        else:
            logging.error('Error : the api key is over used')
        index_api_keys = (index_api_keys + 1) % len(api_keys)
    # Wait every 5 min
    time.sleep(interval_time * 60)
