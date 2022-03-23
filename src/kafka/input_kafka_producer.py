import json
import threading
import time
import urllib.request
import logging
from json import dumps

from kafka import KafkaProducer

logging.getLogger().setLevel(logging.INFO)

# Alpha Vintage Keys
api_keys = ['TJ1BIZPW8CW92HPV', '719F2LKYBXADC9JS', 'CJDRP98HYVLSQ2WN', 'KQ953784UIXYDCGE', 'L0DNST10AIKFNMIT',
            'N127QDY29MMRV8Y2', 'OWO2UGRNDJU4O723', 'O6038C7684U34G04', 'XHHKK0VSMLTK0Z7M', 'FKZ8Y0Q50DVE8FGQ']

symbol_stocks = ['IBM', 'AAPL', 'AMZN', 'TSLA', 'BABA']
currencies = [('USD', 'RUB'), ('USD', 'EUR')]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))


class TimeSeriesIntraday(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.index_api_keys = 0
        self.last_time_refreshed_stocks = {}
        for stock in symbol_stocks:
            self.last_time_refreshed_stocks[stock] = ""

    def run(self):
        while True:
            # Request for stocks values
            for stock in symbol_stocks:
                clean_data_list = []
                url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + stock + \
                      "&interval=5min&apikey=" + api_keys[self.index_api_keys]
                response = urllib.request.urlopen(url)
                data = json.loads(response.read().decode())
                # Check that the data is correct and that it is new
                if 'Meta Data' in data and '3. Last Refreshed' in data['Meta Data'] and \
                        self.last_time_refreshed_stocks[stock] != data['Meta Data']['3. Last Refreshed']:
                    for timestamp, symbol_dict in data['Time Series (5min)'].items():
                        clean_data_list.append({
                            'Timestamp': str(timestamp) + '.000',
                            'Value': float(symbol_dict['4. close']),
                            'Volume': int(symbol_dict['5. volume'])
                        })
                    # Reverse the list of data to send so that they are now ordered correctly by timestamp
                    clean_data_list.reverse()
                    # Send the data to Kafka through a producer
                    for clean_data in clean_data_list:
                        producer.send(stock, value=clean_data)
                        producer.flush()
                        logging.info(stock + ' stocks successfully sent to Kafka Broker')
                    self.last_time_refreshed_stocks[stock] = data['Meta Data']['3. Last Refreshed']
                else:
                    logging.warning(str(self.index_api_keys) + ' index key being used')
                    logging.warning(
                        'TimeSeriesIntraday : No data were found (either the api key is over used, either there is no data '
                        'or there is no new data)')
                time.sleep(30)
                self.index_api_keys = (self.index_api_keys + 1) % len(api_keys)
            # Wait every hour
            time.sleep(60 * 60 * 3)


class CurrencyExchangeRate(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.index_api_keys = 5

    def run(self):
        while True:
            # Request for stocks values
            for (currency1, currency2) in currencies:
                url = "https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=" + currency1 + \
                      "&to_currency=" + currency2 + "&apikey=" + api_keys[self.index_api_keys]
                response = urllib.request.urlopen(url)
                data = json.loads(response.read().decode())
                if 'Realtime Currency Exchange Rate' in data:
                    clean_data = {
                        'From Currency Code': data['Realtime Currency Exchange Rate']['1. From_Currency Code'],
                        'To Currency Code': data['Realtime Currency Exchange Rate']['3. To_Currency Code'],
                        'Exchange Rate': float(data['Realtime Currency Exchange Rate']['5. Exchange Rate']),
                        'Last Refreshed': data['Realtime Currency Exchange Rate']['6. Last Refreshed'] + '.000'
                    }
                    producer.send(currency1 + "_and_" + currency2, value=clean_data)
                    producer.flush()
                    logging.info(currency1 + ' and ' + currency2 +
                                 ' Currency Exchange Rate successfully sent to Kafka Broker')
                else:
                    logging.warning(str(self.index_api_keys) + ' index key being used')
                    logging.warning(
                        'CurrencyExchangeRate : No data were found (either the api key is over used, either there is no data '
                        'or there is no new data)')
                self.index_api_keys = (self.index_api_keys + 1) % len(api_keys)
            # Wait every 3 minutes
            time.sleep(3 * 60)


currencyExchangeRate = CurrencyExchangeRate()
currencyExchangeRate.start()

timeSeriesIntraday = TimeSeriesIntraday()
timeSeriesIntraday.start()
