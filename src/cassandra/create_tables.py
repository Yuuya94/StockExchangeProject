from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(
    username='cassandra', password='cassandra')
cluster = Cluster(["127.0.0.1"], auth_provider=auth_provider)  # Eventually modify
session = cluster.connect()

# Clean keyspace and tables
session.execute("DROP KEYSPACE IF EXISTS stock_exchange")
session.execute("DROP TABLE IF EXISTS stock_exchange.stocks")
session.execute("DROP TABLE IF EXISTS stock_exchange.currencies")
session.execute("DROP TABLE IF EXISTS stock_exchange.currencies")
session.execute("DROP TABLE IF EXISTS stock_exchange.currencies_stats")

# Create Keyspace
session.execute(
    "create keyspace stock_exchange with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};")

# Create tables
session.execute('''
create table stock_exchange.stocks (
 symbol varchar,
 value float,
 time timestamp,
 volume int,
 primary key(symbol, time)
);''')

session.execute('''
create table stock_exchange.currencies (
 from_currency varchar,
 to_currency varchar,
 time timestamp,
 exchange_rate float,
 primary key(from_currency, to_currency, time)
);''')

session.execute('''
create table stock_exchange.currencies_stats (
 start_time timestamp,
 end_time timestamp,
 from_currency varchar,
 to_currency varchar,
 max_exchange_rate float,
 min_exchange_rate float,
 change float,
 primary key(from_currency, to_currency, start_time, end_time)
);''')
