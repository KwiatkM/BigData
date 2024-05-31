from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
from ast import literal_eval

#kafka
consumer = KafkaConsumer('MSFT', 'AAPL', 'GOOG', 'NVDA', 'INTC', 'AMD', bootstrap_servers='localhost:29092')

# cassandra
keyspace = 'bigdata'
cluster = Cluster(['localhost'])
session = cluster.connect()
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
session.set_keyspace(keyspace=keyspace)

session.execute(f"CREATE TABLE IF NOT EXISTS {'stock_data'} (name text, timestamp_ timestamp, price float, PRIMARY KEY (name, timestamp_))")



for msg in consumer:
    d = literal_eval(msg.value.decode("utf-8"))
    p = d.get('price')
    t = d.get('date')
    n =  msg.topic
    #print(t)
    table_name = msg.topic.upper() + "_stock_data"
    query = SimpleStatement(f"INSERT INTO {'stock_data'} (name, timestamp_, price) VALUES ('{n}','{t}', {p})")
    session.execute(query)
    print(str(msg.topic) + " " + str(t) + " " + str(p))