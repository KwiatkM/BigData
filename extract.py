import yfinance as yf
import datetime
import json
from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers='localhost:29092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


while True:
    msft = yf.Ticker("MSFT")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('MSFT', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': msft.info['currentPrice']})
    sleep(5)

    aapl = yf.Ticker("AAPL")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('AAPL', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': aapl.info['currentPrice']})
    sleep(5)

    goog = yf.Ticker("GOOG")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('GOOG', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': goog.info['currentPrice']})
    sleep(5)

    nvda = yf.Ticker("NVDA")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('NVDA', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': nvda.info['currentPrice']})
    sleep(5)

    intc = yf.Ticker("INTC")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('INTC', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': intc.info['currentPrice']})
    sleep(5)

    amd = yf.Ticker("AMD")
    date_utc = datetime.datetime.now(datetime.timezone.utc)
    producer.send('AMD', {'date': date_utc.strftime("%Y-%m-%d %H:%M%z"),
                           'price': amd.info['currentPrice']})
    sleep(5)

    sleep(30)


