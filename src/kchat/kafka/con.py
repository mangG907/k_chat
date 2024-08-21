from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
        "topic1",
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8')), 
        consumer_timeout_ms=15000
        )

print('[Start] get consumer')

for msg in consumer:
    print(msg)
print('[End] get consumer')

