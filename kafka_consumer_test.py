# docker run -d \
#  --name kafka-server \
#  -p 9092:9092 \
#  apache/kafka:latest
#
# pip install kafka-python-ng
#
# python3 kafka_consumer_test.py

import json
from kafka import KafkaConsumer

# 1. Initialize the Consumer
consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Start from the beginning of the topic
    enable_auto_commit=True,      # Automatically mark messages as "read"
    group_id='my-sensor-group',   # Consumers in the same group share the load
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")
try:
    for message in consumer:
        # The 'value' is already a dict thanks to value_deserializer
        reading = message.value
        print(f"Received from Partition {message.partition}: {reading['reading']} at {reading['timestamp']}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()