# python3 kafka_producer_test.py

import json
import time
from kafka import KafkaProducer

# 1. Initialize the Producer
# value_serializer handles converting our dict to bytes automatically
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'sensor-data'

print(f"Starting producer on topic: {topic}")
try:
    for i in range(10):
        data = {
            'sensor_id': 101,
            'reading': 20.5 + i,
            'timestamp': time.time()
        }
        
        # 2. Send the message
        producer.send(topic, value=data)
        print(f"Sent: {data}")
        
        time.sleep(2)
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    # 3. Important: Flush ensures all messages are sent before closing
    producer.flush()
    producer.close()