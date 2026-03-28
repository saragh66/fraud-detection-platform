import pandas as pd
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('data/fraudTrain.csv')
print(f"Sending {len(df)} transactions to Kafka...")

for i, row in df.iterrows():
    message = row.to_dict()
    producer.send('fraud-transactions', value=message)
    if i % 1000 == 0:
        print(f"Sent {i} transactions...")
    time.sleep(0.01)

producer.flush()
print("Done!")