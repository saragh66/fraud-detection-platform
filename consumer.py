from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'fraud-transactions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for transactions...")
count = 0
for message in consumer:
    transaction = message.value
    count += 1
    if count % 1000 == 0:
        print(f"Received {count} transactions | Latest: amt=${transaction.get('amt', 0):.2f} | fraud={transaction.get('is_fraud', 0)}")