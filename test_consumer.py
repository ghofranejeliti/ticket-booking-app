import json
from kafka import KafkaConsumer

print("Testing Kafka consumer...")

consumer = KafkaConsumer(
    'ticket_details',
    bootstrap_servers='localhost:9092',
    group_id='debug-consumer',      # 👈 IMPORTANT
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',   # 👈 FORCE replay
    enable_auto_commit=False,
    consumer_timeout_ms=5000
)

print("✓ Connected to Kafka")
print("Attempting to read messages...")

count = 0
for message in consumer:
    print(f"Message {count + 1}: {message.value}")
    count += 1
    if count >= 5:
        break

if count == 0:
    print("✗ No messages received")
else:
    print(f"✓ Received {count} messages")
