import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

CONFIRMED_TICKET_KAFKA_TOPIC = 'ticket_confirmed'

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def create_kafka_consumer(max_retries=30, retry_interval=2):
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                CONFIRMED_TICKET_KAFKA_TOPIC,
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print('✓ Successfully connected to Kafka')
            return consumer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f'⏳ Kafka not ready, retrying in {retry_interval}s... ({attempt + 1}/{max_retries})')
                time.sleep(retry_interval)
            else:
                print('✗ Failed to connect to Kafka after all retries')
                raise

consumer = create_kafka_consumer()

mails_sent = set()
print('Listening for notifications')
while True:
    for message in consumer:
        consumed_message = message.value
        customer_email = consumed_message['customer_email']
        
        # Save to database
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO notifications (customer_email)
                VALUES (%s)
            """, (customer_email,))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")
        
        print(f'Sending ticket notification to {customer_email}')
        mails_sent.add(customer_email)
        print(f'Total mails sent: {len(mails_sent)} unique mails')