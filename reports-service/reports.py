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

order_count = 0
total_revenue = 0

print('Reports is listening')
while True:
    for message in consumer:
        consumed_message = message.value
        total_cost = float(consumed_message['total_cost'])

        order_count += 1
        total_revenue += total_cost

        # Update database
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE reports_summary 
                SET total_tickets = %s, total_revenue = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (order_count, total_revenue))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")

        print(f'Total tickets generated: {order_count}')
        print(f'Total revenue generated: {total_revenue}')