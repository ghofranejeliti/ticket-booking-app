import json
import time
import os
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

TICKET_KAFKA_TOPIC = 'ticket_details'
CONFIRMED_TICKET_KAFKA_TOPIC = 'ticket_confirmed'

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def create_kafka_consumer(max_retries=30, retry_interval=2):
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                TICKET_KAFKA_TOPIC,
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print('✓ Consumer successfully connected to Kafka')
            return consumer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f'⏳ Kafka not ready for consumer, retrying in {retry_interval}s... ({attempt + 1}/{max_retries})')
                time.sleep(retry_interval)
            else:
                print('✗ Failed to connect consumer to Kafka after all retries')
                raise

def create_kafka_producer(max_retries=30, retry_interval=2):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print('✓ Producer successfully connected to Kafka')
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f'⏳ Kafka not ready for producer, retrying in {retry_interval}s... ({attempt + 1}/{max_retries})')
                time.sleep(retry_interval)
            else:
                print('✗ Failed to connect producer to Kafka after all retries')
                raise

consumer = create_kafka_consumer()
producer = create_kafka_producer()

print('Transaction is listening')
while True:
    for message in consumer:
        print('Transaction in progress')
        consumed_message = message.value
        print(consumed_message)

        passenger_id = consumed_message['passenger_id']
        fare = consumed_message['fare']

        data = {
            'customer_id': passenger_id,
            'customer_email': f'{passenger_id}@mail.com',
            'total_cost': fare
        }

        # Save to database
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO transactions (customer_id, customer_email, total_cost)
                VALUES (%s, %s, %s)
            """, (data['customer_id'], data['customer_email'], data['total_cost']))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")

        print('Transaction done successfully')
        producer.send(CONFIRMED_TICKET_KAFKA_TOPIC, value=data)