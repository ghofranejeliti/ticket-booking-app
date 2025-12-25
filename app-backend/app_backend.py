import time
import json
import os
import psycopg2
from flask_cors import CORS
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)
CORS(app)
TICKET_KAFKA_TOPIC = 'ticket_details'
TICKET_LIMIT = 72

producer = None

def get_db_connection():
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def get_kafka_producer(max_retries=30, retry_interval=2):
    global producer
    if producer is not None:
        return producer
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print('✓ Successfully connected to Kafka')
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f'⏳ Kafka not ready, retrying in {retry_interval}s... ({attempt + 1}/{max_retries})')
                time.sleep(retry_interval)
            else:
                print('✗ Failed to connect to Kafka after all retries')
                raise

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'service': 'app-backend'}), 200

@app.route('/generate-tickets', methods=['POST'])
def generate_tickets():
    try:
        prod = get_kafka_producer()
        conn = get_db_connection()
        cur = conn.cursor()
        
        print('Begin ticket processing')
        
        for ticket in range(1, TICKET_LIMIT + 1):
            data = {
                'ticket_id': ticket,
                'passenger_id': f"limoo_{ticket}",
                'fare': 1000,
                'fro': 'nrb',
                'to': 'eld'
            }
            
            # Save to database
            cur.execute("""
                INSERT INTO tickets (ticket_id, passenger_id, fare, from_location, to_location)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ticket_id) DO NOTHING
            """, (ticket, data['passenger_id'], data['fare'], data['fro'], data['to']))
            
            # Send to Kafka
            prod.send(TICKET_KAFKA_TOPIC, value=data)
            print(f'Ticket {ticket}')
        
        conn.commit()
        cur.close()
        conn.close()
        prod.flush()
        print('✓ All tickets processed')
        
        return jsonify({'message': f'{TICKET_LIMIT} tickets generated successfully'}), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/tickets', methods=['GET'])
def get_tickets():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tickets ORDER BY ticket_id")
        tickets = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'tickets': [
                {
                    'ticket_id': t[0],
                    'passenger_id': t[1],
                    'fare': float(t[2]),
                    'from': t[3],
                    'to': t[4],
                    'created_at': str(t[5])
                } for t in tickets
            ]
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/reports', methods=['GET'])
def get_reports():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM reports_summary ORDER BY id DESC LIMIT 1")
        report = cur.fetchone()
        cur.close()
        conn.close()
        
        if report:
            return jsonify({
                'total_tickets': report[1],
                'total_revenue': float(report[2]),
                'updated_at': str(report[3])
            }), 200
        return jsonify({'total_tickets': 0, 'total_revenue': 0}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/notifications', methods=['GET'])
def get_notifications():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT customer_email, sent_at FROM notifications ORDER BY sent_at DESC LIMIT 100")
        notifications = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'notifications': [
                {
                    'email': n[0],
                    'sent_at': str(n[1])
                } for n in notifications
            ]
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/transactions', methods=['GET'])
def get_transactions():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM transactions ORDER BY processed_at DESC LIMIT 100")
        transactions = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'transactions': [
                {
                    'id': t[0],
                    'customer_id': t[1],
                    'customer_email': t[2],
                    'total_cost': float(t[3]),
                    'processed_at': str(t[4])
                } for t in transactions
            ]
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Initialize database
    time.sleep(5)
    try:
        from init_db import init_database
        init_database()
    except Exception as e:
        print(f"Database init error: {e}")
    
    print('Starting Flask app...')
    app.run(host='0.0.0.0', port=5000, debug=True)