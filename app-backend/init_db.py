import psycopg2
import os

def init_database():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    
    # Create tickets table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY,
            passenger_id VARCHAR(100),
            fare DECIMAL(10, 2),
            from_location VARCHAR(50),
            to_location VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create transactions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            customer_id VARCHAR(100),
            customer_email VARCHAR(255),
            total_cost DECIMAL(10, 2),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create notifications table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            customer_email VARCHAR(255),
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create reports summary table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reports_summary (
            id SERIAL PRIMARY KEY,
            total_tickets INTEGER DEFAULT 0,
            total_revenue DECIMAL(15, 2) DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert initial report record
    cur.execute("""
        INSERT INTO reports_summary (total_tickets, total_revenue)
        VALUES (0, 0)
        ON CONFLICT DO NOTHING
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✓ Database initialized successfully")

if __name__ == '__main__':
    import time
    time.sleep(5)  # Wait for postgres to be ready
    init_database()