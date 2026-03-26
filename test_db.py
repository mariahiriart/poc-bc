import psycopg2
import os
from dotenv import load_dotenv

# Try loading from documentacion/.env if it exists
load_dotenv('d:/Usuario/Desktop/documentacion/.env')

DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)

try:
    conn = psycopg2.connect(**DB_CFG)
    cur  = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) 
        FROM raw_messages 
        WHERE sent_at >= '2026-03-26'::date + interval '6 hours'
    """)
    res = cur.fetchone()
    print(f"Messages for today (Mar 26th): {res[0] if res else 0}")
    cur.close(); conn.close()
except Exception as e:
    print(f"Error: {e}")
