import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CFG = dict(
    host=os.environ.get('DB_HOST', ''),
    port=int(os.environ.get('DB_PORT', 5432)),
    database=os.environ.get('DB_NAME', 'neondb'),
    user=os.environ.get('DB_USER', ''),
    password=os.environ.get('DB_PASSWORD', ''),
    sslmode="require"
)

def check_routes(date_str):
    try:
        conn = psycopg2.connect(**DB_CFG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) 
            FROM routes 
            WHERE operation_date = %s
        """, (date_str,))
        res = cur.fetchone()
        print(f"Routes for {date_str}: {res[0] if res else 0}")
        cur.close(); conn.close()
    except Exception as e:
        print(f"Error check_routes {date_str}: {e}")

if __name__ == "__main__":
    check_routes("2026-03-26")
