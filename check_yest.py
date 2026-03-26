import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.environ.get('DB_HOST'),
    port=5432,
    dbname='neondb',
    user=os.environ.get('DB_USER'),
    password=os.environ.get('DB_PASSWORD'),
    sslmode='require'
)
cur = conn.cursor()
cur.execute("""
    SELECT COUNT(*) 
    FROM raw_messages 
    WHERE sent_at >= '2026-03-25'::date + interval '6 hours' 
      AND sent_at < '2026-03-26'::date + interval '6 hours'
""")
print(f"Messages for Mar 25th: {cur.fetchone()[0]}")
cur.close(); conn.close()
