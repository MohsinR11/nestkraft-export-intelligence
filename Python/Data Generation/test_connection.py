import psycopg2
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG

try:
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["username"],
        password=DB_CONFIG["password"]
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Failed: {e}")