import psycopg2
import clickhouse_connect
from dotenv import load_dotenv
import os

load_dotenv()

# Тест PostgreSQL
print("Проверяю PostgreSQL...")
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="admin",
        password="admin",
        dbname="ecommerce"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print(f"✅ PostgreSQL работает: {cursor.fetchone()[0][:30]}")
    conn.close()
except Exception as e:
    print(f"❌ PostgreSQL ошибка: {e}")

# Тест ClickHouse
print("\nПроверяю ClickHouse...")
try:
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="admin",
        password="admin"
    )
    result = client.query("SELECT version()")
    print(f"✅ ClickHouse работает: {result.result_rows[0][0][:30]}")
except Exception as e:
    print(f"❌ ClickHouse ошибка: {e}")

print("\nГотово!")