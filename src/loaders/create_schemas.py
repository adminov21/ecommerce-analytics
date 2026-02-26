import psycopg2

conn = psycopg2.connect(
    host="localhost", port=5432,
    user="admin", password="admin", dbname="ecommerce"
)
conn.autocommit = True
cursor = conn.cursor()

with open("sql/staging/create_tables.sql", "r") as f:
    sql = f.read()

cursor.execute(sql)
print("✅ Схема staging создана успешно")
conn.close()