import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='admin',
    password='admin'
)

with open('sql/marts/clickhouse_marts.sql', 'r') as f:
    sql = f.read()

# Выполняем каждый запрос отдельно
for statement in sql.split(';'):
    statement = statement.strip()
    if statement:
        client.command(statement)
        print(f"✅ Выполнено: {statement[:50]}...")

print("\nВитрины ClickHouse созданы!")