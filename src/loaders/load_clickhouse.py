import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect

pg_engine = create_engine("postgresql://admin:admin@localhost:5432/ecommerce")
ch_client = clickhouse_connect.get_client(
    host='localhost', port=8123,
    username='admin', password='admin'
)

# Витрина 1: ежедневные продажи
print("Загружаю sales_daily...")
query1 = """
    SELECT 
        DATE(o.order_purchase_timestamp) as date,
        COUNT(DISTINCT o.order_id) as orders_count,
        SUM(p.payment_value) as revenue,
        AVG(p.payment_value) as avg_order_value
    FROM staging.orders o
    JOIN staging.order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE(o.order_purchase_timestamp)
    ORDER BY date
"""
df1 = pd.read_sql(query1, pg_engine)
df1['date'] = pd.to_datetime(df1['date']).dt.date
ch_client.insert_df('marts.sales_daily', df1)
print(f"✅ sales_daily: {len(df1):,} строк")

# Витрина 2: метрики по категориям
print("Загружаю product_category_metrics...")
query2 = """
    SELECT 
        COALESCE(ct.product_category_name_english, p.product_category_name, 'Unknown') as category_name,
        DATE_TRUNC('month', o.order_purchase_timestamp::timestamp)::date as month,
        COUNT(DISTINCT o.order_id) as orders_count,
        SUM(oi.price) as revenue,
        AVG(r.review_score) as avg_review_score
    FROM staging.orders o
    JOIN staging.order_items oi ON o.order_id = oi.order_id
    JOIN staging.products p ON oi.product_id = p.product_id
    LEFT JOIN staging.category_translation ct ON p.product_category_name = ct.product_category_name
    LEFT JOIN staging.order_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY 1, 2
    ORDER BY 2, 1
"""
df2 = pd.read_sql(query2, pg_engine)
df2['month'] = pd.to_datetime(df2['month']).dt.date
df2['avg_review_score'] = df2['avg_review_score'].fillna(0)
ch_client.insert_df('marts.product_category_metrics', df2)
print(f"✅ product_category_metrics: {len(df2):,} строк")

# Витрина 3: RFM сегменты
print("Загружаю customer_rfm...")
query3 = """
    SELECT 
        customer_id,
        recency,
        frequency,
        monetary,
        segment
    FROM staging.customer_rfm
"""
df3 = pd.read_sql(query3, pg_engine)
df3['recency'] = df3['recency'].astype(int)
df3['frequency'] = df3['frequency'].astype(int)
df3['monetary'] = df3['monetary'].astype(float)
ch_client.insert_df('marts.customer_rfm', df3)
print(f"✅ customer_rfm: {len(df3):,} строк")

print("\nВсе витрины загружены!")