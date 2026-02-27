from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def update_sales_daily():
    pg = create_engine("postgresql://admin:admin@postgres:5432/ecommerce")
    ch = clickhouse_connect.get_client(host='clickhouse', port=8123,
                                        username='admin', password='admin')
    query = """
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
    df = pd.read_sql(query, pg)
    df['date'] = pd.to_datetime(df['date']).dt.date
    ch.command("TRUNCATE TABLE marts.sales_daily")
    ch.insert_df('marts.sales_daily', df)
    print(f"✅ sales_daily обновлена: {len(df)} строк")

def update_customer_rfm():
    pg = create_engine("postgresql://admin:admin@postgres:5432/ecommerce")
    ch = clickhouse_connect.get_client(host='clickhouse', port=8123,
                                        username='admin', password='admin')
    query = """
        SELECT customer_id, recency, frequency, monetary, segment
        FROM staging.customer_rfm
    """
    df = pd.read_sql(query, pg)
    df['recency'] = df['recency'].astype(int)
    df['frequency'] = df['frequency'].astype(int)
    df['monetary'] = df['monetary'].astype(float)
    ch.command("TRUNCATE TABLE marts.customer_rfm")
    ch.insert_df('marts.customer_rfm', df)
    print(f"✅ customer_rfm обновлена: {len(df)} строк")

with DAG(
    dag_id='update_marts',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['etl', 'marts']
) as dag:

    task_sales = PythonOperator(
        task_id='update_sales_daily',
        python_callable=update_sales_daily
    )

    task_rfm = PythonOperator(
        task_id='update_customer_rfm',
        python_callable=update_customer_rfm
    )

    task_sales >> task_rfm