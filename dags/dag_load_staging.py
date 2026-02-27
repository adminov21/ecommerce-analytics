from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def load_csv_to_staging(filename, table_name):
    engine = create_engine("postgresql://admin:admin@postgres:5432/ecommerce")
    df = pd.read_csv(f"/opt/airflow/data/{filename}")
    df.to_sql(
        table_name,
        engine,
        schema='staging',
        if_exists='replace',
        index=False
    )
    print(f"Загружено {len(df)} строк в staging.{table_name}")

with DAG(
    dag_id='load_staging',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['etl', 'staging']
) as dag:

    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_orders_dataset.csv',
            'table_name': 'orders'
        }
    )

    load_items = PythonOperator(
        task_id='load_order_items',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_order_items_dataset.csv',
            'table_name': 'order_items'
        }
    )

    load_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_customers_dataset.csv',
            'table_name': 'customers'
        }
    )

    load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_products_dataset.csv',
            'table_name': 'products'
        }
    )

    load_payments = PythonOperator(
        task_id='load_payments',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_order_payments_dataset.csv',
            'table_name': 'order_payments'
        }
    )

    load_reviews = PythonOperator(
        task_id='load_reviews',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'filename': 'olist_order_reviews_dataset.csv',
            'table_name': 'order_reviews'
        }
    )

    # Зависимости — сначала заказы, потом всё остальное
    load_orders >> [load_items, load_customers, load_payments, load_reviews]
    load_products