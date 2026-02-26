import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:admin@localhost:5432/ecommerce")

files = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "product_category_name_translation.csv": "category_translation",
}

for filename, table in files.items():
    try:
        df = pd.read_csv(f"data/{filename}")
        df.to_sql(
            table,
            engine,
            schema="staging",
            if_exists="replace",
            index=False
        )
        print(f"✅ {table}: загружено {len(df)} строк")
    except Exception as e:
        print(f"❌ {table}: ошибка — {e}")

print("\nЗагрузка завершена!")