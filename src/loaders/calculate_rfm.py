import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:admin@localhost:5432/ecommerce")

query = """
    SELECT 
        o.customer_id,
        MAX(o.order_purchase_timestamp) as last_purchase,
        COUNT(DISTINCT o.order_id) as frequency,
        SUM(oi.price) as monetary
    FROM staging.orders o
    JOIN staging.order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.customer_id
"""
rfm = pd.read_sql(query, engine)
rfm['last_purchase'] = pd.to_datetime(rfm['last_purchase'])

reference_date = rfm['last_purchase'].max()
rfm['recency'] = (reference_date - rfm['last_purchase']).dt.days

rfm['r_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1]).astype(int)
rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5]).astype(int)
rfm['m_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5]).astype(int)

def assign_segment(row):
    r, f = row['r_score'], row['f_score']
    if r >= 4 and f >= 4: return 'Champions'
    elif r >= 3 and f >= 3: return 'Loyal Customers'
    elif r >= 4 and f <= 2: return 'New Customers'
    elif r <= 2 and f >= 3: return 'At Risk'
    elif r <= 2 and f <= 2: return 'Lost'
    else: return 'Potential Loyalists'

rfm['segment'] = rfm.apply(assign_segment, axis=1)

rfm[['customer_id','recency','frequency','monetary','segment']].to_sql(
    'customer_rfm', engine, schema='staging', if_exists='replace', index=False
)
print(f"✅ RFM рассчитан и сохранён: {len(rfm):,} строк")
print(rfm['segment'].value_counts())