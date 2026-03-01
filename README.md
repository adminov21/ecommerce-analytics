# E-commerce Analytics Platform

## О проекте
Аналитическая платформа для анализа данных бразильского интернет-магазина Olist.
99 441 заказ, 96 478 покупателей, период 2016-2018.

## Стек
| Инструмент | Назначение |
|---|---|
| PostgreSQL | DWH, staging и core слои |
| ClickHouse | OLAP витрины |
| Apache Airflow | Оркестрация ETL |
| Jupyter | EDA, RFM анализ |
| Apache Superset | Дашборды |
| Docker | Контейнеризация |

## Как запустить
1. Установи Docker Desktop
2. `git clone https://github.com/adminov21/ecommerce-analytics.git`
3. Скачай датасет Olist с Kaggle в папку `data/`
4. `docker compose up -d`
5. `pip install -r requirements.txt`
6. `python src/loaders/create_schemas.py`
7. `python src/loaders/load_staging.py`
8. `python src/loaders/setup_clickhouse.py`
9. `python src/loaders/calculate_rfm.py`
10. `python src/loaders/load_clickhouse.py`

## Основные выводы
- Пик продаж — ноябрь 2017 (Black Friday)
- Топ категория по выручке — bed_bath_table
- 45% покупателей в сегменте Lost
- Среднее время доставки — 12 дней

## Дашборды
[Sales Dashboard](docs/screenshots/)
