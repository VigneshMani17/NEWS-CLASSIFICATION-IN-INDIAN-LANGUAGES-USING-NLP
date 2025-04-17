
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
import time
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from news_etl import NewsExtractor

#1) fetch amazon data (extract) 2) clean data (transform)

obj = NewsExtractor()

#3) create and store data in table on postgres (load)
    
def insert_news_data_into_postgres(ti):
    news_data = ti.xcom_pull(key='news_data', task_ids='fetch_news_data')
    if not news_data:
        raise ValueError("No news data found")

    postgres_hook = PostgresHook(postgres_conn_id='news_connection')
    insert_query = """
    INSERT INTO news (headline, category)
    VALUES (%s, %s)
    """
    for news in news_data:
        postgres_hook.run(insert_query, parameters=(news['headline'], news['category']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_news_headlines',
    default_args=default_args,
    description='A simple DAG to fetch news data from news website and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_news_data_task = PythonOperator(
    task_id='fetch_news_data',
    python_callable=obj.run_news_etl,
    dag=dag,
)

# Replace the PostgresOperator with a PythonOperator for table creation
def create_news_table():
    postgres_hook = PostgresHook(postgres_conn_id='news_connection')
    create_table_query = """
    CREATE TABLE IF NOT EXISTS news (
        id SERIAL PRIMARY KEY,
        headline TEXT NOT NULL,
        category TEXT
    );
    """
    postgres_hook.run(create_table_query)

# Define the PythonOperator for creating the table
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_news_table,
    dag=dag,
)

insert_news_data_task = PythonOperator(
    task_id='insert_news_data',
    python_callable=insert_news_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_news_data_task >> create_table_task >> insert_news_data_task
