from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, DateTime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


def postgres_connecion():
    user = 'user'
    password = 'password'
    host = '35.240.213.100'
    port = '5433'
    db = 'data_warehouse'

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string)
    return engine


def insert_to_postgres():
    engine = postgres_connecion()
    df = pd.read_json('/opt/airflow/data/coupons.json', orient='column')

    df_schema = {
        'id': Integer,
        'discount_percent': Float
    }

    df.to_sql('coupons', con=engine, schema='public',
              if_exists='replace', index=False, dtype=df_schema)


with DAG(
    dag_id='insert_to_postgres',
    default_args=default_args,
    schedule_interval='@once',
)as dag:

    task_ingest_json_to_postgres = PythonOperator(
        task_id='ingest_json_to_postgres',
        python_callable=insert_to_postgres,
        dag=dag
    )
