from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, types
import xlrd

# Deklarasi konfigurasi Airflow
default_args = {
    'owner': 'kelompok 1',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Deklarasi DAG
dag = DAG(
    'Data_ingest_product_category_xls',
    default_args=default_args,
    schedule_interval='@once',
)
# Proses Load data ke dalam database postgres


def load_data_to_postgres():
    df = pd.read_excel('/opt/airflow/data/supplier.xls')
    df.drop('Unnamed: 0', axis=1, inplace=True)

    db_username = 'user'
    db_password = 'password'
    db_host = '35.240.213.100'
    db_port = '5433'
    db_name = 'data_warehouse'

    engine = create_engine(
        f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    table_name = 'product_category'

    column_types = {
        'id': types.Integer,
        'name': types.String,
    }

    df.to_sql(table_name, engine, if_exists='replace',
              index=False, dtype=column_types)


# Load data ke DAG
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=dag,
)
load_data_task
