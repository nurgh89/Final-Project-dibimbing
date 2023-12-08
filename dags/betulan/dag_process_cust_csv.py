import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Boolean, Float, Integer
import requests
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'kelompok1',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 12),
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
}


def csv_to_df():
    df_list = []
    for i in range(10):  # Assuming you have customer_0.csv to customer_9.csv
        file_path = f'data/customer_{i}.csv'
        df = pd.read_csv(file_path, sep=',')
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True)


def insert_to_postgres():
    df = csv_to_df()
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    connection = hook.get_conn()
    cursor = connection.cursor()

    sql_insert = 'insert into public.customer (id, first_name, last_name, address, gender, zip_code) values (%s, %s, %s, %s, %s, %s)'
    for index, row in df.iterrows():
        data_to_insert = (row['id'], row['first_name'], row['last_name'],
                          row['address'], row['gender'], row['zip_code'])
        cursor.execute(sql_insert, data_to_insert)

    connection.commit()

    cursor.close()
    connection.close()


with DAG(
    dag_id='Dag_data_cust_csv',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    read_data_task = PythonOperator(
        task_id='read_data_from_csv',
        python_callable=csv_to_df,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_into_postgres',
        python_callable=insert_to_postgres,
    )

    read_data_task >> insert_data_task
