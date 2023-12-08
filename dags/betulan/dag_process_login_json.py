# Airflow
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, DateTime

default = {
    "owner": "Kelompok1",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
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


# define key data
KEY_DATA_COUPONS = "df_coupons"
KEY_DATA_LOGIN = "df_login"

with DAG(
    dag_id="Dag_login_json",
    start_date=datetime(2023, 8, 12),
    catchup=False,
    default_args=default,
    schedule_interval="@once"
) as dag:

    @task
    def fetch_data_json_coupons(**context):
        file_coupon = [
            'data/coupons.json',
        ]

        df_result = pd.DataFrame()
        for path in file_coupon:
            df_temp = pd.read_json(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result, df_temp])
        print(df_result)
        return context['ti'].xcom_push(key=KEY_DATA_COUPONS, value=df_result)

    @task
    def fetch_data_from_json_login(**context):

        file_login_attempts = [
            'data/login_attempts_0.json',  # file path disesuaikan
            'data/login_attempts_1.json',
            'data/login_attempts_2.json',
            'data/login_attempts_3.json',
            'data/login_attempts_4.json',
            'data/login_attempts_5.json',
            'data/login_attempts_6.json',
            'data/login_attempts_7.json',
            'data/login_attempts_8.json',
            'data/login_attempts_9.json'
        ]

        df_result = pd.DataFrame()
        for path in file_login_attempts:
            df_temp = pd.read_json(path)
            # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
            df_result = pd.concat([df_result, df_temp])
        return context['ti'].xcom_push(key=KEY_DATA_LOGIN, value=df_result)

    @task
    def transfrom_dataset(**context):
        # Transform Coupons Data
        df_coupon = context['ti'].xcom_pull(key=KEY_DATA_COUPONS)
        df_coupon['discount_percent'] = df_coupon['discount_percent'].astype(
            float)
        df_coupon.drop_duplicates(keep='first', inplace=True, subset=['id'])
        # Transform Login Attemps Data
        df_login_attemps = context['ti'].xcom_pull(key=KEY_DATA_LOGIN)
        df_login_attemps.drop_duplicates(
            keep='first', inplace=True, subset=['id'])

        context['ti'].xcom_push(key=KEY_DATA_COUPONS, value=df_coupon)
        context['ti'].xcom_push(key=KEY_DATA_LOGIN, value=df_login_attemps)

    @task
    def insert_to_database(**context):
        df_coupons = context['ti'].xcom_pull(key=KEY_DATA_COUPONS)
        df_login_attemps = context['ti'].xcom_pull(key=KEY_DATA_LOGIN)

        # hook = PostgresHook(postgres_conn_id='postgres_dw')
        engine = postgres_connecion()

        print("=== SEDANG INSERT COUPON")
        df_coupons.to_sql(name='coupons_test', schema='coba', con=engine,
                          if_exists='replace', index=False)

        print("=== SEDANG INSERT LOGIN ATTEMPS")
        df_login_attemps.to_sql(name='login_attemps_test', schema='coba', con=engine,
                                if_exists='replace', index=False)

    [fetch_data_json_coupons(),
     fetch_data_from_json_login()] >> transfrom_dataset() >> insert_to_database()
