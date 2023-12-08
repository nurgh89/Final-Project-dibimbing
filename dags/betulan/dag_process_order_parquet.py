from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def postgres_connecion():
    user = 'user'
    password = 'password'
    host = '35.240.213.100'
    port = '5433'
    db = 'data_warehouse'

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string)
    return engine

# Fungsi untuk membaca dan memproses file Parquet


def process_parquet_file():
    engine = postgres_connecion()
    # Ganti '/path/to/your/parquet/file.parquet' dengan lokasi file Parquet Anda
    file_path = '/data/order.parquet'
    df = pd.read_parquet(file_path)
    # Lakukan pemrosesan data di sini
    # Contoh: Tampilkan informasi dari data
    print(df.head())


# Konfigurasi default arguments
default_args = {
    'owner': 'kelompok 1',
    'start_date': datetime(2023, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisiasi DAG
dag = DAG('Dag_order_parquet',
          default_args=default_args,
          description='DAG to process Parquet data',
          schedule_interval='@once',  # Atur interval sesuai kebutuhan
          catchup=False
          )

# Tugas awal dan akhir DAG
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Tugas untuk memproses file Parquet
process_parquet = PythonOperator(
    task_id='process_parquet',
    python_callable=process_parquet_file,
    dag=dag
)

# Mengatur urutan tugas
start_task >> process_parquet >> end_task
