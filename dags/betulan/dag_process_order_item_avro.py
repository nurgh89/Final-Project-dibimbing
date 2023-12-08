from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, types
from fastavro import reader


def postgres_connecion():
    user = 'user'
    password = 'password'
    host = '35.240.213.100'
    port = '5433'
    db = 'data_warehouse'

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string)
    return engine

# Fungsi untuk membaca dan memproses file Avro


def process_avro_file():
    engine = postgres_connecion()
    # Ganti '/path/to/your/avro/file.avro' dengan lokasi file Avro Anda
    file_path = 'path/to/data/order_item.avro'
    with open(file_path, 'rb') as fo:
        reader = reader(fo)
        for record in reader:
            # Lakukan pemrosesan data di sini
            # Contoh: Tampilkan data
            print(record)
        reader.close()


# Konfigurasi default arguments
default_args = {
    'owner': 'Kelompok 1',
    'start_date': datetime(2023, 8, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inisiasi DAG
dag = DAG('Dag_order_item_avro',
          default_args=default_args,
          description='DAG to process Avro data',
          schedule_interval='@once',  # Atur interval sesuai kebutuhan
          catchup=False
          )

# Tugas awal dan akhir DAG
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Tugas untuk memproses file Avro
process_avro = PythonOperator(
    task_id='process_avro',
    python_callable=process_avro_file,
    dag=dag
)

# Mengatur urutan tugas
start_task >> process_avro >> end_task
