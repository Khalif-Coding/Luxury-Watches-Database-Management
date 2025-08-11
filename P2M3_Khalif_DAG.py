'''
Khalif Prabowo Santoso - 029

Script PM23_Khalif_DAG.py Berisikan Sebuah Struktur Code Atau Flow Untuk Melakukan ETL (Extract Transform Load).
Proses Ini Nantinya Akan Di Automasi Dalam Sebuah DAG Menggunakan Airflow Yang Dimulai Pada 11 November 2024
Dan Dijadwalkan Untuk Dikerjkan Setiap Hari Sabtu Dari Pukul 9.10 - 9.30 Dijalankan Setiap 10 Menit.
Yang Dilakukan Adalah Mengestrak Sebuah Dataset Dari WatchSales Yang Masih Raw -> Dilanjutkan Dengan Preprocess Data Agar Watchsales Menjadi Clean
-> Dilanjutkan Dengan Load Ke Elasticsearch Untuk Bisa Divisualisasikan Atau Dianalisa Dengan Kibana.
'''

# Import Libraries
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from elasticsearch import Elasticsearch

'''
Memberikan Nama Owner Lalu Mendefine Waktu Start DAG Tersebut
Untuk Start Date Akan Di Set Menjadi 1 November 2024.
'''
default_args= {
    'owner': 'Khalif Prabowo Santoso',
    'start_date': datetime(2024, 11, 1)
}

'''
Pada with DAG Sebelum Melakukan Task, Akan Menamai DAGnya Selanjutnya Define default_args Sesuai Yang Sudah Didefinisikan Diawal, Lalu Juga Mengubah Schedule Intervalnya
Menjadi Yang Sudah Disebutkan Di Objectives Serta Mendefine Function Untuk Start Dan End. 
'''
with DAG(
    'Khalif-ETL',
    default_args=default_args, 
    schedule_interval="10-30/10 9 * * 6",
    catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def extract_from_db():
        # Define Database Server
        database = "airflow"
        username = "airflow"
        password = "airflow"
        host = "172.17.0.1"
        port = "5433"

        postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

        engine = create_engine(postgres_url)
        conn = engine.connect()

        # Read Data Pada Table Di Postgre
        df = pd.read_sql('SELECT * FROM table_m3', conn)
        # Menyimpan Data Tersebut Ke Dalam Tipe Data .csv Untuk Disimpan Di Folder Data
        df.to_csv('/opt/airflow/data/Khalif_Raw.csv', index=False)
        
        print("Success")

    @task()
    def preprocess_data():
        # Read Data Dari Folder Data Yang Baru Di Ekstrak
        df = pd.read_csv('/opt/airflow/data/Khalif_Raw.csv') 

        # Melakukan Process Preprocessing
        # Handling On 'Condition'
        df['Condition'] = df['Condition'].replace({
            'Like new & unworn': 'New',
            'Used (Very good)': 'Used (Good)',
            'Used (Fair)': 'Used',
            'Used (Incomplete)': 'Used',
            'Used (Poor)': 'Used'
        })

        # Handling On 'Scope of delivery'
        df['Scope of delivery'] = df['Scope of delivery'].replace({
            'Original box, original papers': 'Original Box and Papers',
            'No original box, no original papers': 'No Box and papers',
            'Original box, no original papers': 'Original Box',
            'Original papers, no original box': 'Original Paper'
        })

        # Handling On 'Availability'
        df['Availability'] = df['Availability'].replace({
            'Item is in stock': 'Available', 
            'Item needs to be procured': 'Available Upon Request',
            'Item available on request': 'Available Upon Request'
        })

        # Handling On 'Watches Sold by the Seller'
        df.dropna(subset=['Watches Sold by the Seller'], inplace=True)
        df['Watches Sold by the Seller'] = df['Watches Sold by the Seller'].astype(int)

        # Handling On Columns Name
        df = df.rename(columns = {
            'Id': 'id',
            'Brand' : 'brand',
            'Movement': 'movement',
            'Case material': 'case_material',
            'Bracelet material': 'bracelet_material', 
            'Condition': 'condition',
            'Scope of delivery': 'scope_of_delivery', 
            'Gender': 'gender', 
            'Price': 'price', 
            'Availability': 'availability',
            'Watches Sold by the Seller': 'watches_sold'
        })

        # Handling On Duplicates
        df.drop_duplicates(inplace = True)

        # Handling On Missing Values
        df.dropna(inplace = True)
            
        # Reset Index
        df.reset_index(drop = True,inplace = True)

        # Finalized
        print("Preprocessed data is Success")
        df.to_csv('/opt/airflow/data/Data_Clean.csv', index=False)

    '''
    Beberapa Hal Yang Dilakukan Dalam Preprocessing, Antara Lain:
    1. Melakukan Replace Pada Value, Agar Lebih Mudah Di Interpretasikan. Sebagai Contoh Mengubah 5 Value Pada Condition Yang Dirasa Mirip Menjadi Total 3 Unique Value.
    2. Melakukan Drop Null Pada' Watch Sold By The Seller' Dan Mengubah Tipe Datanya Jadi 'INT'
    3. Melakukan Rename Column Menjadi Huruf Kecil Juga Mengganti Spasi Dengan '_'.
    4. Drop Semua Duplikat
    5. Drop Semua Missing Values
    6. Reset Index Setelah Melakukan Drop 
    7. Melakukan Penyimpanan Data Dalam Bentuk CSV Kembali Dengan Struktur Data Yang Baru
    '''
    @task()
    def load_to_elastic():
        es = Elasticsearch(["http://elasticsearch:9200"])

        load = pd.read_csv('/opt/airflow/data/Data_Clean.csv')

        for i,row in load.iterrows():
            res = es.index(index='watches', id=i+1, body=row.to_json())

    '''
    Load Data Yang Sudah Dengan Struktur Baru Kedalam ElasticSearch
    '''

    # Define Flow DAG
    start >> extract_from_db() >> preprocess_data() >> load_to_elastic() >> end

    
    
