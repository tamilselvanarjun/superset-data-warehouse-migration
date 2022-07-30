# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os

# get dag directory path
dag_path = os.getcwd()

def split_into_chunks(arr, n):
    return [arr[i : i + n] for i in range(0, len(arr), n)]

def transform_data():
    data_df = pd.read_csv(f"{dag_path}/raw_data/booking.csv", 
                        skiprows=1,
                        header=None,
                        delimiter="\n",
                    )
    series = data_df[0].str.split(";")
    pd_lines = []

    for line in series:
        old_line = [item.strip() for item in line]
        info_index = 4
        info = old_line[:info_index]
        remaining = old_line[info_index:-1]
        chunks = split_into_chunks(remaining, 6)
        for chunk in chunks:
            record = info + chunk
            pd_lines.append(record)

    new_data_df = pd.DataFrame(
        pd_lines,
        columns=[
            "track_id",
            "type",
            "traveled_d",
            "avg_speed",
            "lat",
            "lon",
            "speed",
            "lon_acc",
            "lat_acc",
            "time",
        ],
    )
    new_data_df.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)
    return new_data_df.shape
    #booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    #client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
    #hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

    # merge booking with client
    #data = pd.merge(booking, client, on='client_id')
    #data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

    # merge booking, client & hotel
    #data = pd.merge(data, hotel, on='hotel_id')
    #data.rename(columns={'name': 'hotel_name'}, inplace=True)

    # make date format consistent
    #data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

    # make all cost in GBP currency
    #data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    #data.currency.replace("EUR", "GBP", inplace=True)

    # remove unnecessary columns
    #data = data.drop('address', 1)

    # load processed data
    #data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)


def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/datascience.db")
    c = conn.cursor()
    c.execute('''
                CREATE TABLE IF NOT EXISTS booking_record (
                    id serial primary key,
                    track_id numeric, 
                    type text not null, 
                    traveled_d double precision DEFAULT NULL,
                    avg_speed double precision DEFAULT NULL, 
                    lat double precision DEFAULT NULL, 
                    lon double precision DEFAULT NULL, 
                    speed double precision DEFAULT NULL,    
                    lon_acc double precision DEFAULT NULL, 
                    lat_acc double precision DEFAULT NULL, 
                    time double precision DEFAULT NULL
                );
            ''')
    records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
    records.to_sql('booking_record', conn, if_exists='replace', index=False)


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)


task_1 >> task_2