import pendulum

from airflow.decorators import dag, task
import requests
import pandas as pd
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': "airflow",
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False
}

# Define the DAG
@dag(
    dag_id="crypto_price_dag",
    default_args=default_args,
    description="A DAG to fetch and process cryptocurrency prices",
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 5, 10, tz="UTC"),
    catchup=False,
    tags=["crypto", "data_pipeline"]
)
def currency_price_dag():

    @task
    def fetch_currencies():
           url = 'https://api.nbp.pl/api/exchangerates/tables/A?format=json'
    response = requests.get(url)
    data = response.json()

    # Extract the relevant data
    rates = data[0]['rates']
    df = pd.DataFrame(rates)
    df = df[['currency', 'code', 'mid']]
    df['mid'] = df['mid'].astype(float)
    df['currency'] = df['currency'].str.lower()
    df['code'] = df['code'].str.lower()
    df['mid'] = df['mid'].round(2)