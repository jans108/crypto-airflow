import pendulum

from airflow.decorators import dag, task
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

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
    dag_id="currency_price_dag",
    default_args=default_args,
    description="A DAG to fetch and process cryptocurrency prices",
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 5, 10),
    catchup=False,
    tags=["crypto", "data_pipeline"]
)
def currency_price_dag():

    @task
    def fetch_currencies():
        url = 'https://api.nbp.pl/api/exchangerates/tables/A?format=json'
        response = requests.get(url)
        data = response.json()
        rates = data[0]['rates']
        return rates

    @task
    def transform_currency_data(rates: dict):
        # Extract the relevant data
        df = pd.DataFrame(rates)
        df = df[['currency', 'code', 'mid']]
        df['mid'] = df['mid'].astype(float)
        df['currency'] = df['currency'].str.lower()
        df['code'] = df['code'].str.lower()
        df['mid'] = df['mid'].round(2)
        return df

    @task
    def add_time_and_metadata(df: pd.DataFrame):
        # Add a time and metadata
        current_time = datetime.now(timezone.utc)
        df['source'] = 'NBP'
        df['retrieved_at'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['retrieved_at'] = pd.to_datetime(df['retrieved_at'], format='%Y-%m-%d %H:%M:%S')
        return df

    @task
    def validate_and_clean_data(df: pd.DataFrame):
        # Validate and clean the data
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df.sort_values(by='mid', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
