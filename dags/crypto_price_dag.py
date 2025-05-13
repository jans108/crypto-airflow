import pendulum

from airflow.decorators import dag, task
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# load env variables
load_dotenv()


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
    schedule='@daily',
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

    @task
    def save_to_mysql(df: pd.DataFrame):
        # Save the data to MySQL
        try:
            # Create engine
            wsl_ip = os.getenv('wsl_ip')
            engine = create_engine("mysql+mysqlconnector://airflow:" + os.getenv('ADMIN_PASSWORD') + "@" + wsl_ip + ":3306/airflow_db")
            # Save the DataFrame to MySQL
            df.to_sql('currency', con=engine, if_exists='append', index=False)
            # Close the engine
            engine.dispose()
        except Exception as e:
            print(f"Error saving to MySQL: {e}")
            raise


    # Define the task dependencies
    raw_data = fetch_currencies()
    transformed_data = transform_currency_data(raw_data)
    add_metadata = add_time_and_metadata(transformed_data)
    validated_data = validate_and_clean_data(add_metadata)
    save_to_mysql(validated_data)

# Instantiate the DAG
dag_instance = currency_price_dag()
# Run the DAG
if __name__ == "__main__":
    from airflow.utils.session import create_session

    test_date = datetime(2023, 5, 13)

    print("Testing full DAG execution")
    os.system(f'airflow dags test currency_price_dag {test_date.strftime("%Y-%m-%d")}')


# Note: The above code is a complete Airflow DAG that fetches cryptocurrency prices from the NBP API, transforms the data, adds metadata, validates and cleans the data, and saves it to a MySQL database.
# The code uses the Airflow decorators to define tasks and their dependencies, and it uses the SQLAlchemy library to connect to the MySQL database.
# The code also uses the Pandas library to manipulate the data and the Requests library to make HTTP requests to the API.
# The code is designed to be run in an Airflow environment, and it uses the dotenv library to load environment variables from a .env file.

