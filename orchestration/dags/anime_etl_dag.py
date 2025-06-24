from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def extract_data():
    print("Extracting raw data...")

def validate_data():
    print("Validating bronze data...")

def send_notification():
    print("Pipeline finished successfully!")

default_args = {
    'owner': 'anime_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'anime_etl_dag',
    default_args=default_args,
    description='ETL pipeline: Bronze -> Silver -> Gold', 
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    validate = PythonOperator(
        task_id='validate_bronze_data',
        python_callable=validate_data
    )

    validate_gold = BashOperator(
        task_id='validate_gold_layer',
        bash_command='echo "Validating Gold Layer..."'
    )

    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification
    )

    extract >> validate >> validate_gold >> notify
