from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries':2,
    'retry_delay': timedelta(minutes=1.5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='spark_create_table_and_run_batch_jobs',
    default_args=default_args,
    description='Airflow DAG to orchestrate Spark jobs for Bronze → Silver pipeline',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['spark', 'kafka', 'iceberg', 'bronze', 'silver', 'pipeline'],
) as dag:

    spark_base_command = """
docker exec spark-master spark-submit \
--master spark://spark-master:7077 \
 --deploy-mode client \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.instances=1 \
 --conf spark.executor.memory=512m \
--conf spark.executor.cores=1 \
--conf spark.driver.memory=512m \
  --conf spark.task.cpus=1 \
  --conf spark.cores.max=1 \
--conf spark.ui.port=4040 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
"""


    create_iceberg_table_job = BashOperator(
        task_id='create_iceberg_table_job',
        bash_command= spark_base_command + "/opt/bitnami/spark/jobs/create_iceberg_table.py"
        ,
    )
    
    # run_pos_stream_bronze_job = BashOperator(
    #     task_id='run_pos_stream_bronze_job',
    #     bash_command= spark_base_command + "/opt/bitnami/spark/jobs/pos_stream_bronze.py"
    # )

    # run_supplier_stream_bronze_job = BashOperator(
    #     task_id='run_supplier_stream_bronze_job',
    #     bash_command=spark_base_command + "/opt/bitnami/spark/jobs/supplier_stream_bronze.py",
    # )

    # run_ticket_stream_bronze_job = BashOperator(
    #     task_id='run_ticket_stream_bronze_job',
    #     bash_command=spark_base_command + " /opt/bitnami/spark/jobs/ticket_stream_bronze.py",
    # )

    # run_consession_stream_bronze_job = BashOperator(
    #     task_id='run_consession_stream_bronze_job',
    #     bash_command=spark_base_command + "/opt/bitnami/spark/jobs/consession_stream_bronze.py",
    # )

    run_anime_batch_bronze_job = BashOperator(
        task_id='run_anime_batch_bronze_job',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/anime_batch_bronze.py",
    )

    run_customer_batch_bronze_job = BashOperator(
        task_id='run_customer_batch_bronze_job',
        bash_command=spark_base_command + " /opt/bitnami/spark/jobs/customer_batch_bronze.py",
    )
   

for task in [
    run_anime_batch_bronze_job,
    run_customer_batch_bronze_job,
 
]:
    create_iceberg_table_job >> task

