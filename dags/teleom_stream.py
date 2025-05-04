from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import socket

from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='telecom_stream_pipeline',
    default_args=default_args,
    description='A DAG for streaming telecom data from Kafka to Snowflake',
    schedule='*/5 * * * *',  
    catchup=False,
    tags=['kafka'],
) as dag:

    produce_task = BashOperator(
        task_id='kafka_producer',
        bash_command='python3 /opt/airflow/scripts/Kafka/Producer.py',  # lowercase path
    )

    consume_task = BashOperator(
        task_id='kafka_consumer',
        bash_command='python3 /opt/airflow/scripts/Kafka/consumer.py',  # lowercase path
    )

    cleaning_job_task = SparkSubmitOperator(
        task_id="Spark_Cleaning_Job",
        application="/opt/airflow/scripts/Spark/cleaning_job.py",
        conn_id="spark_conn",  
        verbose=True,
        executor_memory="2g",
        driver_memory="2g",
        num_executors=1,
        total_executor_cores=1,
        executor_cores=1,
    )

    upload_to_snowflake_task = BashOperator(
        task_id='Upload_to_Snowflake',
        bash_command='python3 /opt/airflow/scripts/python/upload_to_snowflake.py && rm -r /opt/airflow/includes/sms_cleaned && rm -r /opt/airflow/includes/call_cleaned',  # lowercase path
    )

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='dbt_pipeline',  
    )


    # Define task dependencies
    produce_task >> consume_task >> cleaning_job_task >> upload_to_snowflake_task >> trigger_dbt_dag


