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



    upload_to_snowflake_task = BashOperator(
        task_id='Upload_to_Snowflake',
        bash_command='python3 /opt/airflow/scripts/python/upload_to_snowflake.py && rm -r /opt/airflow/includes/sms_cleaned && rm -r /opt/airflow/includes/call_cleaned',  # lowercase path
    )

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='dbt_pipeline',  
    )


    # Define task dependencies
  upload_to_snowflake_task >> trigger_dbt_dag


