from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'ma7moud',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
}

dag = DAG(
    dag_id='dbt_pipeline',
    default_args=default_args,
    description='dbt_orchestrator Pipeline',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

profile = '/opt/airflow/includes/dbt'
project_path = '/opt/airflow/includes/dbt/TELECOM'

with dag:
    with TaskGroup("rub_snapshot") as dbt_snapshot_group:
        snap = ['CDC_Cell_Site', 'CDC_Users']
        for snapshot in snap:
            dbt_snapshot = BashOperator(
                task_id=f'snapshot_{snapshot}',
                bash_command=f'dbt snapshot --profiles-dir {profile} --project-dir {project_path} --select {snapshot}',
            )

    with TaskGroup("rub_dimensions") as dbt_dimension_group:
        models = ['Dim_cell_site', 'Dim_date', 'Dim_device_tac', 'Dim_user']
        for model in models:
            dbt_model = BashOperator(
                task_id=f'dimension_{model}',
                bash_command=f'dbt run --models {model} --profiles-dir {profile} --project-dir {project_path}',
            )

    with TaskGroup("rub_facts") as dbt_fact_group:
        models = ['fact_telecom_events']
        for model in models:
            dbt_model = BashOperator(
                task_id=f'fact_{model}',
                bash_command=f'dbt run --models {model} --profiles-dir {profile} --project-dir {project_path}',
            )

    dbt_snapshot_group >> dbt_dimension_group >> dbt_fact_group