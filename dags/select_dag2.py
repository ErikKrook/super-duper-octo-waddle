from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


with DAG(
    dag_id='select2',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, # This makes it "triggered on command"
    catchup=False
) as dag:

    run_select = PostgresOperator(
        task_id='select_task2',
        postgres_conn_id='postgres_con',
        sql="SELECT 2"
    )
