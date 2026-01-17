from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


def get_ready_job():
    hook = PostgresHook(postgres_conn_id='postgres_con')
    # Fetch next job in line to run
    sql = """
    SELECT job_id, dag_id 
    FROM job_control_queue 
    WHERE is_ready = TRUE 
    ORDER BY job_id ASC
    LIMIT 1
    """

    records = hook.get_records(sql)
    return records[0] if records else None

def trigger_generator(**context):
    job = get_ready_job()
    
    if job:
        job_id, dag_id = job
        hook = PostgresHook(postgres_conn_id='postgres_con')

        print(f"Triggering job {job_id} for DAG {dag_id}")
        
        # 1. Immediately flip the status to prevent double-triggering
        # We set it to 'RUNNING' or False
        hook.run(f"UPDATE job_control_queue SET is_ready = FALSE WHERE job_id = {job_id}")
        
        # 2. Trigger the child DAG
        TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}_{job_id}',
            trigger_dag_id=dag_id,
            wait_for_completion=True # Essential if you want the loop to stay 'occupied'
        ).execute(context)
        
        # 3. Mark as finished
        hook.run(f"UPDATE job_control_queue SET last_triggered = NOW() WHERE job_id = {job_id}")
    else:
        print("No jobs ready to run.")

with DAG(
    dag_id='master_dag_loop',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/1 * * * *', # every minute
    catchup=False
) as dag:

    run_jobs = PythonOperator(
        task_id='check_and_trigger_job',
        python_callable=trigger_generator
    )