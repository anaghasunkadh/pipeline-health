from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
def print_hello():
    print("Hello, Feature Store!")
    print("This is my first Airflow task")
    return "success"
default_args = {
    'owner':'Anagha',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}
dag = DAG (
    dag_id='hello_featurestore',
    default_args= default_args,
    description='My first MLpipeline DAG',
    schedule=None,
    start_date=datetime(2026, 2,3),
    catchup=False,
)
task1 = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)