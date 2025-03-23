from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, Airflow!")

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criação da DAG
dag = DAG(
    'dag_teste',
    default_args=default_args,
    description='Uma DAG de teste simples',
    schedule_interval=timedelta(days=1),  # Executa diariamente
    catchup=False,
)

# Definição da tarefa
task_hello = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)

task_hello